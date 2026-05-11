CREATE OR REPLACE VIEW fb_dim_calendar AS
SELECT DISTINCT
    to_date(date) AS day_val,
    year(to_date(date)) AS year_val,
    month(to_date(date)) AS month_val,
    date_format(to_date(date), 'yyyy-MM') AS year_month
FROM matches_rest_view;


CREATE OR REPLACE VIEW fb_dim_teams AS
SELECT DISTINCT
    teamApiId,
    teamFifaApiId,
    teamLongName,
    teamShortName,
    upper(regexp_replace(teamLongName, ' FC$', '')) AS team_name_norm
FROM teams_rest_view;


CREATE OR REPLACE VIEW fb_dim_round AS
SELECT DISTINCT
    round,
    CAST(regexp_extract(round, '[0-9]+', 0) AS INT) AS round_no
FROM matches_rest_view;


CREATE OR REPLACE VIEW fb_fact_matches AS
SELECT
    round,
    to_date(date) AS match_date,
    time AS match_time,
    team1 AS home_team,
    team2 AS away_team,
    score.ht[0] AS ht_home,
    score.ht[1] AS ht_away,
    score.ft[0] AS ft_home,
    score.ft[1] AS ft_away,
    coalesce(score.ft[0], 0) + coalesce(score.ft[1], 0) AS total_goals,
    CASE
        WHEN score.ft IS NULL THEN NULL
        WHEN score.ft[0] > score.ft[1] THEN 'Home Win'
        WHEN score.ft[0] < score.ft[1] THEN 'Away Win'
        ELSE 'Draw'
    END AS result,
    CASE
        WHEN score.ft IS NOT NULL THEN 'played'
        ELSE 'scheduled'
    END AS match_state
FROM matches_rest_view;


CREATE OR REPLACE VIEW fb_fact_team_matches AS
SELECT
    match_date,
    round,
    home_team AS team_name,
    away_team AS opponent,
    'HOME' AS venue,
    ft_home AS goals_for,
    ft_away AS goals_against,
    CASE
        WHEN ft_home IS NULL OR ft_away IS NULL THEN 0
        WHEN ft_home > ft_away THEN 3
        WHEN ft_home = ft_away THEN 1
        ELSE 0
    END AS points,
    CASE
        WHEN ft_home IS NULL OR ft_away IS NULL THEN NULL
        WHEN ft_home > ft_away THEN 'Win'
        WHEN ft_home = ft_away THEN 'Draw'
        ELSE 'Loss'
    END AS team_result,
    match_state
FROM fb_fact_matches

UNION ALL

SELECT
    match_date,
    round,
    away_team AS team_name,
    home_team AS opponent,
    'AWAY' AS venue,
    ft_away AS goals_for,
    ft_home AS goals_against,
    CASE
        WHEN ft_home IS NULL OR ft_away IS NULL THEN 0
        WHEN ft_away > ft_home THEN 3
        WHEN ft_home = ft_away THEN 1
        ELSE 0
    END AS points,
    CASE
        WHEN ft_home IS NULL OR ft_away IS NULL THEN NULL
        WHEN ft_away > ft_home THEN 'Win'
        WHEN ft_home = ft_away THEN 'Draw'
        ELSE 'Loss'
    END AS team_result,
    match_state
FROM fb_fact_matches;


CREATE OR REPLACE VIEW fb_agg_team_month AS
SELECT
    team_name,
    year(match_date) AS year_val,
    month(match_date) AS month_val,
    count(*) AS matches_played,
    sum(goals_for) AS goals_for,
    sum(goals_against) AS goals_against,
    sum(points) AS points
FROM fb_fact_team_matches
WHERE match_state = 'played'
GROUP BY
    team_name,
    year(match_date),
    month(match_date);


SELECT
    r.round,
    coalesce(f.result, 'ALL_RESULTS') AS result,
    count(*) AS no_matches,
    sum(f.total_goals) AS total_goals
FROM fb_fact_matches f
INNER JOIN fb_dim_round r
    ON f.round = r.round
WHERE f.match_state = 'played'
GROUP BY CUBE(r.round, f.result)
ORDER BY r.round, result;


SELECT
    team_name,
    year_val,
    month_val,
    sum(points) AS total_points
FROM fb_agg_team_month
GROUP BY GROUPING SETS (
    (team_name, year_val, month_val),
    (team_name),
    ()
)
ORDER BY team_name, year_val, month_val;




SELECT *
FROM (
    SELECT
        team_name,
        month(match_date) AS month_val,
        points
    FROM fb_fact_team_matches
    WHERE match_state = 'played'
      AND year(match_date) = 2025
) v
PIVOT (
    sum(points)
    FOR month_val IN (8, 9, 10, 11, 12)
)
ORDER BY team_name;



SELECT
    venue_type,
    team_name,
    sum(points) AS total_points,
    sum(goals_for) AS total_goals_for,
    sum(goals_against) AS total_goals_against
FROM (
    SELECT
        'HOME_STATS' AS venue_type,
        team_name,
        points,
        goals_for,
        goals_against
    FROM fb_fact_team_matches
    WHERE match_state = 'played'
      AND venue = 'HOME'

    UNION ALL

    SELECT
        'AWAY_STATS' AS venue_type,
        team_name,
        points,
        goals_for,
        goals_against
    FROM fb_fact_team_matches
    WHERE match_state = 'played'
      AND venue = 'AWAY'
) x
GROUP BY venue_type, team_name
ORDER BY venue_type, total_points DESC, team_name;




SELECT
    team_name,
    sum(points) AS total_points,
    sum(goals_for) - sum(goals_against) AS goal_diff,
    RANK() OVER (
        ORDER BY sum(points) DESC, (sum(goals_for) - sum(goals_against)) DESC
    ) AS rank_pos,
    DENSE_RANK() OVER (
        ORDER BY sum(points) DESC, (sum(goals_for) - sum(goals_against)) DESC
    ) AS dense_rank_pos,
    ROW_NUMBER() OVER (
        ORDER BY sum(points) DESC, (sum(goals_for) - sum(goals_against)) DESC, team_name
    ) AS row_num_pos
FROM fb_fact_team_matches
WHERE match_state = 'played'
GROUP BY team_name
ORDER BY rank_pos, team_name;



SELECT
    team_name,
    match_date,
    points,
    goals_for,
    sum(points) OVER (
        PARTITION BY team_name
        ORDER BY match_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_points,
    sum(goals_for) OVER (
        PARTITION BY team_name
        ORDER BY match_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_goals_for
FROM fb_fact_team_matches
WHERE match_state = 'played'
ORDER BY team_name, match_date;



WITH mapped_team_goals AS (
    SELECT
        coalesce(home_team_pg, home_team) AS team_name_pg,
        sum(ft_home) AS total_goals_home
    FROM fb_matches_mapped
    WHERE match_state = 'played'
      AND ft_home IS NOT NULL
    GROUP BY coalesce(home_team_pg, home_team)
)
SELECT
    round(avg(total_goals_home), 2) AS avg_goals,
    percentile_approx(total_goals_home, 0.5) AS median_goals,
    min(total_goals_home) AS min_goals,
    max(total_goals_home) AS max_goals,
    percentile_approx(total_goals_home, 0.25) AS p25,
    percentile_approx(total_goals_home, 0.50) AS p50,
    percentile_approx(total_goals_home, 0.75) AS p75,
    round(stddev(total_goals_home), 2) AS stddev_goals,
    round(var_samp(total_goals_home), 2) AS variance_goals
FROM mapped_team_goals;