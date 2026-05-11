CREATE OR REPLACE VIEW teams_rest_view AS
WITH json_view AS (
    SELECT from_json(
        json_raw.data,
        'ARRAY<STRUCT<teamApiId:BIGINT,teamFifaApiId:BIGINT,teamLongName:STRING,teamShortName:STRING>>'
    ) AS array
    FROM (
        SELECT java_method(
            'org.spark.service.rest.QueryRESTDataService',
            'getRESTDataDocument',
            'http://localhost:8090/DSA-SQL-JDBCService/rest/football/TeamsView'
        ) AS data
    ) json_raw
)
SELECT v.*
FROM json_view
LATERAL VIEW explode(json_view.array) AS v;


CREATE OR REPLACE VIEW players_rest_view AS
WITH json_view AS (
    SELECT from_json(
        json_raw.data,
        'ARRAY<STRUCT<playerApiId:BIGINT,playerFifaApiId:BIGINT,playerName:STRING,birthday:STRING,height:DOUBLE,weight:DOUBLE>>'
    ) AS array
    FROM (
        SELECT java_method(
            'org.spark.service.rest.QueryRESTDataService',
            'getRESTDataDocument',
            'http://localhost:8090/DSA-SQL-JDBCService/rest/football/PlayersView'
        ) AS data
    ) json_raw
)
SELECT v.*
FROM json_view
LATERAL VIEW explode(json_view.array) AS v;


CREATE OR REPLACE VIEW team_alias_rest_view AS
WITH json_view AS (
    SELECT from_json(
        json_raw.data,
        'ARRAY<STRUCT<sourceName:STRING,canonicalName:STRING>>'
    ) AS array
    FROM (
        SELECT java_method(
            'org.spark.service.rest.QueryRESTDataService',
            'getRESTDataDocument',
            'http://localhost:8097/DSA-DOC-CSVService/rest/TeamAliasView'
        ) AS data
    ) json_raw
)
SELECT v.*
FROM json_view
LATERAL VIEW explode(json_view.array) AS v;


CREATE OR REPLACE VIEW matches_rest_view AS
WITH json_view AS (
    SELECT from_json(
        json_raw.data,
        'ARRAY<STRUCT<round:STRING,date:STRING,time:STRING,team1:STRING,team2:STRING,score:STRUCT<ht:ARRAY<INT>,ft:ARRAY<INT>>>>'
    ) AS array
    FROM (
        SELECT java_method(
            'org.spark.service.rest.QueryRESTDataService',
            'getRESTDataDocument',
            'http://localhost:8093/DSA-NoSQL-MongoDBService/rest/football/MatchesView'
        ) AS data
    ) json_raw
)
SELECT v.*
FROM json_view
LATERAL VIEW explode(json_view.array) AS v;


CREATE OR REPLACE VIEW matches_mapped_view AS
SELECT
    m.round,
    m.date,
    m.time,
    m.team1,
    m.team2,
    m.score.ht AS ht_score,
    m.score.ft AS ft_score,

    a1.canonicalName AS home_canonical_name,
    a2.canonicalName AS away_canonical_name,

    t1.teamApiId AS home_team_api_id,
    t1.teamFifaApiId AS home_team_fifa_api_id,
    t1.teamLongName AS home_team_long_name,
    t1.teamShortName AS home_team_short_name,

    t2.teamApiId AS away_team_api_id,
    t2.teamFifaApiId AS away_team_fifa_api_id,
    t2.teamLongName AS away_team_long_name,
    t2.teamShortName AS away_team_short_name
FROM matches_rest_view m
LEFT JOIN team_alias_rest_view a1
    ON upper(m.team1) = upper(a1.sourceName)
LEFT JOIN team_alias_rest_view a2
    ON upper(m.team2) = upper(a2.sourceName)
LEFT JOIN teams_rest_view t1
    ON upper(t1.teamLongName) = upper(coalesce(a1.canonicalName, m.team1))
LEFT JOIN teams_rest_view t2
    ON upper(t2.teamLongName) = upper(coalesce(a2.canonicalName, m.team2));


CREATE OR REPLACE VIEW fb_matches_mapped AS
SELECT
    f.match_date,
    f.match_time,
    f.round,
    f.home_team,
    f.away_team,
    f.ht_home,
    f.ht_away,
    f.ft_home,
    f.ft_away,
    f.total_goals,
    f.result,
    f.match_state,
    th.teamApiId AS home_team_api_id,
    th.teamLongName AS home_team_pg,
    ta.teamApiId AS away_team_api_id,
    ta.teamLongName AS away_team_pg
FROM fb_fact_matches f
LEFT JOIN (
    SELECT upper(sourceName) AS source_name,
           upper(canonicalName) AS canonical_name
    FROM team_alias_rest_view
) ah
    ON upper(f.home_team) = ah.source_name
LEFT JOIN (
    SELECT upper(sourceName) AS source_name,
           upper(canonicalName) AS canonical_name
    FROM team_alias_rest_view
) aa
    ON upper(f.away_team) = aa.source_name
LEFT JOIN fb_dim_teams th
    ON th.team_name_norm = coalesce(ah.canonical_name, upper(regexp_replace(f.home_team, ' FC$', '')))
LEFT JOIN fb_dim_teams ta
    ON ta.team_name_norm = coalesce(aa.canonical_name, upper(regexp_replace(f.away_team, ' FC$', '')));