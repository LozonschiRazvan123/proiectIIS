BEGIN
  ORDS.ENABLE_SCHEMA(
    p_enabled => TRUE,
    p_schema => 'FDBO',
    p_url_mapping_type => 'BASE_PATH',
    p_url_mapping_pattern => 'fdbo',
    p_auto_rest_auth => FALSE
  );
  COMMIT;
END;
/



BEGIN
  ORDS.ENABLE_OBJECT(
    p_enabled => TRUE,
    p_schema => 'FDBO',
    p_object => 'FB_FACT_MATCHES',
    p_object_type => 'VIEW',
    p_object_alias => 'fb_fact_matches',
    p_auto_rest_auth => FALSE
  );

  ORDS.ENABLE_OBJECT(
    p_enabled => TRUE,
    p_schema => 'FDBO',
    p_object => 'FB_FACT_TEAM_MATCHES',
    p_object_type => 'VIEW',
    p_object_alias => 'fb_fact_team_matches',
    p_auto_rest_auth => FALSE
  );

  ORDS.ENABLE_OBJECT(
    p_enabled => TRUE,
    p_schema => 'FDBO',
    p_object => 'FB_MATCHES_MAPPED',
    p_object_type => 'VIEW',
    p_object_alias => 'fb_matches_mapped',
    p_auto_rest_auth => FALSE
  );

  ORDS.ENABLE_OBJECT(
    p_enabled => TRUE,
    p_schema => 'FDBO',
    p_object => 'FB_AGG_TEAM_MONTH',
    p_object_type => 'VIEW',
    p_object_alias => 'fb_agg_team_month',
    p_auto_rest_auth => FALSE
  );

  COMMIT;
END;
/




BEGIN
  ORDS.DEFINE_MODULE(
    p_module_name => 'fdbo.football.api',
    p_base_path => '/football/',
    p_items_per_page => 25,
    p_status => 'PUBLISHED'
  );

  ORDS.DEFINE_TEMPLATE(
    p_module_name => 'fdbo.football.api',
    p_pattern => 'matches'
  );

  ORDS.DEFINE_HANDLER(
    p_module_name => 'fdbo.football.api',
    p_pattern => 'matches',
    p_method => 'GET',
    p_source_type => 'json/collection',
    p_source => 'SELECT * FROM FB_MATCHES_MAPPED'
  );

  ORDS.DEFINE_TEMPLATE(
    p_module_name => 'fdbo.football.api',
    p_pattern => 'team-month'
  );

  ORDS.DEFINE_HANDLER(
    p_module_name => 'fdbo.football.api',
    p_pattern => 'team-month',
    p_method => 'GET',
    p_source_type => 'json/collection',
    p_source => 'SELECT * FROM FB_AGG_TEAM_MONTH'
  );

  ORDS.DEFINE_TEMPLATE(
    p_module_name => 'fdbo.football.api',
    p_pattern => 'team-standings'
  );

  ORDS.DEFINE_HANDLER(
    p_module_name => 'fdbo.football.api',
    p_pattern => 'team-standings',
    p_method => 'GET',
    p_source_type => 'json/collection',
    p_source => '
      SELECT team_name,
             SUM(points) AS total_points,
             SUM(goals_for) - SUM(goals_against) AS goal_diff,
             RANK() OVER (
               ORDER BY SUM(points) DESC,
                        SUM(goals_for) - SUM(goals_against) DESC
             ) AS rank_pos
      FROM FB_FACT_TEAM_MATCHES
      WHERE match_state = ''played''
      GROUP BY team_name
    '
  );

  COMMIT;
END;
/



SELECT *
FROM FB_MATCHES_MAPPED;

-- Page 2: Interactive Grid - Fact Team Matches
SELECT *
FROM FB_FACT_TEAM_MATCHES;

-- Page 3: Chart - Points by Team
SELECT
  team_name,
  SUM(points) AS total_points
FROM FB_FACT_TEAM_MATCHES
WHERE match_state = 'played'
GROUP BY team_name
ORDER BY total_points DESC;

-- Page 4: Chart - Goals by Month
SELECT
  year_val,
  month_val,
  SUM(goals_for) AS goals_for
FROM FB_AGG_TEAM_MONTH
GROUP BY year_val, month_val
ORDER BY year_val, month_val;