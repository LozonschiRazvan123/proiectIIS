SELECT HTTPURITYPE.createuri('http://localhost:3000/teams').getclob() AS doc
FROM dual;

WITH rest_doc AS (
    SELECT HTTPURITYPE.createuri('http://localhost:3000/teams').getclob() AS doc
    FROM dual
)
SELECT
    team_api_id,
    team_fifa_api_id,
    team_long_name,
    team_short_name
FROM JSON_TABLE(
    (SELECT doc FROM rest_doc),
    '$[*]'
    COLUMNS (
        team_api_id       NUMBER        PATH '$.team_api_id',
        team_fifa_api_id  NUMBER        PATH '$.team_fifa_api_id',
        team_long_name    VARCHAR2(150) PATH '$.team_long_name',
        team_short_name   VARCHAR2(50)  PATH '$.team_short_name'
    )
);