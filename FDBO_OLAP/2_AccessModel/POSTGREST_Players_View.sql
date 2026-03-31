SELECT HTTPURITYPE.createuri('http://localhost:3000/players').getclob() AS doc
FROM dual;

CREATE OR REPLACE VIEW players_view AS
WITH rest_doc AS (
    SELECT HTTPURITYPE.createuri('http://localhost:3000/players').getclob() AS doc
    FROM dual
)
SELECT
    player_api_id,
    player_fifa_api_id,
    player_name,
    birthday,
    height,
    weight
FROM JSON_TABLE(
    (SELECT doc FROM rest_doc),
    '$[*]'
    COLUMNS (
        player_api_id       NUMBER        PATH '$.player_api_id',
        player_fifa_api_id  NUMBER        PATH '$.player_fifa_api_id',
        player_name         VARCHAR2(150) PATH '$.player_name',
        birthday            VARCHAR2(50)  PATH '$.birthday',
        height              NUMBER        PATH '$.height',
        weight              NUMBER        PATH '$.weight'
    )
);