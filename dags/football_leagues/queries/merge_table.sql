MERGE INTO LEAGUES.PUBLIC.football_leagues AS target
USING LEAGUES.PUBLIC.football_leagues_staging AS source
ON target.ID_TEAM = source.ID_TEAM

WHEN MATCHED THEN
    UPDATE SET
        target.EQUIPO = source.EQUIPO,
        target.JUGADOS = source.JUGADOS,
        target.GANADOS = source.GANADOS,
        target.EMPATADOS = source.EMPATADOS,
        target.PERDIDOS = source.PERDIDOS,
        target.GOLES_A_FAVOR = source.GOLES_A_FAVOR,
        target.GOLES_EN_CONTRA = source.GOLES_EN_CONTRA,
        target.DIFERENCIA = source.DIFERENCIA,
        target.PUNTOS = source.PUNTOS,
        target.LIGA = source.LIGA,
        target.CREATED_AT = source.CREATED_AT

WHEN NOT MATCHED THEN
    INSERT (
        ID_TEAM,
        EQUIPO,
        JUGADOS,
        GANADOS,
        EMPATADOS,
        PERDIDOS,
        GOLES_A_FAVOR,
        GOLES_EN_CONTRA,
        DIFERENCIA,
        PUNTOS,
        LIGA,
        CREATED_AT
    )
    VALUES (
        source.ID_TEAM,
        source.EQUIPO,
        source.JUGADOS,
        source.GANADOS,
        source.EMPATADOS,
        source.PERDIDOS,
        source.GOLES_A_FAVOR,
        source.GOLES_EN_CONTRA,
        source.DIFERENCIA,
        source.PUNTOS,
        source.LIGA,
        source.CREATED_AT
    );