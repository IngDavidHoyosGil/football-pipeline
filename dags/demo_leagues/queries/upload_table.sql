COPY INTO LEAGUES.PUBLIC.{{ params.table }}
FROM @LEAGUES.PUBLIC.{{ params.stage }}/premier_positions.csv.gz
FILE_FORMAT=(
    TYPE=CSV
    FIELD_DELIMITER=','
    SKIP_HEADER=1
)
ON_ERROR='CONTINUE';