COPY INTO LEAGUES.PUBLIC.football_leagues_staging
FROM @LEAGUES.PUBLIC.{{ params.stage }}
FILE_FORMAT=(
    TYPE=CSV
    FIELD_DELIMITER=','
    SKIP_HEADER=1
)
PATTERN='.*football_positions_{{ ts_nodash }}_.*\.csv\.gz'
ON_ERROR='ABORT_STATEMENT';