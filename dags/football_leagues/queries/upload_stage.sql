PUT file:///usr/local/airflow/football_positions_{{ ts_nodash }}_*.csv
@LEAGUES.PUBLIC.{{ params.stage }}
AUTO_COMPRESS=true;