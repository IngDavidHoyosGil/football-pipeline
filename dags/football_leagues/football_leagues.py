from datetime import datetime, timedelta

import os
import pandas as pd

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from utils import build_team_table, get_data

REQUEST_DELAY_MIN = 1
REQUEST_DELAY_MAX = 3

default_arguments = {
                        "owner": "David_Hoyos",
                        "retries":1 ,
                        "retry_delay":timedelta(minutes=5)
                }

def update_team_table(leagues, current_team_table):
    new_team_table = build_team_table(
        leagues,
        existing_team_table = current_team_table
    )

    filepath = "/usr/local/airflow/team_table.csv"

    new_team_table.to_csv(
        filepath,
        index=False
    )

def extract_league(league_info):
    df_team = pd.read_csv("/usr/local/airflow/team_table.csv")

    df_data = get_data(
        league_info["URL"],
        league_info["LIGA"],
        REQUEST_DELAY_MIN,
        REQUEST_DELAY_MAX
    )

    df_final = pd.merge(
        df_data,
        df_team,
        how="inner",
        on="EQUIPO"
    )

    df_final = df_final[
        [
            "ID_TEAM",
            "EQUIPO",
            "JUGADOS",
            "GANADOS",
            "EMPATADOS",
            "PERDIDOS",
            "GOLES_A_FAVOR",
            "GOLES_EN_CONTRA",
            "DIFERENCIA",
            "PUNTOS",
            "LIGA",
            "CREATED_AT"
        ]
    ]

    context = get_current_context()
    run_timestamp = context["ts_nodash"]

    filename = (
        f"football_positions_"
        f"{run_timestamp}_"
        f"{league_info['LIGA'].lower().replace(' ', '_')}.csv"
    )

    filepath = f"/usr/local/airflow/{filename}"

    df_final.to_csv(
        filepath,
        index=False
    )

    return {
        "filename": filename
    } 

with DAG(
        dag_id="football_leagues",
        default_args=default_arguments,
        description="Extract and load football league data into Snowflake" ,
        start_date=datetime(2026, 8, 1),
        schedule=None,
        tags=["tabla_espn", "football", "snowflake"],
        catchup=False
        ) as dag:


        params_info = Variable.get("feature_info", deserialize_json=True)
        df = pd.read_csv("/usr/local/airflow/df_ligas.csv")
        leagues = df.to_dict("records")

        team_table_path = "/usr/local/airflow/team_table.csv"

        if os.path.exists(team_table_path):
            df_team = pd.read_csv(team_table_path)
        else:
            df_team = None

        update_team = PythonOperator(
                            task_id="update_team_table",
                            python_callable=update_team_table,
                            op_kwargs={
                                "leagues": df,
                                "current_team_table": df_team
                            }
                        )

        extract_data = PythonOperator.partial(
                            task_id="extract_league",
                            python_callable=extract_league
                        ).expand(
                            op_kwargs=[
                                {"league_info": league}
                                for league in leagues
                            ]
                        )

        upload_stage = SQLExecuteQueryOperator(
                            task_id="upload_data_stage",
                            sql="./queries/upload_stage.sql",
                            conn_id="snowflake_connection",
                            database=params_info["DB"],
                            params=params_info,
                        )

        truncate_staging = SQLExecuteQueryOperator(
                task_id="truncate_staging",
                sql="./queries/truncate_staging.sql",
                conn_id="snowflake_connection",
                database=params_info["DB"],
                params=params_info,
                )

        ingest_table = SQLExecuteQueryOperator(
                task_id="ingest_table",
                sql="./queries/upload_table.sql",
                conn_id="snowflake_connection",
                database=params_info["DB"],
                params=params_info,
                )

        merge_table = SQLExecuteQueryOperator(
                task_id="merge_table",
                sql="./queries/merge_table.sql",
                conn_id="snowflake_connection",
                database=params_info["DB"],
                params=params_info,
                )

        remove_stage_file = SQLExecuteQueryOperator(
                task_id="remove_stage_file",
                sql="./queries/remove_stage_file.sql",
                conn_id="snowflake_connection",
                database=params_info["DB"],
                params=params_info,
                )

        update_team >> extract_data >> upload_stage >> truncate_staging >> ingest_table >> merge_table >> remove_stage_file