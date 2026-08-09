from datetime import datetime, timedelta

import pandas as pd

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from utils import data_processing


default_arguments = {
                        "owner": "data_engineering",
                        "retries":1 ,
                        "retry_delay":timedelta(minutes=5)
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
        df_team = pd.read_csv("/usr/local/airflow/team_table.csv")

        def extract_info(df ,df_team):
            df_data = data_processing(df)

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
                    "J",
                    "G",
                    "E", 
                    "P", 
                    "GF", 
                    "GC", 
                    "DIF", 
                    "PTS", 
                    "LIGA",
                    "CREATED_AT"
                ]
            ]

            df_final.to_csv(
                    "/usr/local/airflow/premier_positions.csv",
                    index=False
            )


        extract_data = PythonOperator(
                                task_id="extract_and_transform_football_data",
                                python_callable=extract_info,
                                op_kwargs={"df":df,"df_team":df_team}
                                )

        upload_stage = SQLExecuteQueryOperator(
                task_id="upload_data_stage",
                sql="./queries/upload_stage.sql",
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

        extract_data >>  upload_stage >> ingest_table