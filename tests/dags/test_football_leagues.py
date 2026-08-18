import pandas as pd
from airflow.models import DagBag
from unittest.mock import patch
from utils import clean_team_name

def test_dag_imports():
    dag_bag = DagBag(include_examples=False)

    assert not dag_bag.import_errors, (
        f"El DAG tiene errores de importación: {dag_bag.import_errors}"
    )

def test_dag_structure():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("football_leagues")

    expected_tasks = {
        "update_team_table",
        "extract_league",
        "upload_data_stage",
        "truncate_staging",
        "ingest_table",
        "merge_table",
        "remove_stage_file",
    }

    assert set(dag.task_ids) == expected_tasks

def test_dag_dependencies():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("football_leagues")

    assert dag.get_task("extract_league").upstream_task_ids == {
        "update_team_table"
    }

    assert dag.get_task("upload_data_stage").upstream_task_ids == {
        "extract_league"
    }

    assert dag.get_task("truncate_staging").upstream_task_ids == {
        "upload_data_stage"
    }

    assert dag.get_task("ingest_table").upstream_task_ids == {
        "truncate_staging"
    }

    assert dag.get_task("merge_table").upstream_task_ids == {
        "ingest_table"
    }

    assert dag.get_task("remove_stage_file").upstream_task_ids == {
        "merge_table"
    }

def test_dag_schedule():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("football_leagues")

    assert dag.schedule == "0 7 * * 1,4"
    assert str(dag.timezone) == "America/Bogota"

def test_clean_team_name():
    # Removes position and duplicated team name
    assert clean_team_name("1BarcelonaBarcelona") == "Barcelona"

    # Removes position
    assert clean_team_name("4Real Betis") == "Real Betis"

    # Preserves already-clean names
    assert clean_team_name("Girona FC") == "Girona FC"

    # Removes 3-letter ESPN code
    assert clean_team_name("BARBarcelona") == "Barcelona"

    # Removes 4-letter ESPN code
    assert clean_team_name("BARCBarcelona") == "Barcelona"

def test_leagues_file():
    df = pd.read_csv("/usr/local/airflow/df_ligas.csv")

    assert not df.empty
    assert set(df.columns) == {"LIGA", "URL"}
    assert df["LIGA"].notna().all()
    assert df["URL"].notna().all()
    assert df["LIGA"].is_unique

@patch("utils.pd.read_html")
def test_get_data(mock_read_html):
    mock_read_html.return_value = [
        pd.DataFrame({
            0: [
                "1ESPEspanyol",
                "2ALAAlavés",
            ]
        }),
        pd.DataFrame({
            0: [1, 1],
            1: [1, 1],
            2: [0, 0],
            3: [0, 0],
            4: [3, 3],
            5: [0, 0],
            6: [3, 3],
            7: [3, 3],
        }),
    ]

    from utils import get_data

    df = get_data(
        "https://example.com",
        "ESPAÑA",
        0,
        0
    )

    expected_columns = {
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
        "CREATED_AT",
    }

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df["LIGA"].eq("ESPAÑA").all()
    assert df["CREATED_AT"].notna().all()

    assert "Espanyol" in df["EQUIPO"].values
    assert "Alavés" in df["EQUIPO"].values

    mock_read_html.assert_called_once_with("https://example.com")

@patch("utils.pd.read_html")
def test_build_team_table(mock_read_html):
    mock_read_html.return_value = [
        pd.DataFrame({
            0: [
                "1ESPEspanyol",
                "2ALAAlavés",
                "3ESPEspanyol",
            ]
        }),
        pd.DataFrame({
            0: [1, 1, 1],
            1: [1, 1, 1],
            2: [0, 0, 0],
            3: [0, 0, 0],
            4: [3, 3, 3],
            5: [0, 0, 0],
            6: [3, 3, 3],
            7: [3, 3, 3],
        }),
    ]

    from utils import build_team_table

    leagues = pd.DataFrame({
        "LIGA": ["ESPAÑA"],
        "URL": ["https://example.com"],
    })

    existing_team_table = pd.DataFrame({
        "EQUIPO": ["Espanyol"],
        "ID_TEAM": ["existing123"],
    })

    team_table = build_team_table(
        leagues,
        existing_team_table
    )

    assert not team_table.empty

    assert set(team_table.columns) == {
        "EQUIPO",
        "ID_TEAM",
    }

    assert team_table["EQUIPO"].is_unique

    assert "Espanyol" in team_table["EQUIPO"].values
    assert "Alavés" in team_table["EQUIPO"].values

    espanyol_id = team_table.loc[
        team_table["EQUIPO"] == "Espanyol",
        "ID_TEAM"
    ].iloc[0]

    alaves_id = team_table.loc[
        team_table["EQUIPO"] == "Alavés",
        "ID_TEAM"
    ].iloc[0]

    assert espanyol_id == "existing123"
    assert len(alaves_id) == 8

    mock_read_html.assert_called_once_with("https://example.com")

def test_extract_league():
    import importlib.util

    dag_path = "/usr/local/airflow/dags/football_leagues/football_leagues.py"

    spec = importlib.util.spec_from_file_location(
        "football_leagues_dag",
        dag_path
    )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    extract_league = module.extract_league

    team_table = pd.DataFrame({
        "EQUIPO": ["Espanyol", "Alavés"],
        "ID_TEAM": ["id123456", "id789012"],
    })

    data = pd.DataFrame({
        "EQUIPO": ["Espanyol", "Alavés"],
        "JUGADOS": [1, 1],
        "GANADOS": [1, 1],
        "EMPATADOS": [0, 0],
        "PERDIDOS": [0, 0],
        "GOLES_A_FAVOR": [3, 3],
        "GOLES_EN_CONTRA": [0, 0],
        "DIFERENCIA": [3, 3],
        "PUNTOS": [3, 3],
        "LIGA": ["ESPAÑA", "ESPAÑA"],
        "CREATED_AT": ["2026-08-17", "2026-08-17"],
    })

    league_info = {
        "LIGA": "ESPAÑA",
        "URL": "https://example.com",
    }

    context = {
        "ts_nodash": "20260817T070000"
    }

    with patch.object(module.pd, "read_csv", return_value=team_table), \
         patch.object(module, "get_data", return_value=data), \
         patch.object(module, "get_current_context", return_value=context), \
         patch.object(pd.DataFrame, "to_csv") as mock_to_csv:

        result = extract_league(league_info)

    expected_filename = (
        "football_positions_"
        "20260817T070000_"
        "españa.csv"
    )

    assert result["filename"] == expected_filename

    mock_to_csv.assert_called_once_with(
        "/usr/local/airflow/football_positions_20260817T070000_españa.csv",
        index=False
    )

def test_merge_table_sql():
    with open(
        "/usr/local/airflow/dags/football_leagues/queries/merge_table.sql"
    ) as file:
        sql = file.read()

    assert "MERGE INTO LEAGUES.PUBLIC.football_leagues AS target" in sql
    assert "USING LEAGUES.PUBLIC.football_leagues_staging AS source" in sql
    assert "ON target.ID_TEAM = source.ID_TEAM" in sql

    assert "WHEN MATCHED THEN" in sql
    assert "WHEN NOT MATCHED THEN" in sql

    expected_columns = [
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
        "CREATED_AT",
    ]

    for column in expected_columns:
        assert column in sql

def test_upload_stage_sql():
    with open(
        "/usr/local/airflow/dags/football_leagues/queries/upload_stage.sql"
    ) as file:
        sql = file.read()

    assert "PUT" in sql

    assert (
        "file:///usr/local/airflow/"
        "football_positions_{{ ts_nodash }}_*.csv"
    ) in sql

    assert "@LEAGUES.PUBLIC.{{ params.stage }}" in sql

    assert "AUTO_COMPRESS=true" in sql

def test_truncate_staging_sql():
    with open(
        "/usr/local/airflow/dags/football_leagues/queries/truncate_staging.sql"
    ) as file:
        sql = file.read().strip()

    assert sql == (
        "TRUNCATE TABLE LEAGUES.PUBLIC.football_leagues_staging;"
    )

def test_upload_table_sql():
    with open(
        "/usr/local/airflow/dags/football_leagues/queries/upload_table.sql"
    ) as file:
        sql = file.read()

    assert "COPY INTO LEAGUES.PUBLIC.football_leagues_staging" in sql

    assert "FROM @LEAGUES.PUBLIC.{{ params.stage }}" in sql

    assert "TYPE=CSV" in sql
    assert "FIELD_DELIMITER=','" in sql
    assert "SKIP_HEADER=1" in sql

    assert (
        "PATTERN='.*football_positions_{{ ts_nodash }}_.*\\.csv\\.gz'"
    ) in sql

    assert "ON_ERROR='ABORT_STATEMENT'" in sql

def test_remove_stage_file_sql():
    with open(
        "/usr/local/airflow/dags/football_leagues/queries/remove_stage_file.sql"
    ) as file:
        sql = file.read()

    assert "REMOVE @LEAGUES.PUBLIC.{{ params.stage }}" in sql

    assert (
        "PATTERN='.*football_positions_{{ ts_nodash }}_.*\\.csv\\.gz'"
    ) in sql