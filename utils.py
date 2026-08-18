import random
import re
import time
import uuid
from datetime import datetime

import pandas as pd

def clean_team_name(team_name):
    # Remove the position number
    team_name = re.sub(r"^\d+", "", team_name)

    # Handle 4-letter codes that repeat the beginning of the team name
    match = re.match(r"^([A-Z]{4})([A-Z][a-z].*)$", team_name)

    if match and match.group(1).lower() == match.group(2)[:4].lower():
        team_name = match.group(2)

    # Remove 3-letter ESPN codes
    elif re.search(r"^[A-Z]{3}[A-Z]", team_name):
        team_name = team_name[3:]

    else:
        # Handle repeated team names
        for i in range(1, len(team_name)):
            first_part = team_name[:i]
            second_part = team_name[i:]

            if second_part.startswith(first_part):
                team_name = second_part
                break

    return team_name.strip()

def build_team_table(leagues, existing_team_table=None):
    teams = []

    for _, row in leagues.iterrows():
        raw = pd.read_html(row["URL"])

        df_league = pd.concat(
            [raw[0], raw[1]],
            ignore_index=True,
            axis=1
        )

        for team in df_league[0]:
            teams.append(clean_team_name(team))

    teams = sorted(set(teams))

    if existing_team_table is None:
        existing_team_table = pd.DataFrame(
            columns=["EQUIPO", "ID_TEAM"]
        )

    existing_ids = dict(
        zip(
            existing_team_table["EQUIPO"],
            existing_team_table["ID_TEAM"]
        )
    )

    team_table = pd.DataFrame({
        "EQUIPO": teams,
        "ID_TEAM": [
            existing_ids.get(team, str(uuid.uuid4())[:8])
            for team in teams
        ]
    })

    return team_table

def get_data(url, league, delay_min, delay_max):

    time.sleep(random.uniform(delay_min, delay_max))

    df = pd.read_html(url)

    df = pd.concat(
        [df[0], df[1]],
        ignore_index=True,
        axis=1
    )

    df = df.rename(
        columns={
            0: "EQUIPO",
            1: "JUGADOS", 
            2: "GANADOS", 
            3: "EMPATADOS", 
            4: "PERDIDOS", 
            5: "GOLES_A_FAVOR", 
            6: "GOLES_EN_CONTRA", 
            7: "DIFERENCIA", 
            8: "PUNTOS"
        }
    )
    df["EQUIPO"] = df["EQUIPO"].apply(clean_team_name)

    df["LIGA"] = league

    df["CREATED_AT"] = datetime.now().strftime("%Y-%m-%d")

    return df

