import json

import pandas as pd
from yfpy.query import YahooFantasySportsQuery  # noqa

league_key_dict = {
    2023: 690306,
    2022: 730871,
    2021: 586801,
    2020: 542510,
    2019: 104313,
    2018: 154043,
    2017: 97976,
    2016: 286064,
    2015: 244951,
    2014: 228755,
}


### Create game key dictionary by season
def create_game_id_dict(
    min_year: int,
    max_year: int,
    auth_dir: str,
    game_code: str = "nfl",
) -> dict:
    """Game ID of selected Yahoo fantasy game corresponding to a specific year, and defaulting to the current year.

    Args:
        min_year (int): Minimum year you want league data
        max_year (int): Maximum year you want league data
        auth_dir (str): Path to local directory containing yahoo API keys
        game_code (str, optional): Just choose NFL. Defaults to "nfl".

    Returns:
        dict: list of key: value pairs for year: game_id
    """
    yahoo = YahooFantasySportsQuery(
        auth_dir,
        league_id=690306,
        game_code=game_code,
        offline=False,
        all_output_as_json_str=True,
    )

    game_id_dict = {}

    for year in range(min_year, max_year + 1):
        game_id_dict[year] = yahoo.get_game_key_by_season(year)

    return game_id_dict


### Season summary
def get_season_results_by_team(min_year: int, max_year: int, auth_dir: str, game_code: str = "nfl") -> pd.DataFrame:
    """Query the Yahoo Fantasy Football API to return seasonal summaries. The following data are returned...
        1. Winning PCT
        2. Points for average by season
        3. Points against avg by season
        4. Fantasy bowl victories
        5. Fantasy bowl runner ups
        6. Playoff appearances

    Args:
        min_year (int): Minimum year you want league data
        max_year (int): Maximum year you want league data
        auth_dir (str): Path to local directory containing yahoo API keys
        game_code (str, optional): Just choose NFL. Defaults to "nfl".

    Returns:
        pd.DataFrame: Pandas data frame with the aforementioned data points
    """

    year_list = create_game_id_dict(min_year, max_year, auth_dir)

    select_cols = [
        "name",
        "number_of_moves",
        "number_of_trades",
        "team_id",
        # "managers.manager.image_url",
        # "managers.manager.manager_id",
        "managers.manager.nickname",
        # "team_logos.team_logo.url",
        "team_points.season",
        "team_standings.outcome_totals.losses",
        "team_standings.outcome_totals.percentage",
        "team_standings.outcome_totals.ties",
        "team_standings.outcome_totals.wins",
        "team_standings.points_against",
        "team_standings.points_for",
        "team_standings.rank",
    ]

    team_results_by_year = []

    for game_id in year_list.values():
        year = [i for i in year_list if year_list[i] == game_id][0]
        yahoo = YahooFantasySportsQuery(
            auth_dir,
            league_id=league_key_dict[year],
            game_code=game_code,
            game_id=game_id,
            offline=False,
            all_output_as_json_str=True,
        )

        yahoo.league_key = f"{game_id}.l.{league_key_dict[year]}"

        team_ids = get_teams_by_year(year, year, auth_dir)

        for id in range(len(team_ids)):
            teams = yahoo.get_team_info(team_ids.loc[id, "team_id"])
            teams_json = json.loads(teams)
            df_results = pd.json_normalize(teams_json)[select_cols]
            df_results["clinched_playoffs"] = team_ids.loc[id, "clinched_playoffs"]
            df_results["managers.manager.nickname"] = df_results["managers.manager.nickname"].str.upper()

            team_results_by_year.append(df_results)

    df_return = pd.concat(team_results_by_year)

    return df_return


### Get All teams by number of year
def get_teams_by_year(
    min_year: int,
    max_year: int,
    auth_dir: str,
    game_code: str = "nfl",
) -> pd.DataFrame:
    """Generate team names and coaches by year

    Args:
         min_year (int): Minimum year you want league data
         max_year (int): Maximum year you want league data
         auth_dir (str): Path to local directory containing yahoo API keys
         game_code (str, optional): Just choose NFL. Defaults to "nfl".

     Returns:
         pd.DataFrame: Name, manager, playoff status, and draft_grade by year
    """
    year_list = create_game_id_dict(min_year, max_year, auth_dir)

    select_cols = [
        "name",
        "managers.manager.nickname",
        "clinched_playoffs",
        "draft_grade",
        "team_id",
    ]

    teams_by_year = []

    for game_id in year_list.values():
        year = [i for i in year_list if year_list[i] == game_id][0]
        yahoo = YahooFantasySportsQuery(
            auth_dir,
            league_id=league_key_dict[year],
            game_code=game_code,
            game_id=game_id,
            offline=False,
            all_output_as_json_str=True,
        )

        yahoo.league_key = f"{game_id}.l.{league_key_dict[year]}"

        teams = yahoo.get_league_teams()

        teams_json = json.loads(teams)
        df_teams = pd.json_normalize(teams_json)
        df_teams = df_teams[select_cols].copy()
        df_teams["year"] = year
        df_teams["managers.manager.nickname"] = df_teams["managers.manager.nickname"].str.upper()
        df_teams["clinched_playoffs"] = df_teams["clinched_playoffs"].fillna(0)

        teams_by_year.append(df_teams)

    df_return = pd.concat(teams_by_year)

    return df_return


### Weekly results by year (W/L, make playoffs, etc.)
def get_weekly_matchup_results(
    min_year: int,
    max_year: int,
    auth_dir: str,
    game_code: str = "nfl",
) -> pd.DataFrame:
    """Generate data frame using Yahoo Fantasy Football API with weekly results for list of years selected.

    Args:
        min_year: int,
        max_year: int,
        auth_dir: str,
        game_code: str = "nfl",

    Returns:
        pd.DataFrame: Data frame with weekly results for selected years
    """
    weekly_matchups = []
    select_cols = [
        "team.managers.manager.nickname",
        "team.name",
        "team.team_id",
        "team.team_key",
        "team.team_points.total",
        "team.team_projected_points.total",
        "team.team_points.week",
    ]

    year_list = create_game_id_dict(min_year, max_year, auth_dir)

    for game_id in year_list.values():
        year = [i for i in year_list if year_list[i] == game_id][0]
        print(f"Building matchup dataset for {year}")
        yahoo = YahooFantasySportsQuery(
            auth_dir,
            league_id=league_key_dict[year],
            game_code=game_code,
            game_id=game_id,
            offline=False,
            all_output_as_json_str=True,
        )

        yahoo.league_key = f"{game_id}.l.{league_key_dict[year]}"

        num_weeks = yahoo.get_game_weeks_by_game_id(game_id)
        num_weeks_json = json.loads(num_weeks)
        season_length = len(num_weeks_json)

        for week in range(1, season_length):
            week_number = week
            matchups = yahoo.get_league_scoreboard_by_week(week)
            matchups_json = json.loads(matchups)

            for matchup in range(0, len(matchups_json["matchups"])):
                df_tmp = pd.json_normalize(
                    matchups_json["matchups"][matchup]["matchup"]["teams"],
                    errors="ignore",
                )
                df_tmp = df_tmp[select_cols].copy()
                df_tmp.columns = [col[len("team.") :] for col in df_tmp.columns]
                df_tmp["year"] = year
                df_tmp["week"] = week_number
                df_tmp["matchup_id"] = matchup

                weekly_matchups.append(df_tmp)

    df_return = pd.concat(weekly_matchups)

    return df_return
