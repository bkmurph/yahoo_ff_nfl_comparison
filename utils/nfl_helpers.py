import numpy as np
import pandas as pd

team_rename_map = {
    "WASHINGTON REDSKINS": "WASHINGTON COMMIES",
    "WASHINGTON COMMANDERS": "WASHINGTON COMMIES",
    "WASHINGTON FOOTBALL TEAM": "WASHINGTON COMMIES",
    "OAKLAND RAIDERS": "RAIDERS",
    "LAS VEGAS RAIDERS": "RAIDERS",
}


def clean_nfl_scoring_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up data that is processed in the get_nfl_team_scoring data function. This functions does several things...
        1. Fill ties with 0 where it is null. Some years have no ties so it doesn't populate in the data
        2. Add in the "games played" feature.
        3. Use games played to determine points scored/allowed per game
        4. Map stupid teams like Washington / Raiders to a common name

    Args:
        df (pd.DataFrame): NFL Seasonal Data by team returned by get_nfl_te

    Returns:
        pd.DataFrame: Cleaned up pandas data frame
    """

    df["T"] = df["T"].fillna(0)
    df[["W", "L", "T", "PF", "PA", "W-L%"]] = df[["W", "L", "T", "PF", "PA", "W-L%"]].astype("Float64")
    df["games_played"] = df["W"] + df["L"] + df["T"]

    # df["pointsForPerGame"] = df["PF"] / df["games_played"]
    # df["pointsAgainstPerGame"] = df["PA"] / df["games_played"]

    # Clean Up Stupid ass team names
    df["Tm"] = np.where(
        df["Tm"].isin([key for key in team_rename_map]), df["Tm"].map(team_rename_map).fillna(df["Tm"]), df["Tm"]
    )

    df = df.reset_index(drop=True).copy()

    return df


def get_playoff_data(years: list) -> dict:
    """
    This function will generate three things by year...
        1. Which team one the Super Bowl
        2. Which team lost the Super Bowl
        3. Which teams made the playoffs

    The data is pulled from Pro Football Reference.

    Args:
        years (list): A list of years that you want playoff information for

    Returns:
        dict: Playoff information for each year passed to years
    """
    playoff_data = {}
    for year in years:
        url = f"https://www.pro-football-reference.com/years/{year}/games.htm"
        playoffs = pd.read_html(url)[0]
        # We only want games that occurred in the playoff rounds
        playoffs = playoffs[playoffs["Week"].isin(["WildCard", "Division", "ConfChamp", "SuperBowl"])].copy()
        playoffs["Winner/tie"] = playoffs["Winner/tie"].str.replace("[^\w\s]", "", regex=True).str.upper()
        playoffs["Loser/tie"] = playoffs["Loser/tie"].str.replace("[^\w\s]", "", regex=True).str.upper()

        playoff_teams = set(playoffs["Winner/tie"])
        playoff_losers = set(playoffs["Loser/tie"])
        playoff_teams.update(playoff_losers)
        super_bowl_winner = set(playoffs[playoffs["Week"] == "SuperBowl"]["Winner/tie"])
        super_bowl_runner_up = set(playoffs[playoffs["Week"] == "SuperBowl"]["Loser/tie"])

        playoff_data[year] = {
            "playoff_teams": list(playoff_teams),
            "super_bowl_winner": list(super_bowl_winner),
            "super_bowl_runner_up": list(super_bowl_runner_up),
        }

    return playoff_data


def create_playoff_ind_columns(df: pd.DataFrame, years: list):
    """This function iterates through a data frame with seasonal NFL data by team and determines whether the team
    won the Super Bowl, lost the Super Bowl, or made the playoffs.

    Args:
        df (pd.DataFrame): NFL Seasonal Data by team returned by get_nfl_team_scoring_data function
        years (list): A list of years that you want playoff information for

    Returns:
        _super_bowl: Array of boolean indicators determining if a team won the Super Bowl
        _runner_up: Array of boolean indicators determining if a team lost the Super Bowl
        _playoffs: Array of boolean indicators determining if a team made the playoffs
    """
    playoffs = get_playoff_data(years)
    _super_bowl = []
    _runner_up = []
    _playoffs = []

    for i in range(len(df)):
        year = df.loc[i, "season"]
        super_bowl_ind = np.where(df.loc[i, "Tm"] in playoffs[year]["super_bowl_winner"], 1, 0)
        runner_up_ind = np.where(df.loc[i, "Tm"] in playoffs[year]["super_bowl_runner_up"], 1, 0)
        playoffs_ind = np.where(df.loc[i, "Tm"] in (playoffs[year]["playoff_teams"]), 1, 0)

        _super_bowl.append(super_bowl_ind)
        _runner_up.append(runner_up_ind)
        _playoffs.append(playoffs_ind)

    return _super_bowl, _runner_up, _playoffs


def get_nfl_team_scoring_data(years: list) -> pd.DataFrame:
    """Return data frame that will be used to aggregate NFL comparison by season. Looking for the following...
        1. Winning PCT
        2. Points for average by season
        3. Points against avg by season
        4. Super Bowl victories
        5. Super Bowl runner ups
        6. Playoff appearances

    Args:
        years (list): A list of years that you want seasonal information for

    Returns:
        pd.DataFrame: Data frame that contains the information referenced in the description
    """
    season_data = []
    for year in years:
        print(f"Creating regular season data for {year}")
        url = f"https://www.pro-football-reference.com/years/{year}/#team_scoring"
        df_afc = pd.read_html(url)[0]
        df_nfc = pd.read_html(url)[1]

        df_nfl = pd.concat([df_afc, df_nfc])
        df_nfl = df_nfl[~df_nfl["W"].str.contains("AFC|NFC", case=False)].copy()
        df_nfl["season"] = year
        df_nfl["Tm"] = df_nfl["Tm"].str.replace("[^\w\s]", "", regex=True).str.upper()

        season_data.append(df_nfl)

    # Aggregate the nfl scoring data by season
    df = pd.concat(season_data).reset_index(drop=True)

    # Get super bowl, runner up, and playoff appearance indicators
    print("Creating playoff performance indicator columns")
    _super_bowl, _runner_up, _playoffs = create_playoff_ind_columns(df, years)

    # Add Indicator Columns to main df
    df["super_bowl_ind"] = _super_bowl
    df["runner_up_ind"] = _runner_up
    df["playoff_appearance_ind"] = _playoffs

    print("Cleaning up data before returning final DF")
    df_return = clean_nfl_scoring_data(df)

    return df_return
