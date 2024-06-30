import numpy as np
import pandas as pd

rename_index = {
    # "team_standings.outcome_totals.percentage": "averageWinningPct",
    "clinched_playoffs": "playoffAppearances",
    # "team_standings.points_for": "pointsForAverage",
    # "team_standings.points_against": "pointsAgainstAverage",
    "champion_ind": "numberChampionships",
    "runner_up_ind": "numberRunnerUps",
    "super_bowl_ind": "numberChampionships",
    "managers.manager.nickname": "team",
    "Tm": "team",
    "playoff_appearance_ind": "playoffAppearances",
}


def create_yahoo_aggregation_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Create final aggregation table using Yahoo Fantasy Football data.
    The output of this df is meant to be passed to the k-nearest neighbors function

    Args:
        df (pd.DataFrame): DataFrame with raw data from Yahoo Fantasy Football API

    Returns:
        pd.DataFrame: Aggregated data frame to pass to k-nearest neighbors algo
    """

    ## Determine if someone won the league or was runner-up
    df["champion_ind"] = np.where(df["team_standings.rank"] == 1, 1, 0)
    df["runner_up_ind"] = np.where(df["team_standings.rank"] == 2, 1, 0)

    ## Determine how many games someone played
    df["games_played"] = (
        df["team_standings.outcome_totals.losses"]
        + df["team_standings.outcome_totals.ties"]
        + df["team_standings.outcome_totals.wins"]
    )

    ## Points per Game
    ### Helps normalize for years with varying regular season lengths
    # df["avg_points_for"] = df["team_standings.points_for"] / df["games_played"]
    # df["avg_points_against"] = df["team_standings.points_against"] / df["games_played"]

    ## Create Raw Aggregation table

    df_agg = df.groupby("managers.manager.nickname").agg(
        {
            "team_standings.outcome_totals.wins": "sum",
            "team_standings.outcome_totals.losses": "sum",
            "team_standings.outcome_totals.ties": "sum",
            "games_played": "sum",
            "team_standings.points_for": "sum",
            "team_standings.points_against": "sum",
            "team_standings.outcome_totals.percentage": "mean",
            "clinched_playoffs": "sum",
            "champion_ind": "sum",
            "runner_up_ind": "sum",
        }
    )

    df_agg = (
        df_agg.assign(
            averageWinningPct=lambda x: (
                df_agg["team_standings.outcome_totals.wins"] + (df_agg["team_standings.outcome_totals.ties"] * 0.5)
            )
            / df_agg["games_played"],
            pointsForAverage=lambda x: df_agg["team_standings.points_for"] / df_agg["games_played"],
            pointsAgainstAverage=lambda x: df_agg["team_standings.points_against"] / df_agg["games_played"],
        )
        .reset_index()
        .rename(columns=rename_index)
    )

    return df_agg


def create_nfl_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    df_agg = df.groupby("Tm").agg(
        {
            "W": "sum",
            "L": "sum",
            "T": "sum",
            "PF": "sum",
            "PA": "sum",
            "super_bowl_ind": "sum",
            "runner_up_ind": "sum",
            "playoff_appearance_ind": "sum",
            "games_played": "sum",
        }
    )

    df_agg = (
        df_agg.assign(
            averageWinningPct=lambda x: (df_agg["W"] + (df_agg["T"] * 0.5)) / df_agg["games_played"],
            pointsForAverage=lambda x: df_agg["PF"] / df_agg["games_played"],
            pointsAgainstAverage=lambda x: df_agg["PA"] / df_agg["games_played"],
        )
        .reset_index()
        .rename(columns=rename_index)
    )

    return df_agg
