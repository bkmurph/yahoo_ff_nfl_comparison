import numpy as np
import pandas as pd

rename_index = {
    "team_standings.outcome_totals.percentage": "averageWinningPct",
    "clinched_playoffs": "playoffAppearances",
    "team_standings.points_for": "pointsForAverage",
    "team_standings.points_against": "pointsAgainstAverage",
    "champion_ind": "numberChampionships",
    "runner_up_ind": "numberRunnerUps",
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
    df["avg_points_for"] = df["team_standings.points_for"] / df["games_played"]
    df["avg_points_against"] = df["team_standings.points_against"] / df["games_played"]

    ## Create Raw Aggregation table

    df_agg = (
        df.groupby("managers.manager.nickname")
        .agg(
            {
                "team_standings.outcome_totals.percentage": "mean",
                "clinched_playoffs": "sum",
                "avg_points_for": "mean",
                "avg_points_against": "mean",
                "champion_ind": "sum",
                "runner_up_ind": "sum",
            }
        )
        .sort_values("team_standings.outcome_totals.percentage", ascending=False)
        .rename(columns=rename_index)
    )

    return df_agg
