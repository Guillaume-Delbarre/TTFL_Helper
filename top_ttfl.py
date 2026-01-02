from typing import Optional
import pandas as pd
from fetch_players_stats import get_players_stats, compute_ttfl_score


def fetch_players_ttfl(cache_date: Optional[str] = None, force_refresh: bool = False) -> pd.DataFrame:
    """Récupère (ou charge depuis cache) le DataFrame players+games avec `TTFL_SCORE`.

    Args:
        cache_date (Optional[str]): Date du cache au format YYYY-MM-DD.
        force_refresh (bool): Forcer la récupération depuis l'API.

    Returns:
        pandas.DataFrame: DataFrame contenant au moins `PERSON_ID`,
                          `DISPLAY_FIRST_LAST`, `GAME_DATE`, et `TTFL_SCORE`.
    """
    df = get_players_stats(cache_date=cache_date, force_refresh=force_refresh)
    # Ensure TTFL_SCORE exists
    if "TTFL_SCORE" not in df.columns:
        df = compute_ttfl_score(df)
    # Ensure GAME_DATE is datetime
    if "GAME_DATE" in df.columns:
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    return df


def top_players_by_ttfl(df: pd.DataFrame, top_n: int = 10, last_x: int = 5, min_games: int = 10) -> pd.DataFrame:
    """Calcule et renvoie les meilleurs joueurs par moyenne TTFL sur la saison
    et sur leurs `last_x` derniers matches.

    Returns a DataFrame with columns: PERSON_ID, DISPLAY_FIRST_LAST, TEAM_ABBREVIATION,
    SEASON_AVG, SEASON_VAR, N_GAMES, LASTX_AVG, LASTX_VAR, LASTX_N (number of games used).
    """
    # Season stats
    season_group = df.groupby(["PERSON_ID", "DISPLAY_FIRST_LAST", "TEAM_ABBREVIATION"], as_index=False)
    # Use sample standard deviation (ddof=1) to measure match-to-match variability
    season_stats = season_group["TTFL_SCORE"].agg(
        N_GAMES="count",
        SEASON_AVG="mean",
        SEASON_STD=lambda x: x.std(ddof=1)
    )

    # Last X stats: need GAME_DATE
    if "GAME_DATE" in df.columns and df["GAME_DATE"].notna().any():
        lastx_list = []
        for pid, g in df.groupby("PERSON_ID"):
            g_sorted = g.sort_values("GAME_DATE")
            last_games = g_sorted.tail(last_x)
            lastx_list.append({
                "PERSON_ID": pid,
                "LASTX_N": len(last_games),
                "LASTX_AVG": last_games["TTFL_SCORE"].mean() if not last_games.empty else float('nan'),
                "LASTX_STD": last_games["TTFL_SCORE"].std(ddof=1) if len(last_games) > 0 else float('nan')
            })
        lastx_df = pd.DataFrame(lastx_list)
    else:
        # No dates available -> cannot compute last X reliably
        lastx_df = pd.DataFrame(columns=["PERSON_ID", "LASTX_N", "LASTX_AVG", "LASTX_VAR"]) 

    out = pd.merge(season_stats, lastx_df, on="PERSON_ID", how="left")
    # Keep only players with at least `min_games` games
    out = out[out["N_GAMES"] >= min_games]
    # Sort by lastx avg primarily, fallback to season avg
    out["SORT_KEY"] = out["LASTX_AVG"].fillna(out["SEASON_AVG"])
    out = out.sort_values("SORT_KEY", ascending=False)
    return out.head(top_n).drop(columns=["SORT_KEY"]) 


def print_top_players(df: pd.DataFrame, top_n: int = 10, last_x: int = 5, min_games: int = 10):
    tbl = top_players_by_ttfl(df, top_n=top_n, last_x=last_x, min_games=min_games)
    if tbl.empty:
        print("Aucun joueur avec des matches disponibles.")
        return
    display_df = tbl[["DISPLAY_FIRST_LAST", "TEAM_ABBREVIATION", "N_GAMES", "SEASON_AVG", "SEASON_STD", "LASTX_N", "LASTX_AVG", "LASTX_STD"]].copy()
    # Rename TEAM_ABBREVIATION -> TEAM for display
    display_df.rename(columns={"TEAM_ABBREVIATION": "TEAM"}, inplace=True)
    # Round numeric columns to 2 decimals for readability
    for col in ["SEASON_AVG", "SEASON_STD", "LASTX_AVG", "LASTX_STD"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].round(2)
    print(display_df.to_string(index=False))


if __name__ == "__main__":
    # Usage example: récupère (ou charge cache), puis affiche top 10
    df = fetch_players_ttfl()
    print_top_players(df, top_n=10, last_x=5)
