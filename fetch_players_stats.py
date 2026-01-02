from nba_api.stats.endpoints import CommonAllPlayers, PlayerGameLog
from cache_players_utils import is_cached, load_df_cache, save_df_cache, CACHE_DIR
import os
from datetime import date
from typing import Callable, Optional
from tqdm import tqdm
import pandas as pd
import time

SEASON = "2025-26"


def compute_ttfl_score(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute la colonne `TTFL_SCORE` au DataFrame selon la formule :

    POINTS + REBONDS + PASSES + INTERCEPTIONS + CONTRES + TIRS RÉUSSIS
    + 3PTS RÉUSSIS + LF RÉUSSIS - (BALLES PERDUES + TIRS RATÉS + 3PTS RATÉS + LF RATÉS)

    La fonction s'assure que les colonnes nécessaires existent et remplace
    les valeurs manquantes par 0.

    Args:
        df (pandas.DataFrame): DataFrame contenant les colonnes de match.

    Returns:
        pandas.DataFrame: Même DataFrame avec la colonne `TTFL_SCORE` ajoutée.
    """
    cols = ["PTS", "REB", "AST", "STL", "BLK", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "TOV"]
    for c in cols:
        if c not in df.columns:
            df[c] = 0
    # Convert to numeric and fillna
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    shots_missed = df["FGA"] - df["FGM"]
    threes_missed = df["FG3A"] - df["FG3M"]
    ft_missed = df["FTA"] - df["FTM"]

    df["TTFL_SCORE"] = (
        df["PTS"] + df["REB"] + df["AST"] + df["STL"] + df["BLK"]
        + df["FGM"] + df["FG3M"] + df["FTM"]
        - (df["TOV"] + shots_missed + threes_missed + ft_missed)
    )

    return df

def get_players_from_season() :
    """Récupère la liste des joueurs actifs pour la saison courante.

    Utilise un fichier de cache local si présent afin d'éviter d'appeler
    l'API à chaque exécution. Si aucun cache n'est présent, interroge
    l'API `CommonAllPlayers` puis sauvegarde le résultat dans le cache.

    Returns:
        pandas.DataFrame: DataFrame contenant les joueurs actifs avec au
                          minimum les colonnes `PERSON_ID`,
                          `DISPLAY_FIRST_LAST`, `PLAYERCODE`, `TEAM_ID`,
                          `TEAM_ABBREVIATION`.
    """
    json_name = "players/players.csv"
    cache_path = os.path.join(CACHE_DIR, json_name)
    if is_cached(cache_path):
        print(f"📦 Cache utilisé pour récupérer les joueurs de la saison")
        return load_df_cache(cache_path)
    
    print(f"🌐 Appel NBA.com pour les joueurs de la saison")
    players_df = CommonAllPlayers(is_only_current_season=1).get_data_frames()[0]
    players_df = players_df[players_df["GAMES_PLAYED_FLAG"] == "Y"][["PERSON_ID", "DISPLAY_FIRST_LAST", "PLAYERCODE", "TEAM_ID", "TEAM_ABBREVIATION"]]
    save_df_cache(players_df, cache_path)
    return players_df

def get_players_stats(season: str = SEASON,
                      cache_date: Optional[str] = None,
                      force_refresh: bool = False,
                      transform_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None):
    """Récupère les game logs pour les joueurs et met en cache le résultat.

    Cette fonction collecte les game logs pour chaque joueur actif de la
    saison, concatène les logs, effectue une jointure avec la table des
    joueurs et sauvegarde le DataFrame fusionné dans un fichier de cache
    daté.

    Args:
        season (str): Saison à interroger (format 'YYYY-YY').
        cache_date (Optional[str]): Date du cache au format 'YYYY-MM-DD'.
                                    Si None, la date du jour est utilisée.
        force_refresh (bool): Forcer la récupération depuis l'API même si
                              un cache existe.
        transform_fn (Optional[Callable[[pd.DataFrame], pd.DataFrame]]):
            Fonction optionnelle appliquée au DataFrame fusionné avant
            l'écriture du cache (ex: ajout de colonnes calculées).

    Returns:
        pandas.DataFrame: DataFrame fusionné `players + games` si des logs
                          existent, sinon le DataFrame des joueurs seuls.
    """
    if cache_date is None:
        cache_date = date.today().isoformat()

    cache_filename = f"players/players_games_{season.replace('-', '')}_{cache_date}.csv"
    cache_path = os.path.join(CACHE_DIR, cache_filename)

    if is_cached(cache_path) and not force_refresh:
        print(f"📦 Cache utilisé pour récupérer players+games ({cache_date})")
        return load_df_cache(cache_path)

    players_df = get_players_from_season()
    players_ids = players_df["PERSON_ID"].unique()
    all_games = []

    for pid in tqdm(players_ids):
        try:
            games_log = PlayerGameLog(player_id=pid, season=season).get_data_frames()[0]
            # print(f"Game log pour {pid} : {games_log.shape[0]} lignes")
            if not games_log.empty:
                all_games.append(games_log)
        except Exception as e:
            print(f"[ERROR] Erreur dans la récupération des logs de l'id {pid} : {e}")
        time.sleep(0.6)  # throttle to avoid rate limits

    if all_games:
        games_df = pd.concat(all_games, ignore_index=True)

        # Colonnes à conserver : identifiants joueurs + stats demandées
        keep_player_cols = ["PERSON_ID", "DISPLAY_FIRST_LAST", "PLAYERCODE", "TEAM_ID", "TEAM_ABBREVIATION"]
        # Colonnes issues de game logs utiles pour l'analyse (laisser de quoi calculer les dérivées)
        candidate_game_cols = [
            "Player_ID", "GAME_DATE", "MATCHUP", "PTS", "REB", "AST", "STL", "BLK",
            "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "TOV"
        ]

        # Keep only existing columns (API sometimes varie)
        game_cols = [c for c in candidate_game_cols if c in games_df.columns]
        games_df = games_df[game_cols]

        merged_df = pd.merge(players_df, games_df, left_on="PERSON_ID", right_on="Player_ID", how="left")

        # Allow user to add computed columns before caching
        if transform_fn is not None:
            merged_df = transform_fn(merged_df)

        # Compute TTFL score and add to DataFrame
        merged_df = compute_ttfl_score(merged_df)

        # Ensure cache dir exists then save
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        save_df_cache(merged_df, cache_path)
        print(f"📦 Cache sauvegardé: {cache_path}")

        return merged_df

    return players_df
    

if __name__ == "__main__":
    df = get_players_stats()
    print(df.head())