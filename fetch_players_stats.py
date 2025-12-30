from nba_api.stats.endpoints import CommonAllPlayers, PlayerGameLog
from cache_players_utils import is_cached, load_df_cache, save_df_cache
from tqdm import tqdm
import pandas as pd
from time import time

SEASON = "2025-26"

def get_players_from_season() :
    if is_cached() :
        print(f"📦 Cache utilisé pour récupérer les joueurs de la saison")
        return load_df_cache()
    
    print(f"🌐 Appel NBA.com pour les joueurs de la saison")
    players_df = CommonAllPlayers(is_only_current_season=1).get_data_frames()[0]
    players_df = players_df[players_df["GAMES_PLAYED_FLAG"] == "Y"][["PERSON_ID", "DISPLAY_FIRST_LAST", "PLAYERCODE", "TEAM_ID", "TEAM_ABBREVIATION"]]
    save_df_cache(players_df)
    return players_df

def get_players_stats(season:str = SEASON) :
    players_df = get_players_from_season()
    players_ids = players_df["PERSON_ID"].unique()

    for pid in tqdm(players_ids) :
        try :
            games_log = PlayerGameLog(player_id=pid, season=season).get_data_frames()[0]
            if not games_log.empty :
                players_df = pd.merge(players_df, games_log, left_on="PERSON_ID", right_on="Player_ID", how="left")
        except Exception as e :
            print(f"[ERROR] Exception : {e}")
        time.sleep(0.6)  # throttle to avoid rate limits

print(get_players_stats)