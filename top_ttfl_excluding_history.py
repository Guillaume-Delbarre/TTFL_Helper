import argparse
import sys
import glob
import os
from datetime import datetime, timedelta
import pandas as pd
import top_ttfl
from nba_api.stats.endpoints import ScoreboardV2
from fetch_players_stats import SEASON as NBA_SEASON
import time

CACHE_PLAYERS_DIR = os.path.join("cache", "players")
CACHE_TTFL_DIR = os.path.join("cache", "ttfl")


def _load_recent_history(target_date: datetime.date, lookback_days: int = 30):
    files = glob.glob(os.path.join(CACHE_TTFL_DIR, "ttfl_history_*.csv"))
    if not files:
        return pd.DataFrame(columns=["Date", "Joueur"])
    dfs = []
    for f in files:
        try:
            d = pd.read_csv(f)
            if "Date" in d.columns:
                d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
            dfs.append(d)
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame(columns=["Date", "Joueur"])
    all_hist = pd.concat(dfs, ignore_index=True)
    if "Date" not in all_hist.columns:
        return pd.DataFrame(columns=["Date", "Joueur"])
    start = pd.Timestamp(target_date) - pd.Timedelta(days=lookback_days)
    end = pd.Timestamp(target_date)  # include target day
    recent = all_hist[(all_hist["Date"] >= start) & (all_hist["Date"] <= end)]
    return recent


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", "-d", help="Target date YYYY-MM-DD (can be used multiple times)", action="append", default=None)
    p.add_argument("--start", "-s", help="Start date YYYY-MM-DD to define a range (inclusive)")
    p.add_argument("--days", help="Number of days to include starting from start date (inclusive)", type=int, default=None)
    p.add_argument("--top", "-n", help="Number of top players to show", type=int, default=20)
    p.add_argument("--lookback", help="Lookback days for history exclusion", type=int, default=30)
    # If no arguments provided, show help
    if len(sys.argv) == 1:
        p.print_help()
        return

    args = p.parse_args()

    # Build list of target dates
    dates = []
    if args.start is not None:
        # use start + days range (inclusive)
        try:
            start_date = datetime.fromisoformat(args.start).date()
        except Exception:
            print(f"Date de départ invalide: {args.start}")
            return
        if args.days is None:
            print("Si --start est fourni, --days doit être précisé (nombre de jours inclus).")
            return
        if args.days <= 0:
            print("--days doit être un entier positif")
            return
        for i in range(args.days):
            dates.append(start_date + timedelta(days=i))
    elif args.date:
        for dstr in args.date:
            for part in dstr.split(','):
                part = part.strip()
                if not part:
                    continue
                try:
                    dates.append(datetime.fromisoformat(part).date())
                except Exception:
                    print(f"Date invalide: {part}")
                    return
    else:
        dates = [datetime.today().date()]

    df = top_ttfl.fetch_players_ttfl(cache_date=None, force_refresh=False)
    if "GAME_DATE" in df.columns:
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date

    name_col = None
    for candidate in ("DISPLAY_FIRST_LAST", "DISPLAY_NAME", "Player"):
        if candidate in df.columns:
            name_col = candidate
            break
    if name_col is None:
        print("Le DataFrame ne contient pas de colonne de nom de joueur attendue.")
        return

    df["_name_norm"] = df[name_col].astype(str).str.strip().str.lower()

    for target in dates:
        print(f"\n== Date: {target.isoformat()} ==")
        players_on_day = set()
        try:
            # retry helper for nba_api calls
            def _call_with_retries(fn, retries: int = 5, base_delay: float = 2.0):
                last_exc = None
                for attempt in range(retries):
                    try:
                        return fn()
                    except Exception as e:
                        last_exc = e
                        # exponential backoff
                        time.sleep(base_delay * (2 ** attempt))
                raise last_exc
            # increase timeout for nba_api calls to reduce ReadTimeouts
            sb = _call_with_retries(lambda: ScoreboardV2(game_date=target.isoformat(), timeout=60))
            dfs = sb.get_data_frames()
            # be polite: small pause after successful scoreboard call
            time.sleep(0.6)

            team_ids = set()
            team_abbrs = set()
            for d in dfs:
                cols = d.columns
                # heuristics for home/away team id columns
                possible_id_pairs = [
                    ("HOME_TEAM_ID", "VISITOR_TEAM_ID"),
                    ("TEAM_ID_HOME", "TEAM_ID_AWAY"),
                    ("TEAM_ID_HOME", "TEAM_ID_VISITOR"),
                    ("HOME_TEAM_ID", "AWAY_TEAM_ID"),
                ]
                for a, b in possible_id_pairs:
                    if a in cols and b in cols:
                        for _, row in d.iterrows():
                            try:
                                team_ids.add(int(row[a]))
                                team_ids.add(int(row[b]))
                            except Exception:
                                continue

                # try to get team abbreviations directly (preferred)
                possible_abbr_pairs = [
                    ("HOME_TEAM_ABBREVIATION", "VISITOR_TEAM_ABBREVIATION"),
                    ("TEAM_ABBREVIATION_HOME", "TEAM_ABBREVIATION_AWAY"),
                    ("HOME_TEAM_ABBREVIATION", "AWAY_TEAM_ABBREVIATION"),
                ]
                for a, b in possible_abbr_pairs:
                    if a in cols and b in cols:
                        team_abbrs.update(d[a].dropna().astype(str).str.strip().str.upper().unique())
                        team_abbrs.update(d[b].dropna().astype(str).str.strip().str.upper().unique())

                if not team_abbrs:
                    for col in d.columns:
                        if "TEAM_ABBREVIATION" in col.upper():
                            team_abbrs.update(d[col].dropna().astype(str).str.strip().str.upper().unique())

            # If we couldn't get abbreviations from the scoreboard, map team_ids -> abbreviations via the players cache
            if not team_abbrs and team_ids:
                team_abbrs.update(df.loc[df["TEAM_ID"].isin(team_ids), "TEAM_ABBREVIATION"].dropna().astype(str).str.strip().str.upper().unique())

            if team_abbrs:
                # select players from the merged history cache who belong to those teams
                players_on_day.update(df.loc[df["TEAM_ABBREVIATION"].astype(str).str.strip().str.upper().isin(team_abbrs), name_col].astype(str).str.strip().str.lower().unique())
            else:
                # no team info found => cannot determine players for the day
                raise RuntimeError(f"Unable to determine teams for scoreboard on {target}")
        except Exception as e:
            raise RuntimeError(f"NBA API failure while retrieving scoreboard for {target}: {e}")
        if not players_on_day:
            print(f"Aucun match trouvé pour la date {target}")
            continue

        # load recent history for exclusion (inclusive up to target)
        recent_hist = _load_recent_history(target, lookback_days=args.lookback)
        exclude_names = set()
        if "Joueur" in recent_hist.columns:
            exclude_names = set(recent_hist["Joueur"].dropna().astype(str).str.strip().str.lower())

        # candidates: all history rows for players_on_day
        candidates = df[df["_name_norm"].isin(players_on_day)].copy()
        # exclude by recent history
        candidates = candidates[~candidates["_name_norm"].isin(exclude_names)].copy()
        if candidates.empty:
            print("Après exclusion par historique récent, aucun joueur restant")
            continue

        # print formatted ranking using top_ttfl
        last_x = args.lookback if args.lookback is not None else 5
        top_ttfl.print_top_players(candidates, top_n=args.top, last_x=last_x)


if __name__ == "__main__":
    main()
