import os
import glob
from datetime import date
import pandas as pd
from bs4 import BeautifulSoup

from cache_players_utils import is_cached, load_df_cache, save_df_cache
import ttfl_getter


TTFL_CACHE_DIR = os.path.join("cache", "ttfl")


def _cache_path_for_date(d: str) -> str:
    return os.path.join(TTFL_CACHE_DIR, f"ttfl_history_{d}.csv")


def _parse_mu_table_from_html(html: str) -> pd.DataFrame:
    """Parse le HTML fourni et retourne uniquement les colonnes Date et Joueur.

    Args:
        html (str): Contenu HTML à parser.

    Returns:
        pandas.DataFrame: DataFrame avec colonnes `Date` (datetime) et `Joueur` (str).
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="MuTabme")
    if table is None:
        raise RuntimeError("Table #MuTabme introuvable dans le HTML fourni")

    # headers (may contain duplicates)
    headers = [th.get_text(strip=True) for th in table.find_all("th")]

    rows = []
    for tr in table.find("tbody").find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if not cells:
            continue
        if len(cells) < len(headers):
            cells += [""] * (len(headers) - len(cells))
        rows.append(cells[: len(headers)])
    # Make header names unique by position to avoid duplicate-label selection issues
    seen = {}
    unique_headers = []
    for h in headers:
        key = str(h).strip()
        cnt = seen.get(key, 0)
        if cnt == 0:
            unique = key
        else:
            unique = f"{key}__{cnt}"
        seen[key] = cnt + 1
        unique_headers.append(unique)

    df = pd.DataFrame(rows, columns=unique_headers)

    # Find first occurrence index for Date and Joueur in the original headers
    date_idx = next((i for i, h in enumerate(headers) if str(h).strip().lower() == "date"), None)
    joueur_idx = next((i for i, h in enumerate(headers) if str(h).strip().lower() == "joueur"), None)

    if date_idx is None and joueur_idx is None:
        return pd.DataFrame(columns=["Date", "Joueur"])

    selected = {}
    if date_idx is not None:
        selected[unique_headers[date_idx]] = "Date"
    if joueur_idx is not None:
        selected[unique_headers[joueur_idx]] = "Joueur"

    df2 = df[list(selected.keys())].rename(columns=selected)

    # Parse Date to datetime if present
    if "Date" in df2.columns:
        df2["Date"] = pd.to_datetime(df2["Date"], errors="coerce")

    return df2


def get_ttfl_history(cache_date: str | None = None, force_refresh: bool = False) -> pd.DataFrame:
    """Retourne le DataFrame de l'historique TTFL, en utilisant un cache daté.

    Si le cache du jour existe et `force_refresh` est False, il est chargé.
    Sinon on supprime les anciens caches, on récupère le HTML via `ttfl_getter.get_history()`
    (en mémoire), on parse et on sauvegarde le CSV daté dans `cache/ttfl/`.
    """
    if cache_date is None:
        cache_date = date.today().isoformat()

    os.makedirs(TTFL_CACHE_DIR, exist_ok=True)
    cache_path = _cache_path_for_date(cache_date)

    if is_cached(cache_path) and not force_refresh:
        return load_df_cache(cache_path)

    # remove old caches
    for f in glob.glob(os.path.join(TTFL_CACHE_DIR, "ttfl_history_*.csv")):
        try:
            os.remove(f)
        except Exception:
            pass

    # Get HTML in memory (no file written)
    html = ttfl_getter.get_history()

    df = _parse_mu_table_from_html(html)

    # Save cache (only Date and Joueur columns present)
    save_df_cache(df, cache_path)
    return df


if __name__ == "__main__":
    df = get_ttfl_history()
    print(df.head())
