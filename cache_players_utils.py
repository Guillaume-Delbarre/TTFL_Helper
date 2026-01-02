import os
import pandas as pd

CACHE_DIR = "cache/"


def ensure_cache_dir(path: str) -> None:
    """Crée le répertoire du cache si nécessaire.

    Args:
        path (str): Chemin vers un fichier ou un dossier. Si "path" contient une
                    extension de fichier, la fonction créera le répertoire parent.

    Returns:
        None: Crée les dossiers requis si ils n'existent pas.
    """
    dirpath = path
    # If a file path, get its directory
    if os.path.splitext(path)[1]:
        dirpath = os.path.dirname(path)
    if not dirpath:
        dirpath = CACHE_DIR
    os.makedirs(dirpath, exist_ok=True)


def is_cached(path: str = CACHE_DIR) -> bool:
    """Vérifie si un fichier de cache existe.

    Args:
        path (str): Chemin du fichier de cache à vérifier. Par défaut
                    `CACHE_DIR`.

    Returns:
        bool: True si le fichier existe, False sinon.
    """
    return os.path.exists(path)


def load_df_cache(path: str = CACHE_DIR) -> pd.DataFrame:
    """Charge un DataFrame depuis un fichier CSV de cache.

    Args:
        path (str): Chemin du fichier CSV à charger. Par défaut
                    `CACHE_DIR`.

    Returns:
        pandas.DataFrame: Le DataFrame chargé depuis le CSV.
    """
    return pd.read_csv(path)


def save_df_cache(df: pd.DataFrame, path: str = CACHE_DIR) -> None:
    """Enregistre un DataFrame dans un fichier CSV de cache.

    Crée le répertoire de destination si nécessaire.

    Args:
        df (pandas.DataFrame): DataFrame à sauvegarder.
        path (str): Chemin du fichier CSV de destination. Par défaut
                    `CACHE_DIR`.

    Returns:
        None
    """
    # Ensure cache directory exists before saving
    ensure_cache_dir(path)
    df.to_csv(path, index=False)
