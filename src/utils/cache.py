import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Union

def save_to_cache(
    data: Union[Dict, List, pd.DataFrame],
    file_path: str,
    orient: str = "records"
) -> None:
    """Sauvegarde les données dans un fichier JSON.
    
    Args:
        data: Données à sauvegarder (Dict, List, ou DataFrame).
        file_path: Chemin du fichier de cache.
        orient: Format pour les DataFrames (ex: "records", "split").
    """
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    if isinstance(data, pd.DataFrame):
        data = data.to_dict(orient=orient)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_from_cache(
    file_path: str,
    as_dataframe: bool = False,
    orient: str = "records"
) -> Optional[Union[Dict, List, pd.DataFrame]]:
    """Charge les données depuis un fichier JSON.

    Args:
        file_path: Chemin du fichier de cache.
        as_dataframe: Si True, convertit les données en DataFrame.
        orient: Format pour les DataFrames (ex: "records", "split").

    Returns:
        Données chargées (Dict, List, ou DataFrame) ou None si le fichier n'existe pas.
    """
    if not Path(file_path).exists():
        return None

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if as_dataframe:
        return pd.DataFrame(data)
    return data