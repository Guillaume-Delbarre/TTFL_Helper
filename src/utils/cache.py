import json
from pathlib import Path

def save_to_cache(
    data: Dict,
    file_path: str
) -> None:
    """Sauvegarde les données dans un fichier JSON."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(data, f)

def load_from_cache(file_path: str) -> Optional[Dict]:
    """Charge les données depuis un fichier JSON."""
    if Path(file_path).exists():
        with open(file_path, "r") as f:
            return json.load(f)
    return None