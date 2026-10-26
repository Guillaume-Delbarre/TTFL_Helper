import os
from pathlib import Path

# ======================
# Chemins des fichiers
# ======================
BASE_DIR = Path(__file__).parent.parent.parent  # Racine du projet (ttfl_helper/)
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
DB_DIR = DATA_DIR / "db"
LOGS_DIR = BASE_DIR / "logs"

# Fichiers JSON pour le cache
PLAYERS_JSON_PATH = CACHE_DIR / "players.json"
GAMES_JSON_PATH = CACHE_DIR / "games.json"

# Base de données SQLite
DB_PATH = DB_DIR / "ttfl.db"

# ======================
# Paramètres par défaut
# ======================
# Nombre de matchs pour calculer la moyenne glissante
MOVING_AVERAGE_GAMES = 5

# Poids pour le calcul des recommandations
WEIGHT_RECENT_PERFORMANCE = 0.3  # Poids de la moyenne glissante
WEIGHT_SEASON_AVERAGE = 0.7      # Poids de la moyenne saisonnière

# Formule de calcul des points TTFL
TTFL_POINTS_FORMULA = (
    "PTS + REB + AST + STL + BLK + FG_Made + FG3_Made + FT_Made "
    "- (TO + FG_Missed + FG3_Missed + FT_Missed)"
)

# ======================
# Paramètres de logging
# ======================
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = LOGS_DIR / "ttfl_helper.log"

# ======================
# Clés API (si nécessaire)
# ======================
NBA_API_KEY = None  # À définir si nécessaire (ex: pour des données premium)