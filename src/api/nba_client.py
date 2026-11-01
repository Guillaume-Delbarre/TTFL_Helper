import pandas as pd

from src.utils.logger import logger
from src.utils.config import NBA_SEASON
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import leaguegamelog, boxscoretraditionalv3

class NBAClient:
    """Client pour récupérer les données NBA"""

    def __init__(self, season: str = NBA_SEASON):
        """Initialise le client NBA avec une saison spécifique.

        Args:
            season: Saison NBA (ex: "2023-24").
        """
        self.season = season
        logger.info(f"Initialisation du client NBA pour la saison {self.season}")

    def get_all_players(self) -> List[Dict]:
        """Récupère tous les joueurs actifs de la saison.

        Returns:
            List[Dict]: Liste des joueurs avec leurs informations (id, nom, équipe, etc.).
        """
        logger.info("Récupération de la liste des joueurs")
        try :
            return players.get_active_players()
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des joueurs: {e}")
            return []

    def get_all_games(self) -> pd.DataFrame:
        """Récupère tous les matchs de la saison.

        Returns:
            pd.DataFrame: DataFrame avec les informations sur les matchs.
        """
        logger.info("Récupération de tous les matchs de la saison")
        try :
            game_logs = leaguegamelog.LeagueGameLog(
                season=self.season,
                season_type_all_star="Regular Season"
            ).get_data_frames()[0]
            return game_logs
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des joueurs: {e}")
            return pd.DataFrame()

    def get_game_stats(self, game_id: str) -> Optional[pd.DataFrame]:
        """Récupère les statistiques des joueurs pour un match spécifique.

        Args:
            game_id: ID du match (ex: "0022600153").

        Returns:
            Optional[pd.DataFrame]: DataFrame avec les stats des joueurs pour le match.
        """
        logger.info(f"Récupération des stats pour le match {game_id}")
        try :
            box_score = boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id
            ).get_data_frames()[0]
            return box_score
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des stats pour le match {game_id}: {e}")
            return None