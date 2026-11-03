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

    def get_all_players_from_api(self) -> List[Dict]:
        """Récupère tous les joueurs actifs de la saison via l'api.

        Returns:
            List[Dict]: Liste des joueurs avec leurs informations (id, nom, équipe, etc.).
        """
        logger.info("Récupération de la liste des joueurs via l'api NBA")
        try :
            return players.get_active_players()
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des joueurs: {e}")
            return []

    def get_all_players(self) -> list[Dict]:
        """Récupère tous les joueurs actifs de la saison (avec cache).

        Returns:
            List[Dict]: Liste des joueurs avec leurs informations (id, nom, équipe, etc.).
        """
        logger.info("Récupération de la liste des joueurs")

        # Vérification si les joueurs sont déjà en cache
        cached_players = load_from_cache(PLAYERS_JSON_PATH)
        if cached_players is not None:
            logger.info("Joueurs chargés depuis le cache.")
            return cached_players

        # Sinon, récupère depuis l'API
        else :
            players_dict = self.get_all_players_from_api()
            save_to_cache(players_dict, PLAYERS_JSON_PATH)
            return players_dict


    def get_all_games_api(self) -> pd.DataFrame:
        """Récupère tous les matchs de la saison via l'api.

        Returns:
            pd.DataFrame: DataFrame avec les informations sur les matchs.
        """
        logger.info("Récupération de tous les matchs de la saison via l'api")
        try :
            game_logs = leaguegamelog.LeagueGameLog(
                season=self.season,
                season_type_all_star="Regular Season"
            ).get_data_frames()[0]
            return game_logs
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des joueurs: {e}")
            return pd.DataFrame()
    
    def get_all_games(self) -> pd.DataFrame:
        """Récupère tous les matchs de la saison (avec cache).

        Returns:
            pd.DataFrame: DataFrame avec les informations sur les matchs.
        """
        games_file = GAMES_JSON_PATH.format(season=self.season.replace("-", "_"))
        logger.info("Récupération de tous les matchs de la saison")

        # Vérification si les matchs sont déjà en cache
        cached_games = load_from_cache(games_file, as_dataframe=True)
        if cached_games is not None:
            logger.info("Matchs chargés depuis le cache.")
            return cached_games

        # Sinon, récupère depuis l'API
        else :
            games = self.get_all_games_from_api()
            save_to_cache(games, games_file)
            return players_dict



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