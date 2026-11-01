import pytest
from src.api.nba_client import NBAClient

@pytest.fixture
def client():
    return NBAClient(season="2026-27")

def test_get_all_players(client):
    """Test de la fonction de récupération de tous les joueurs"""
    players = client.get_all_players()
    assert isinstance(players, list)
    assert len(players) > 0

def test_get_all_games(client):
    """Test de la fonction de récupération des matchs"""
    games = client.get_all_games()
    assert not games.empty

def test_get_game_stats(client):
    """Test de la fonction de récupération des stats pour un match"""
    game_id = "0022600153" 
    stats = client.get_game_stats(game_id)
    assert stats is not None
    assert not stats.empty