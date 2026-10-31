import logging
from pathlib import Path
import pytest
from src.utils.config import LOG_FILE
from src.utils.logger import setup_logger, logger

@pytest.fixture
def cleanup_logs():
    """Fixture pour nettoyer les fichiers de log de test."""
    # Nettoyer les fichiers de log avant le test
    for log_file in Path(LOG_FILE).parent.glob("ttfl_helper*.log"):
        log_file.unlink()
    yield
    # Nettoyer les fichiers de log après le test
    for log_file in Path(LOG_FILE).parent.glob("ttfl_helper*.log"):
        log_file.unlink()

def test_setup_logger_creates_log_file(cleanup_logs):
    """Test que setup_logger crée un fichier de log."""
    test_logger = setup_logger()
    test_logger.info("Test message")

    # Vérification que le fichier existe bien
    assert Path(LOG_FILE).exists()

    # Vérification du contenu du fichier
    with open(LOG_FILE, "r") as f:
        content = f.read()
        assert "Test message" in content


def test_setup_logger_default_name(cleanup_logs):
    """Test que setup_logger utilise le nom par défaut."""
    test_logger = setup_logger()
    assert test_logger.name == "ttfl_helper"

def test_setup_logger_custom_name(cleanup_logs):
    """Test que setup_logger utilise un nom personnalisé."""
    test_logger = setup_logger(name="custom_logger")
    assert test_logger.name == "custom_logger"

def test_setup_logger_returns_logger_instance(cleanup_logs):
    """Test que setup_logger retourne une instance de logging.Logger."""
    test_logger = setup_logger()
    assert isinstance(test_logger, logging.Logger)

def test_global_logger_instance(cleanup_logs):
    """Test que l'instance globale logger est correctement configurée."""
    assert logger.name == "ttfl_helper"
    assert isinstance(logger, logging.Logger)