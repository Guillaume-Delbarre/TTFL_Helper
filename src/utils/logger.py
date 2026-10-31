import logging
from src.utils.config import LOG_LEVEL, LOG_FORMAT, LOG_FILE
from pathlib import Path
from typing import Optional

def setup_logger(
    name: str = "ttfl_helper"
) -> logging.Logger:
    """Création et configuration de l'instance logger"""
    # Création du répertoire de log si il n'existe pas
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))

    # Create file handler
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(getattr(logging, LOG_LEVEL))

    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT)
    file_handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(file_handler)

    return logger

logger = setup_logger()

__all__ = ["logger"]