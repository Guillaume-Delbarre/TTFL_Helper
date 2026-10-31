import logging
from src.utils.config import LOG_LEVEL, LOG_FORMAT, LOG_FILE
from pathlib import Path
from typing import Optional

def setup_logger(
    name: str = "ttfl_helper",
    when: str = "midnight",  # Rotation à minuit
    interval: int = 1,       # Tous les jours
    backup_count: int = 7,   # Conserver 7 jours de logs
) -> logging.Logger:
    """Création et configuration de l'instance logger"""
    # Création du répertoire de log si il n'existe pas
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

    # Création du logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))

    # Création du timed rotating file handler
    timed_handler = TimedRotatingFileHandler(
        LOG_FILE,
        when=when,
        interval=interval,
        backupCount=backup_count,
        encoding="utf-8",
    )
    timed_handler.setLevel(getattr(logging, LOG_LEVEL))

    # Création du formatter à partir des configs
    formatter = logging.Formatter(LOG_FORMAT)
    timed_handler.setFormatter(formatter)

    # Ajout du handler
    logger.timed_handler(rotating_handler)

    return logger

logger = setup_logger()