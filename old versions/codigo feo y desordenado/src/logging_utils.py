from __future__ import annotations

import logging


def configure_logging(level: int) -> logging.Logger:
    """
    Configura logging a consola.

    :param level: Nivel logging.
    :type level: int
    :return: Logger.
    :rtype: logging.Logger
    """
    logger = logging.getLogger("firestore_to_sheets")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False
    return logger
