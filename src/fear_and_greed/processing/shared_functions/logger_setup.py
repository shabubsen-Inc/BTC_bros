import logging
import json


def setup_logger() -> logging.Logger:
    """
    Sets up logging in JSON format to include severity levels.
    This is useful for structured logging in cloud environments.

    Returns:
        logging.Logger: Configured logger object.
    """
    logger = logging.getLogger("cloud_logger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            json.dumps(
                {
                    "severity": "%(levelname)s",
                    "message": "%(message)s",
                    "timestamp": "%(asctime)s",
                }
            )
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# Initialize logger
logger = setup_logger()
