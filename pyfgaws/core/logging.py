"""
Methods for setting up logging for tools.
-----------------------------------------
"""

import logging
import socket
from threading import RLock


# Global that is set to True once logging initialization is run to prevent running > once.
__PYFGAWS_LOGGING_SETUP: bool = False

# A lock used to make sure initialization is performed only once
__LOCK = RLock()


def setup_logging(level: str = "INFO") -> None:
    """Globally configure logging for all modules under pyfgaws.

    Configures logging to run at a specific level and output messages to stderr with
    useful information preceding the actual log message.
    """
    global __PYFGAWS_LOGGING_SETUP

    with __LOCK:
        if not __PYFGAWS_LOGGING_SETUP:
            format = (
                f"%(asctime)s {socket.gethostname()} %(name)s:%(funcName)s:%(lineno)s "
                + "[%(levelname)s]: %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setLevel(level)
            handler.setFormatter(logging.Formatter(format))

            logger = logging.getLogger("pyfgaws")
            logger.setLevel(level)
            logger.addHandler(handler)
        else:
            logging.getLogger(__name__).warn("Logging already initialized.")

        __PYFGAWS_LOGGING_SETUP = True
