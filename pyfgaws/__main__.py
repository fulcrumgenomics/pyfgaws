"""Main entry point for all pyfgaws tools."""

import logging
import sys
from typing import Callable
from typing import List

import defopt

try:
    from pyfgaws.batch.tools import BATCH_TOOLS
except ImportError:
    BATCH_TOOLS = []

try:
    from pyfgaws.logs.tools import LOGS_TOOLS
except ImportError:
    LOGS_TOOLS = []

TOOLS: List[Callable] = sorted(BATCH_TOOLS + LOGS_TOOLS, key=lambda f: f.__name__)


def main(argv: List[str] = sys.argv[1:]) -> None:
    logger = logging.getLogger(__name__)
    if len(argv) != 0 and all(arg not in argv for arg in ["-h", "--help"]):
        logger.info("Running command: fgaws-tools" + " ".join(argv))
    try:
        defopt.run(funcs=TOOLS, argv=argv)
        logger.info("Completed successfully.")
    except Exception as e:
        logger.info("Failed on command: " + " ".join(argv))
        raise e
