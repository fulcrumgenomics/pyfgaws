"""Main entry point for all pyfgaws tools."""

import logging
import sys
from typing import Any
from typing import Callable
from typing import Dict
from typing import List

import defopt
import json

from pyfgaws.batch.tools import run_job
from pyfgaws.batch.tools import watch_job
from pyfgaws.batch import Status
from pyfgaws.logs.tools import watch_logs
from mypy_boto3_batch.type_defs import KeyValuePairTypeDef as BatchKeyValuePairTypeDef  # noqa


# The list of tools to expose on the command line
TOOLS: List[Callable] = sorted([run_job, watch_job, watch_logs], key=lambda f: f.__name__)


def _parse_key_value_pair_type(string: str) -> BatchKeyValuePairTypeDef:
    """Parses a simple dictionary that must contain two key-value pairs."""
    pair = json.loads(string)
    assert isinstance(pair, dict), "argument is not a list"
    assert "name" in pair, "name not found in key-value pair"
    assert "value" in pair, "name not found in key-value pair"
    assert len(pair.keys()) == 2, "Found extra keys in key-value pair: " + ", ".join(pair.keys())
    return pair  # type: ignore



def _parsers() -> Dict[type, Callable[[str], Any]]:
    """Returns the custom parsers for defopt"""
    return {
        Dict[str, Any]: lambda string: json.loads(string),
        BatchKeyValuePairTypeDef: _parse_key_value_pair_type,
        Status: lambda string: Status.from_string(string)
    }


def main(argv: List[str] = sys.argv[1:]) -> None:
    logger = logging.getLogger(__name__)
    if len(argv) != 0 and all(arg not in argv for arg in ["-h", "--help"]):
        logger.info("Running command: fgaws-tools " + " ".join(argv))
    try:
        defopt.run(funcs=TOOLS, argv=argv, parsers=_parsers())
        logger.info("Completed successfully.")
    except Exception as e:
        logger.info("Failed on command: " + " ".join(argv))
        raise e
