"""Tests for :module:`~pyfgws.tools.__main__`

Motivation
~~~~~~~~~~

The idea is to run help on the main tools method (`-h`) as well as on
each tool (ex. `tool-name -h`).  This should force :module:`~defopt`
to parse the docstring for the method.  Since :module:`~defopt` uses
:module:`~argparse` underneath, `SystemExit`s are raised, which are
different than regular `Exceptions`.  The exit code returned by help
(the usage) is 0.  A improperly formatted docstring throws a
:class:`NotImplementedException`.  We add a few test below to show
this.
"""

import defopt
import pytest

from pyfgaws.__main__ import TOOLS
from pyfgaws.__main__ import main
from pyfgaws.tests import test_tool_funcs as _test_tool_funcs


def invalid_docs_func(*, a: str) -> None:
    """This is a dummy method with incorrectly formatted docs

        * this is bad

    Args:
        a: some string
    """
    print(a)


def valid_docs_func(*, a: int) -> None:
    """Some func

    * this is good

    Args:
        a: some int
    """
    print(a)


def test_incorrect_docs() -> None:
    """A little test to demonstrate that when the error when a tools docs are
    incorrectly formatted, a TypeError is thrown"""
    with pytest.raises(TypeError):
        defopt.run(invalid_docs_func, argv=["-a", "str"])


def test_incorrect_param_type() -> None:
    """A little test to demonstrate that when the input arg has the wrong type,
    a SystemExit is thrown with exit code 2"""
    with pytest.raises(SystemExit) as e:
        defopt.run(valid_docs_func, argv=["-a", "str"])
    assert e.type == SystemExit
    assert e.value.code == 2  # code should be 2 for parse error


def test_tools_help() -> None:
    """ Tests that running dts.tools with -h exits OK"""
    argv = ["-h"]
    with pytest.raises(SystemExit) as e:
        main(argv=argv)
    assert e.type == SystemExit
    assert e.value.code == 0  # code should be 0 for help


@pytest.mark.parametrize("tool", TOOLS)
def test_tool_funcs(tool) -> None:  # type: ignore
    for subcommand, tools in TOOLS.items():
        for tool in tools:
            _test_tool_funcs(subcommand, tool, main)
