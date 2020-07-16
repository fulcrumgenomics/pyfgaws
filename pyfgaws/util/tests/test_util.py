"""Tests for :module:`~pyfgaws.util`"""

from pyfgaws.util import add_jitter
import pytest


def test_add_jitter() -> None:
    for delay in [1, 5, 10, 5.5]:
        for width in [1, 5, 10, 2.5]:
            for minima in [0, 1, 5, 10, 0.5]:
                for _ in range(15):
                    jittered = add_jitter(delay=delay, width=width, minima=minima)
                    label: str = f"add_jitter(delay={delay}, width={width}, minima={minima})"
                    assert jittered >= minima, label
                    assert jittered >= delay - width, label
                    assert jittered <= max(minima, delay) + width, label


def test_add_jitter_bad_args() -> None:
    with pytest.raises(AssertionError):
        add_jitter(delay=0)
    with pytest.raises(AssertionError):
        add_jitter(delay=-10)
    with pytest.raises(AssertionError):
        add_jitter(width=0)
    with pytest.raises(AssertionError):
        add_jitter(width=-10)
    with pytest.raises(AssertionError):
        add_jitter(minima=-0.001)
    with pytest.raises(AssertionError):
        add_jitter(minima=-10)
