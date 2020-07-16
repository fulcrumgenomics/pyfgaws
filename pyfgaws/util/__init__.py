from random import uniform
from typing import Union

# The minimum delay ins seconds between batch job API requests
MINIMUM_DELAY: int = 1


def add_jitter(
    delay: Union[int, float] = MINIMUM_DELAY,
    width: Union[int, float] = 1,
    minima: Union[int, float] = 0,
) -> float:
    """Apply a jitter to the delay, to help avoid AWS batch API limits for monitoring batch
    jobs in the cases of many requests across concurrent jobs.

    Args:
        delay: the number of seconds to wait upon making a subsequent request
        width: the width for the random jitter, centered around delay
        minima: the minimum delay allowed

    Returns:
        the new delay with the jitter applied (`uniform(delay - width, delay + width)`)
    """
    assert delay > 0, f"Delay must be > 0: {delay}"
    assert width > 0, f"Width must be > 0: {width}"
    assert minima >= 0, f"Minima must be >= 0: {minima}"
    delay = abs(delay)
    width = abs(width)
    minima = abs(minima)
    lower = max(minima, delay - width)
    upper = max(minima, delay) + width
    assert upper >= lower
    return uniform(lower, upper)
