"""Test utilities for improving pytest DX."""

import pytest


def parametrize(*test_cases: dict):
    """Decorator that wraps pytest.mark.parametrize with a dict-based API.

    Instead of:
        @pytest.mark.parametrize("one, two", [(1, 2), ('one', 'two')])
        def test_something(one, two): ...

    Use:
        @parametrize(dict(one=1, two=2), dict(one='one', two='two'))
        def test_something(one, two): ...

    All dicts must have the same keys in the correct argument order.
    """
    if not test_cases:
        raise ValueError("At least one test case dict must be provided.")

    keys = list(test_cases[0].keys())

    # Validate all dicts share the same keys
    for i, case in enumerate(test_cases[1:], start=1):
        if list(case.keys()) != keys:
            raise ValueError(
                f"Test case {i} has keys {list(case.keys())!r}, expected {keys!r}."
            )

    return pytest.mark.parametrize(
        argnames=", ".join(keys),
        argvalues=[tuple(case[k] for k in keys) for case in test_cases],
    )
