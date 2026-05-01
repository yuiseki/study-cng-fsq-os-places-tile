from __future__ import annotations

import pytest

from fsq_os_places_cng.filters import parse_top_labels


def test_parse_single_top_level():
    assert parse_top_labels("Dining and Drinking") == ["Dining and Drinking"]


def test_parse_multiple():
    assert parse_top_labels("Dining and Drinking, Retail, Health and Medicine") == [
        "Dining and Drinking",
        "Retail",
        "Health and Medicine",
    ]


def test_parse_strips_whitespace_and_drops_empty():
    assert parse_top_labels(" A ,, B ") == ["A", "B"]


def test_parse_empty_raises():
    with pytest.raises(ValueError):
        parse_top_labels("")


def test_parse_only_separators_raises():
    with pytest.raises(ValueError):
        parse_top_labels(" , , ")
