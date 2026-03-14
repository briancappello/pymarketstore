"""Tests for pymarketstore.enums — EnumMeta, OrderedEnumMeta, OrderedEnum, and Freq."""

import pytest

from pymarketstore.enums import Enum, EnumMeta, Freq, OrderedEnum, OrderedEnumMeta


# ---------------------------------------------------------------------------
# EnumMeta — lookup by name and value, membership
# ---------------------------------------------------------------------------


class TestEnumMeta:
    def test_getitem_by_name(self):
        assert Freq["second"] is Freq.second
        assert Freq["day"] is Freq.day

    def test_getitem_by_value(self):
        assert Freq["1Sec"] is Freq.second
        assert Freq["1Min"] is Freq.min_1
        assert Freq["1D"] is Freq.day

    def test_getitem_invalid_raises(self):
        with pytest.raises((KeyError, ValueError)):
            Freq["nonexistent"]

    def test_contains_by_name(self):
        assert "second" in Freq
        assert "day" in Freq
        assert "min_1" in Freq

    def test_contains_by_value(self):
        assert "1Sec" in Freq
        assert "1Min" in Freq
        assert "1D" in Freq

    def test_contains_negative(self):
        assert "10Y" not in Freq
        assert "bogus" not in Freq

    def test_contains_member(self):
        assert Freq.second in Freq
        assert Freq.day in Freq


# ---------------------------------------------------------------------------
# OrderedEnumMeta — integer indexing and slicing
# ---------------------------------------------------------------------------


class TestOrderedEnumMeta:
    def test_getitem_positive_index(self):
        assert Freq[0] is Freq.second
        assert Freq[1] is Freq.min_1
        assert Freq[8] is Freq.day

    def test_getitem_negative_index(self):
        assert Freq[-1] is Freq.year
        assert Freq[-2] is Freq.quarter

    def test_getitem_out_of_range_raises(self):
        with pytest.raises(KeyError):
            Freq[100]

    def test_slice_integers(self):
        result = Freq[0:3]
        assert result == [Freq.second, Freq.min_1, Freq.min_2]

    def test_slice_members(self):
        result = Freq[Freq.min_5 : Freq.hour]
        assert result == [Freq.min_5, Freq.min_10, Freq.min_15, Freq.min_30]

    def test_slice_with_step(self):
        result = Freq[0:6:2]
        assert result == [Freq.second, Freq.min_2, Freq.min_10]

    def test_slice_open_start(self):
        result = Freq[:3]
        assert result == [Freq.second, Freq.min_1, Freq.min_2]

    def test_slice_open_end(self):
        result = Freq[Freq.month :]
        assert result == [Freq.month, Freq.quarter, Freq.year]

    def test_slice_negative_stop(self):
        result = Freq[-3:]
        assert result == [Freq.month, Freq.quarter, Freq.year]


# ---------------------------------------------------------------------------
# OrderedEnum — arithmetic and comparison
# ---------------------------------------------------------------------------


class TestOrderedEnum:
    def test_add(self):
        assert Freq.second + 1 is Freq.min_1
        assert Freq.min_1 + 3 is Freq.min_10

    def test_add_non_int_raises(self):
        with pytest.raises(TypeError, match="int required"):
            Freq.second + 1.5

    def test_add_overflow_raises(self):
        with pytest.raises(NotImplementedError, match="out-of-bounds"):
            Freq.year + 1

    def test_sub(self):
        assert Freq.min_1 - 1 is Freq.second
        assert Freq.day - 1 is Freq.hour

    def test_sub_non_int_raises(self):
        with pytest.raises(TypeError, match="int required"):
            Freq.day - "1"

    def test_sub_underflow_raises(self):
        with pytest.raises(NotImplementedError, match="out-of-bounds"):
            Freq.second - 1

    def test_lt(self):
        assert Freq.second < Freq.min_1
        assert Freq.day < Freq.year
        assert not Freq.year < Freq.second

    def test_le(self):
        assert Freq.second <= Freq.second
        assert Freq.second <= Freq.min_1
        assert not Freq.year <= Freq.day

    def test_symmetry(self):
        """a + n - n should return the original member."""
        assert Freq.min_5 + 3 - 3 is Freq.min_5


# ---------------------------------------------------------------------------
# Freq — value correctness
# ---------------------------------------------------------------------------


class TestFreq:
    def test_all_members_exist(self):
        expected = [
            ("second", "1Sec"),
            ("min_1", "1Min"),
            ("min_2", "2Min"),
            ("min_5", "5Min"),
            ("min_10", "10Min"),
            ("min_15", "15Min"),
            ("min_30", "30Min"),
            ("hour", "1H"),
            ("day", "1D"),
            ("week", "1W"),
            ("month", "1M"),
            ("quarter", "3M"),
            ("year", "1Y"),
        ]
        for name, value in expected:
            member = Freq[name]
            assert member.value == value, f"Freq.{name} should have value {value!r}"

    def test_total_count(self):
        assert len(Freq) == 13

    def test_ordering_is_ascending(self):
        members = list(Freq)
        for i in range(len(members) - 1):
            assert members[i] < members[i + 1], (
                f"Freq.{members[i].name} should be < Freq.{members[i + 1].name}"
            )
