from enum import Enum as BaseEnum
from enum import EnumMeta as BaseEnumMeta


class EnumMeta(BaseEnumMeta):
    def __getitem__(self, item):
        try:
            return self(item)
        except ValueError:
            return self._member_map_[item]

    def __contains__(self, item):
        if item in self._member_map_ or item in self._value2member_map_:
            return True
        return super().__contains__(item)


class OrderedEnumMeta(EnumMeta):
    def __new__(metacls, cls, bases, classdict, **kwds):
        if kwds.get("_simple"):
            return super().__new__(metacls, cls, bases, classdict, **kwds)

        klass = super().__new__(metacls, cls, bases, classdict, **kwds)
        klass._index2member_map_ = {
            i: member for i, member in enumerate(klass._member_map_.values())
        }
        klass._member2index_map_ = {
            member: i for i, member in enumerate(klass._member_map_.values())
        }
        return klass

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._index2member_map_[item if item >= 0 else (len(self) + item)]
        elif not isinstance(item, slice):
            return super().__getitem__(item)

        def idx(slice_n, default):
            if slice_n is None:
                return default
            elif isinstance(slice_n, int) and slice_n < 0:
                return len(self) + slice_n
            return self._member2index_map_.get(slice_n, slice_n)

        return [
            self._index2member_map_[i]
            for i in range(
                idx(item.start, 0),
                idx(item.stop, len(self)),
                item.step or 1,
            )
        ]


class Enum(BaseEnum, metaclass=EnumMeta):
    pass


class OrderedEnum(Enum, metaclass=OrderedEnumMeta):
    def __add__(self, other):
        if not isinstance(other, int):
            raise TypeError(
                f"Unsupported operand type for +: {type(other)!r} (int required)"
            )
        try:
            return self._index2member_map_[self._member2index_map_[self] + other]
        except KeyError:
            raise NotImplementedError(
                "Requested index is out-of-bounds. "
                "(Overflow / wrap-around is not supported.)"
            )

    def __sub__(self, other):
        if not isinstance(other, int):
            raise TypeError(
                f"Unsupported operand type for -: {type(other)!r} (int required)"
            )
        try:
            return self._index2member_map_[self._member2index_map_[self] - other]
        except KeyError:
            raise NotImplementedError(
                "Requested index is out-of-bounds. "
                "(Overflow / wrap-around is not supported.)"
            )

    def __lt__(self, other):
        return self._member2index_map_[self] < self._member2index_map_[other]

    def __le__(self, other):
        return self._member2index_map_[self] <= self._member2index_map_[other]


class Freq(OrderedEnum):
    second = "1Sec"
    min_1 = "1Min"
    min_2 = "2Min"
    min_5 = "5Min"
    min_10 = "10Min"
    min_15 = "15Min"
    min_30 = "30Min"
    hour = "1H"
    day = "1D"
    week = "1W"
    month = "1M"
    quarter = "3M"
    year = "1Y"
