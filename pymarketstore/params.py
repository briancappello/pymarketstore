from enum import Enum
from typing import *

from .utils import get_timestamp, is_iterable


class DataType(Enum):
    float32 = "FLOAT32"
    float64 = "FLOAT64"

    int16 = "INT16"
    int32 = "INT32"
    int64 = "INT64"

    uint8 = "UINT8"
    uint16 = "UINT16"
    uint32 = "UINT32"
    uint64 = "UINT64"

    epoch = "EPOCH"
    byte = "BYTE"
    bool = "BOOL"
    none = "NONE"
    string = "STRING"


class DataShape:
    def __init__(self, data_shape: List[Tuple[str, Union[DataType, str]]] = None):
        self.col_names = set()
        self.data_shape = []
        for col_name, data_type in (data_shape or ()):
            self.add(col_name, data_type)

    def add(self, col_name: str, data_type: Union[DataType, str]):
        if col_name in self.col_names:
            raise ValueError(f"`{col_name}` has already been specified")
        if isinstance(data_type, str):
            data_type = DataType[data_type.lower()]
        self.col_names.add(col_name)
        self.data_shape.append((col_name, data_type))

    def __iter__(self):
        return iter(self.data_shape)


class ListSymbolsFormat(Enum):
    """
    format of the list_symbols response.
    """
    # symbol names only. (e.g. ["AAPL", "AMZN", ...])
    SYMBOL = "symbol"
    # {symbol}/{timeframe}/{attribute_group} format. (e.g. ["AAPL/1Min/TICK", "AMZN/1Sec/OHLCV",...])
    TBK = "tbk"


class Params:

    def __init__(self, symbols: Union[List[str], str], timeframe: str, attrgroup: str,
                 start: Union[int, str] = None, end: Union[int, str] = None,
                 limit: int = None, limit_from_start: bool = None,
                 columns: List[str] = None):
        if not is_iterable(symbols):
            symbols = [symbols]
        self.tbk = ','.join(symbols) + "/" + timeframe + "/" + attrgroup
        self.key_category = None  # server default
        self.start = get_timestamp(start)
        self.end = get_timestamp(end)
        self.limit = limit
        self.limit_from_start = limit_from_start
        self.columns = columns
        self.functions = None

    def set(self, key: str, val: Any):
        if not hasattr(self, key):
            raise AttributeError()
        if key in ('start', 'end'):
            setattr(self, key, get_timestamp(val))
        else:
            setattr(self, key, val)
        return self

    def to_query_request(self) -> dict:
        query = {'destination': self.tbk}
        if self.key_category is not None:
            query['key_category'] = self.key_category
        if self.start is not None:
            query['epoch_start'], start_nanos = divmod(self.start.value, 10 ** 9)
            if start_nanos != 0:
                query['epoch_start_nanos'] = start_nanos
        if self.end is not None:
            query['epoch_end'], end_nanos = divmod(self.end.value, 10 ** 9)
            if end_nanos != 0:
                query['epoch_end_nanos'] = end_nanos
        if self.limit is not None:
            query['limit_record_count'] = self.limit
        if self.limit_from_start is not None:
            query['limit_from_start'] = bool(self.limit_from_start)
        if self.functions is not None:
            query['functions'] = self.functions
        return query

    def __repr__(self) -> str:
        content = ('tbk={}, start={}, end={}, '.format(
            self.tbk, self.start, self.end,
        ) +
                   'limit={}, '.format(self.limit) +
                   'limit_from_start={}'.format(self.limit_from_start) +
                   'columns={}'.format(self.columns))
        return 'Params({})'.format(content)
