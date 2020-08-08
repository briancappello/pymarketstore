from typing import Union, List, Any
import pandas as pd
import numpy as np

def get_timestamp(value: Union[int, str]) -> pd.Timestamp:
    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return pd.Timestamp(value, unit='s')
    return pd.Timestamp(value)


def isiterable(something: Any) -> bool:
    """
    check if something is a list, tuple or set
    :param something: any object
    :return: bool. true if something is a list, tuple or set
    """
    return isinstance(something, (list, tuple, set))


class Params(object):

    def __init__(self, symbols: Union[List[str], str], timeframe: str, attrgroup: str,
                 start: Union[int, str] = None, end: Union[int, str] = None,
                 limit: int = None, limit_from_start: bool = None,
                 columns: List[str] = None):
        if not isiterable(symbols):
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
