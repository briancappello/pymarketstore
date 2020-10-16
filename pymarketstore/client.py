import logging
import numpy as np
import pandas as pd
import re

from typing import *

from .grpc_client import GRPCClient
from .jsonrpc_client import JsonRpcClient
from .params import DataShape, DataType, ListSymbolsFormat, Params
from .results import QueryReply

logger = logging.getLogger(__name__)

http_regex = re.compile(r'^https?://(.+):\d+/rpc')  # http:// or https://


class Client:
    def __init__(self, endpoint: str = 'http://localhost:5993/rpc', grpc: bool = False):
        self.endpoint = endpoint
        if not grpc:
            self.client = JsonRpcClient(self.endpoint)
            return

        # when endpoint is specified in "http://{host}:{port}/rpc" format,
        # extract the host and initialize GRPC client with default port(5995) for compatibility
        match = re.findall(http_regex, endpoint)
        if match:
            host = match[0] if match[0] != "" else "localhost"  # default host is "localhost"
            self.endpoint = "{}:5995".format(host)  # default port is 5995
        self.client = GRPCClient(self.endpoint)

    def query(self, params: Union[Params, List[Params]]) -> QueryReply:
        """
        query the MarketStore server

        :param params: Params object used to query
        :return: QueryReply object
        """
        return self.client.query(params)

    def write(self,
              data: Union[pd.DataFrame, pd.Series, np.ndarray, np.recarray],
              tbk: str,
              is_variable_length: bool = False,
              ) -> dict:
        """
        write data to the MarketStore server

        :param data: A pd.DataFrame, pd.Series, np.ndarray, or np.recarray to write
        :param tbk: Time Bucket Key string.
        ('{symbol name}/{time frame}/{attribute group name}' ex. 'TSLA/1Min/OHLCV' , 'AAPL/1Min/TICK' )
        :param is_variable_length: should be set true if the record content is variable-length array
        :return:
        """
        return self.client.write(data, tbk, is_variable_length=is_variable_length)

    def list_symbols(self, fmt: ListSymbolsFormat = ListSymbolsFormat.SYMBOL) -> List[str]:
        """
        list symbols stored on the MarketStore server

        Optionally specify `fmt=ListSymbolsFormat.TBK` to get back a list of
        time bucket keys

        :param fmt: The symbol format to request
        :type fmt: ListSymbolsFormat
        """
        return self.client.list_symbols(fmt)

    def create(self,
               tbk: str,
               data_shape: Union[DataShape, List[Tuple[str, Union[DataType, str]]]],
               row_type: str = "fixed",
               ) -> Dict:
        """
        create a new time bucket key on the MarketStore server

        :param tbk: The time bucket key to create (eg 'TSLA/1D/OHLCV')
        :param data_shape: The shape of the data (column names and their types)
        :param row_type: Whether the data's row type is "fixed" or "variable"
        """
        if not isinstance(data_shape, DataShape):
            data_shape = DataShape(data_shape)
        return self.client.create(tbk, data_shape, row_type)

    def destroy(self, tbk: str) -> Dict:
        """
        delete a time bucket key and its data from the MarketStore server

        :param tbk: The time bucket key to delete (eg 'TSLA/1D/OHLCV')
        """
        return self.client.destroy(tbk)

    def server_version(self) -> str:
        return self.client.server_version()

    def __repr__(self):
        return self.client.__repr__()
