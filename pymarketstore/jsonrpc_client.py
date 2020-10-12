import logging
import numpy as np
import pandas as pd
import re
import requests

from typing import Union, Dict, List

from .jsonrpc import MsgpackRpcClient
from .params import DataShapes, Params, ListSymbolsFormat
from .results import QueryReply
from .stream import StreamConn
from .utils import is_iterable, timeseries_data_to_write_request

logger = logging.getLogger(__name__)


class JsonRpcClient(object):

    def __init__(self, endpoint: str = 'http://localhost:5993/rpc'):
        self.endpoint = endpoint
        self.rpc = MsgpackRpcClient(self.endpoint)

    def _request(self, method: str, **query) -> Dict:
        try:
            return self.rpc.call(method, **query)
        except requests.exceptions.ConnectionError:
            msg = 'Could not connect to marketstore at {}'.format(self.endpoint)
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise
        raise Exception(msg)

    def query(self, params: Union[Params, List[Params]]) -> QueryReply:
        if not is_iterable(params):
            params = [params]

        reply = self._request('DataService.Query', requests=[
            p.to_query_request() for p in params
        ])
        return QueryReply.from_response(reply)

    def write(self,
              data: Union[pd.DataFrame, pd.Series, np.ndarray, np.recarray],
              tbk: str,
              isvariablelength: bool = False,
              ) -> dict:
        dataset = timeseries_data_to_write_request(data, tbk)
        return self._request("DataService.Write", requests=[dict(
            dataset=dict(
                types=dataset['column_types'],
                names=dataset['column_names'],
                data=dataset['column_data'],
                startindex={tbk: 0},
                lengths={tbk: len(data)},
            ),
            is_variable_length=isvariablelength,
        )])

    def list_symbols(self, fmt: ListSymbolsFormat = ListSymbolsFormat.SYMBOL) -> List[str]:
        reply = self._request('DataService.ListSymbols', format=fmt.value)
        return reply.get('Results') or []

    def create(self, tbk: str, data_shapes: DataShapes, row_type: str = "fixed"):
        if row_type not in {"fixed", "variable"}:
            raise TypeError("`row_type` must be 'fixed' or 'variable'")
        return self._request('DataService.Create', requests=[dict(
            key=f"{tbk}:Symbol/Timeframe/AttributeGroup",
            datashapes=str(data_shapes),
            rowtype=row_type,
        )])

    def destroy(self, tbk: str) -> Dict:
        """
        Delete a bucket
        :param tbk: Time Bucket Key Name (i.e. "TEST/1Min/Tick" )
        :return: reply object
        """
        return self._request('DataService.Destroy', requests=dict(key=tbk))

    def server_version(self) -> str:
        resp = requests.head(self.endpoint)
        return resp.headers.get('Marketstore-Version')

    def stream(self):
        endpoint = re.sub('^http', 'ws',
                          re.sub(r'/rpc$', '/ws', self.endpoint))
        return StreamConn(endpoint)

    def __repr__(self):
        return 'MsgPackRPCClient("{}")'.format(self.endpoint)
