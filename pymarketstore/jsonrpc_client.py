import logging
import msgpack
import numpy as np
import pandas as pd
import re
import requests

from typing import *

from .params import DataShape, ListSymbolsFormat, Params
from .results import QueryReply
from .stream import StreamConn
from .utils import is_iterable, timeseries_data_to_write_request

logger = logging.getLogger(__name__)


class JsonRpcClient:

    def __init__(self, endpoint: str = 'http://localhost:5993/rpc'):
        self.endpoint = endpoint
        self.rpc = MsgpackRpcClient(self.endpoint)

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
              is_variable_length: bool = False,
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
            is_variable_length=is_variable_length,
        )])

    def list_symbols(self, fmt: ListSymbolsFormat = ListSymbolsFormat.SYMBOL) -> List[str]:
        reply = self._request('DataService.ListSymbols', format=fmt.value)
        return reply.get('Results') or []

    def create(self, tbk: str, data_shape: DataShape, row_type: str = "fixed"):
        if row_type not in {"fixed", "variable"}:
            raise TypeError("`row_type` must be 'fixed' or 'variable'")
        return self._request('DataService.Create', requests=[dict(
            key=tbk,
            data_shapes=':'.join(f'{col}/{data_type.value.lower()}'
                                 for col, data_type in data_shape),
            row_type=row_type,
        )])

    def destroy(self, tbk: str) -> Dict:
        """
        Delete a bucket
        :param tbk: Time Bucket Key Name (i.e. "TEST/1Min/Tick" )
        :return: reply object
        """
        return self._request('DataService.Destroy', requests=[dict(key=tbk)])

    def server_version(self) -> str:
        resp = requests.head(self.endpoint)
        return resp.headers.get('Marketstore-Version')

    def stream(self):
        endpoint = re.sub('^http', 'ws',
                          re.sub(r'/rpc$', '/ws', self.endpoint))
        return StreamConn(endpoint)

    def _request(self, method: str, **query) -> Dict:
        try:
            return self.rpc.call(method, **query)
        except requests.exceptions.ConnectionError:
            msg = 'Could not connect to marketstore at {}'.format(self.endpoint)
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise
        raise Exception(msg)

    def __repr__(self):
        return 'MsgPackRPCClient("{}")'.format(self.endpoint)


class MsgpackRpcClient:
    codec = msgpack
    mimetype = "application/x-msgpack"

    def __init__(self, endpoint: str):
        if not endpoint:
            raise ValueError('The `endpoint` parameter is required')

        self._id = 1
        self._endpoint = endpoint
        self._session = requests.Session()

    def call(self, rpc_method: str, **query):
        reply = self._rpc_request(rpc_method, **query)
        return self._rpc_response(reply)

    def _rpc_request(self, method: str, **query) -> Union[Dict, requests.Response]:
        http_resp = self._session.post(
            self._endpoint,
            data=self.codec.dumps(dict(
                method=method,
                id=str(self._id),
                jsonrpc='2.0',
                params=query,
            )),
            headers={"Content-Type": self.mimetype}
        )

        # compat with unittest.mock
        if (not isinstance(requests.Response, type)
                or not isinstance(http_resp, requests.Response)):
            return http_resp

        http_resp.raise_for_status()
        return self.codec.loads(http_resp.content)

    @staticmethod
    def _rpc_response(reply: Dict) -> dict:
        error = reply.get('error', None)
        if error:
            raise Exception('{}: {}'.format(error['message'],
                                            error.get('data', '')))

        if 'result' in reply:
            return reply['result']

        raise Exception('invalid JSON-RPC protocol: missing error or result key')
