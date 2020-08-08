from __future__ import absolute_import

import logging
import re
from typing import Any
from typing import Union, Dict, List

import numpy as np
import pandas as pd
import requests

from .jsonrpc import MsgpackRpcClient
from .params import Params
from .results import QueryReply
from .stream import StreamConn

logger = logging.getLogger(__name__)


def isiterable(something: Any) -> bool:
    return isinstance(something, (list, tuple, set))


def get_timestamp(value: Union[int, str]) -> pd.Timestamp:
    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return pd.Timestamp(value, unit='s')
    return pd.Timestamp(value)


class JsonRpcClient(object):

    def __init__(self, endpoint: str = 'http://localhost:5993/rpc', ):
        self.endpoint = endpoint
        self.rpc = MsgpackRpcClient(self.endpoint)

    def _request(self, method: str, **query) -> Dict:
        try:
            return self.rpc.call(method, **query)
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise

    def query(self, params: Params) -> QueryReply:
        if not isiterable(params):
            params = [params]

        reply = self._request('DataService.Query', requests=[
            p.to_query_request() for p in params
        ])
        return QueryReply.from_response(reply)

    def write(self, recarray: np.array, tbk: str, isvariablelength: bool = False) -> str:
        data = {}
        data['types'] = [
            recarray.dtype[name].str.replace('<', '')
            for name in recarray.dtype.names
        ]
        data['names'] = recarray.dtype.names
        data['data'] = [
            bytes(memoryview(recarray[name]))
            for name in recarray.dtype.names
        ]
        data['length'] = len(recarray)
        data['startindex'] = {tbk: 0}
        data['lengths'] = {tbk: len(recarray)}
        write_request = {}
        write_request['dataset'] = data
        write_request['is_variable_length'] = isvariablelength
        writer = {}
        writer['requests'] = [write_request]

        try:
            return self.rpc.call("DataService.Write", **writer)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                "Could not contact server")

    def list_symbols(self) -> List[str]:
        reply = self._request('DataService.ListSymbols')
        if 'Results' in reply.keys():
            return reply['Results']
        return []

    def destroy(self, tbk: str) -> Dict:
        """
        Delete a bucket
        :param tbk: Time Bucket Key Name (i.e. "TEST/1Min/Tick" )
        :return: reply object
        """
        destroy_req = {'requests': [{'key': tbk}]}
        reply = self._request('DataService.Destroy', **destroy_req)
        return reply

    def server_version(self) -> str:
        resp = requests.head(self.endpoint)
        return resp.headers.get('Marketstore-Version')

    def stream(self):
        endpoint = re.sub('^http', 'ws',
                          re.sub(r'/rpc$', '/ws', self.endpoint))
        return StreamConn(endpoint)

    def __repr__(self):
        return 'MsgPackRPCClient("{}")'.format(self.endpoint)
