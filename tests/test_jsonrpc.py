import pytest

from unittest.mock import patch

from pymarketstore import jsonrpc_client


@patch.object(jsonrpc_client, 'requests')
def test_jsonrpc(requests):
    requests.Session().post.return_value = 'dummy_data'

    cli = jsonrpc_client.MsgpackRpcClient('http://localhost:5993/rcp')
    result = cli._rpc_request('DataService.Query', a=1)
    assert result == 'dummy_data'
    resp = {
        'jsonrpc': '2.0',
        'id': 1,
        'result': {'ok': True},
    }
    assert cli._rpc_response(resp)['ok']

    del resp['result']
    resp['error'] = {
        'message': 'Error',
        'data': 'something',
    }
    with pytest.raises(Exception) as e:
        cli._rpc_response(resp)
    assert 'Error: something' in str(e)
