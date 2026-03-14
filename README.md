# pymarketstore
Python driver for MarketStore

Pymarketstore can query and write financial timeseries data from [MarketStore](https://github.com/briancappello/marketstore)

Tested with 3.12+

## How to install

```
$ uv add pymarketstore
```

## Store (high-level API)

```python
import pandas as pd

from pymarketstore import Store, Freq

store = Store()

symbols: list[str] = store.get_symbols()

df: pd.DataFrame = store.get('AMD', freq=Freq.day)
latest_bars: dict[str, pd.DataFrame] = store.get(['AMD', 'NVDA', 'INTC'], freq=Freq.min_1, limit=1)

store.write('AMD', Freq.day, df)
```

## Client (low-level API)

`pymkts.Client(endpoint='http://localhost:5993/rpc')`

Construct a client object with endpoint.

## Query

`pymkts.Client#query(symbols, timeframe, attrgroup, start=None, end=None, limit=None, limit_from_start=False)`

You can build parameters using `pymkts.Params`.

- symbols: string for a single symbol or a list of symbol string for multi-symbol query
- timeframe: timeframe string
- attrgroup: attribute group string.  symbols, timeframe and attrgroup compose a bucket key to query in the server
- start: unix epoch second (int), datetime object or timestamp string. The result will include only data timestamped equal to or after this time.
- end: unix epoch second (int), datetime object or timestamp string.  The result will include only data timestamped equal to or before this time.
- limit: the number of records to be returned, counting from either start or end boundary.
- limit_from_start: boolean to indicate `limit` is from the start boundary.  Defaults to False.

Pass one or multiple instances of `Params` to `Client.query()`.  It will return `QueryReply` object which holds internal numpy array data returned from the server.

## Write

`pymkts.Client#write(data, tbk)`

You can write a numpy array to the server via `Client.write()` method.  The data parameter must be numpy's [recarray type](https://docs.scipy.org/doc/numpy-dev/reference/generated/numpy.recarray.html) with
a column named `Epoch` in int64 type at the first column.  `tbk` is the bucket key of the data records.

## List Symbols

`pymkts.Client#list_symbols()`

The list of all symbols stored in the server are returned.

## Server version

`pymkts.Client#server_version()`

Returns a string of Marketstore-Version header from a server response.

## Streaming

If the server supports WebSocket streaming, you can connect to it using
`pymkts.StreamConn` class.  For convenience, you can call `pymkts.Client#stream()` to obtain the instance with the same server
information as REST client.

Once you have this instance, you will set up some event handles by
either `register()` method or `@on()` decorator.  These methods accept
regular expressions to filter which stream to act on.

To actually connect and start receiving the messages from the server,
you will call `run()` with the stream names.  By default, it subscribes
to all by `*/*/*`.

`pymkts.Client#stream()`

Return a `StreamConn` which is a websocket connection to the server.

`pymkts.StreamConn#(endpoint)`

Create a connection instance to the `endpoint` server. The endpoint
string is a full URL with "ws" or "wss" scheme with the port and path.

`pymkts.StreamConn#register(stream_path, func)`
`@pymkts.StreamConn#on(stream_path)`

Add a new message handler to the connection.  The function will be called
with `handler(StreamConn, {"key": "...", "data": {...,}})` if the key
(time bucket key) matches with the `stream_path` regular expression.
The `on` method is a decorator version of `register`.

`pymkts.StreamConn#run([stream1, stream2, ...])`

Start communication with the server and go into an indefinite loop. It
does not return until unhandled exception is raised, in which case the
connection is closed so you need to implement retry.  Also, since this is
a blocking method, you may need to run it in a background thread.


An example code is as follows.

```
import pymarketstore as pymkts

conn = pymkts.StreamConn('ws://localhost:5993/ws')

@conn.on(r'^BTC/')
def on_btc(conn, msg):
    print('received btc', msg['data'])

conn.run(['BTC/*/*'])  # runs until exception

-> received btc {'Open': 4370.0, 'High': 4372.93, 'Low': 4370.0, 'Close': 4371.74, 'Volume': 3.3880948699999993, 'Epoch': 1507299600}
```


## Async Streaming

`AsyncStreamConn` is the asyncio-native streaming client. It uses the `websockets`
library and supports automatic reconnection, making it suitable for use inside
asyncio event loops (e.g. NautilusTrader or a custom `asyncio.run()` entrypoint).

`pymkts.AsyncStreamConn(endpoint, reconnect_delay=3.0)`

Create an async connection instance. `endpoint` is a full WebSocket URL (`ws://`
or `wss://`). `reconnect_delay` is the number of seconds to wait between
reconnection attempts after an unexpected disconnect.

`pymkts.AsyncStreamConn#register(stream_pat, func)`
`@pymkts.AsyncStreamConn#on(stream_pat)`

Register a message handler. `stream_pat` is a regular expression matched against
the stream key. The handler is called as `handler(key: str, data: dict)` — note
that unlike the sync `StreamConn`, the key and data are passed as two separate
arguments rather than a single message dict.

`pymkts.AsyncStreamConn#deregister(stream_pat)`

Remove a previously registered handler. Silently ignored if the pattern has no
registered handler.

`await pymkts.AsyncStreamConn#run([stream1, stream2, ...])`

Connect to the server, subscribe to the given stream patterns, and enter a
receive loop. Reconnects automatically on disconnect. The coroutine runs until
`stop()` is called or the task is cancelled.

`await pymkts.AsyncStreamConn#stop()`

Gracefully close the WebSocket connection and exit the `run()` loop.

```python
import asyncio
import pymarketstore as pymkts

conn = pymkts.AsyncStreamConn('ws://localhost:5993/ws', reconnect_delay=3.0)

@conn.on(r'^BTC/')
def on_btc(key: str, data: dict):
    print('received btc', key, data)

asyncio.run(conn.run(['BTC/*/*']))

# -> received btc BTC/1Min/OHLCV {'Open': 4370.0, 'High': 4372.93, 'Low': 4370.0, 'Close': 4371.74, 'Volume': 3.39, 'Epoch': 1507299600}
```

To stop the connection from within a running event loop, call `await conn.stop()`
from another coroutine, or cancel the task returned by `asyncio.create_task()`:

```python
async def main():
    conn = pymkts.AsyncStreamConn('ws://localhost:5993/ws')

    @conn.on(r'^BTC/')
    def on_btc(key: str, data: dict):
        print('received btc', key, data)

    task = asyncio.create_task(conn.run(['BTC/*/*']))

    await asyncio.sleep(60)  # stream for 60 seconds, then stop
    await conn.stop()
    await task

asyncio.run(main())
```

## Proto Update Workflow Summary

### For marketstore (Go server):
1. Edit proto/marketstore.proto with new fields

2. Install compatible protoc plugins (one-time setup)

```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
```

3. Regenerate Go files

```shell
cd proto && make protoc
```

### For pymarketstore (Python client):

#### For local development

```shell
cd ~/dev/pymarketstore/
cp ~/dev/marketstore/proto/marketstore.proto ./pymarketstore/proto/
uv run python -m grpc_tools.protoc -I./ --python_out=./ --grpc_python_out=./ ./pymarketstore/proto/marketstore.proto
```

## License

Apache 2.0
