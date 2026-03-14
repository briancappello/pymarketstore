"""
Command-line interface for pymarketstore.
"""

import sys

import click

from tabulate import tabulate

import pymarketstore as pymkts

from .enums import Freq


@click.group()
@click.version_option(version=pymkts.__version__, prog_name="pymkts")
def cli():
    """pymkts - Query and manage MarketStore data."""
    pass


@cli.command()
@click.argument("symbols")
@click.argument("freq", type=Freq, default=Freq.day)
@click.option(
    "--start",
    "-s",
    type=str,
    default=None,
    help="Start date/time (e.g., '2024-01-01', '2024-01-01T09:30:00')",
)
@click.option(
    "--end",
    "-e",
    type=str,
    default=None,
    help="End date/time (e.g., '2024-12-31', '2024-12-31T16:00:00')",
)
@click.option(
    "--limit",
    "-n",
    type=int,
    default=None,
    help="Limit number of records returned",
)
@click.option(
    "--format",
    "-f",
    "output_format",
    type=click.Choice(["table", "csv", "json"]),
    default="table",
    help="Output format",
)
def query(symbols, freq, start, end, limit, output_format):
    """Query OHLCV data for a SYMBOL at a given TIMEFRAME.

    Examples:
        pymkts query AAPL 1D
        pymkts query BTCUSD 1Min --start 2024-01-01 --end 2024-01-31
        pymkts query TSLA 1H --limit 100 --format csv
    """
    store = pymkts.Store()

    data = store.get(
        symbols=[x.strip() for x in symbols.split(",")],
        freq=freq,
        start_dt=start,
        end_dt=end,
        limit=limit,
    )

    if data is None or not len(data):
        click.echo(f"No data found for {','.join(symbols)}/{freq}/OHLCV", err=True)
        sys.exit(1)

    for symbol, df in data.items():
        print(f"\n{symbol}")
        _output_dataframe(df, output_format)


@cli.command("list")
@click.option(
    "--host",
    "-h",
    type=str,
    default="localhost",
    help="MarketStore server host",
)
@click.option(
    "--port",
    "-p",
    type=int,
    default=5993,
    help="MarketStore server port",
)
@click.option(
    "--freq",
    "-f",
    type=str,
    default=None,
    help="Filter by timeframe/frequency (e.g., '1D', '1Min')",
)
@click.option(
    "--tbk",
    "-t",
    is_flag=True,
    default=False,
    help="Show full time bucket keys (symbol/timeframe/attrgroup)",
)
@click.option(
    "--grpc",
    "-g",
    is_flag=True,
    default=False,
    help="Use gRPC instead of JSON-RPC",
)
def list_symbols(host, port, freq, tbk, grpc):
    """List all symbols stored in MarketStore with their available timeframes.

    Examples:
        pymkts list
        pymkts list --freq 1D
        pymkts list --freq 1Min --host 192.168.1.100
    """
    endpoint = f"http://{host}:{port}/rpc"

    try:
        client = pymkts.Client(endpoint=endpoint, grpc=grpc)
        tbks = client.list_symbols(fmt=pymkts.ListSymbolsFormat.TBK)

        if tbk:
            # Show full time bucket keys (optionally filtered)
            for key in sorted(tbks):
                if freq is None or key.split("/")[1] == freq:
                    click.echo(key)
        else:
            # Show symbols with timeframes and date ranges in a table
            symbol_data = _get_symbol_data(client, tbks, freq)

            if not symbol_data:
                if freq:
                    click.echo(f"No symbols found with timeframe '{freq}'", err=True)
                else:
                    click.echo("No symbols found", err=True)
                sys.exit(1)

            # Build table rows
            rows = []
            for symbol in sorted(symbol_data.keys()):
                timeframes = symbol_data[symbol]
                for tf in sorted(timeframes.keys(), key=_timeframe_sort_key):
                    start_date, end_date = timeframes[tf]
                    rows.append([symbol, tf, start_date, end_date])

            headers = ["Symbol", "Timeframe", "Start", "End"]
            click.echo(tabulate(rows, headers=headers, tablefmt="simple"))

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def _get_symbol_data(client, tbks, freq_filter=None):
    """
    Get symbol data with date ranges.

    Returns dict: {symbol: {timeframe: (start_date, end_date), ...}, ...}
    """
    # Parse TBKs into symbol -> timeframes mapping
    symbol_timeframes = {}
    for tbk in tbks:
        parts = tbk.split("/")
        if len(parts) >= 3:
            symbol, timeframe, attrgroup = parts[0], parts[1], parts[2]
            # Only include OHLCV attrgroup and apply frequency filter
            if attrgroup == "OHLCV":
                if freq_filter is None or timeframe == freq_filter:
                    if symbol not in symbol_timeframes:
                        symbol_timeframes[symbol] = set()
                    symbol_timeframes[symbol].add(timeframe)

    # Query date ranges for each symbol/timeframe combination
    symbol_data = {}
    for symbol, timeframes in symbol_timeframes.items():
        symbol_data[symbol] = {}
        for tf in timeframes:
            start_date, end_date = _get_date_range(client, symbol, tf)
            symbol_data[symbol][tf] = (start_date, end_date)

    return symbol_data


def _get_date_range(client, symbol, timeframe):
    """Query the first and last record dates for a symbol/timeframe."""
    try:
        # Get first record
        params_start = pymkts.Params(
            symbols=symbol,
            timeframe=timeframe,
            attrgroup="OHLCV",
            limit=1,
            limit_from_start=True,
        )
        reply_start = client.query(params_start)
        df_start = reply_start.first().df()

        # Get last record
        params_end = pymkts.Params(
            symbols=symbol,
            timeframe=timeframe,
            attrgroup="OHLCV",
            limit=1,
            limit_from_start=False,
        )
        reply_end = client.query(params_end)
        df_end = reply_end.first().df()

        if df_start.empty or df_end.empty:
            return ("N/A", "N/A")

        start_ts = df_start.index[0]
        end_ts = df_end.index[0]

        # Format based on timeframe
        start_str = _format_timestamp(start_ts, timeframe)
        end_str = _format_timestamp(end_ts, timeframe)
        return (start_str, end_str)

    except Exception:
        return ("N/A", "N/A")


def _is_daily_or_higher(timeframe):
    """Check if timeframe is daily or higher (D, W, M, Y)."""
    daily_units = {"D", "W", "M", "Y"}
    for unit in daily_units:
        if timeframe.endswith(unit):
            return True
    return False


def _format_timestamp(ts, timeframe):
    """Format timestamp based on timeframe.

    For daily or higher: just show date (YYYY-MM-DD)
    For intraday: show datetime in America/New_York
    """
    if _is_daily_or_higher(timeframe):
        return ts.strftime("%Y-%m-%d")
    else:
        # Convert to America/New_York timezone
        try:
            import zoneinfo

            ny_tz = zoneinfo.ZoneInfo("America/New_York")
        except ImportError:
            # Python < 3.9 fallback (shouldn't happen with requires-python >= 3.9)
            import pytz

            ny_tz = pytz.timezone("America/New_York")

        ts_ny = (
            ts.tz_convert(ny_tz) if ts.tzinfo else ts.tz_localize("UTC").tz_convert(ny_tz)
        )
        return ts_ny.strftime("%Y-%m-%d %H:%M")


def _timeframe_sort_key(tf):
    """Sort timeframes by duration (smallest to largest)."""
    unit_order = {"Sec": 0, "Min": 1, "H": 2, "D": 3, "W": 4, "M": 5, "Y": 6}

    for unit, order in unit_order.items():
        if tf.endswith(unit):
            try:
                num = int(tf[: -len(unit)]) if tf[: -len(unit)] else 1
                return (order, num)
            except ValueError:
                pass

    return (999, tf)


@cli.command()
@click.argument("symbol")
@click.option(
    "--freq",
    "-f",
    type=str,
    default=None,
    help="Only delete specific timeframe (e.g., '1D', '1Min'). If not specified, deletes all timeframes.",
)
@click.option(
    "--host",
    "-h",
    type=str,
    default="localhost",
    help="MarketStore server host",
)
@click.option(
    "--port",
    "-p",
    type=int,
    default=5993,
    help="MarketStore server port",
)
@click.option(
    "--grpc",
    "-g",
    is_flag=True,
    default=False,
    help="Use gRPC instead of JSON-RPC",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Skip confirmation prompt",
)
def delete(symbol, freq, host, port, grpc, yes):
    """Delete all data for a SYMBOL from MarketStore.

    \b
    Examples:
        pymkts delete AAPL
        pymkts delete BTCUSD --freq 1Min
        pymkts delete TSLA --yes
    """
    endpoint = f"http://{host}:{port}/rpc"

    try:
        client = pymkts.Client(endpoint=endpoint, grpc=grpc)

        # Get all TBKs for this symbol
        all_tbks = client.list_symbols(fmt=pymkts.ListSymbolsFormat.TBK)
        symbol_tbks = [
            tbk
            for tbk in all_tbks
            if tbk.startswith(f"{symbol}/")
            and (freq is None or tbk.split("/")[1] == freq)
        ]

        if not symbol_tbks:
            if freq:
                click.echo(
                    f"No data found for {symbol} with timeframe '{freq}'", err=True
                )
            else:
                click.echo(f"No data found for {symbol}", err=True)
            sys.exit(1)

        # Show what will be deleted
        click.echo("The following time bucket keys will be deleted:")
        for tbk in sorted(symbol_tbks):
            click.echo(f"  - {tbk}")

        # Confirm deletion
        if not yes:
            if not click.confirm("\nAre you sure you want to delete this data?"):
                click.echo("Aborted.")
                sys.exit(0)

        # Delete each TBK
        deleted = 0
        errors = []
        for tbk in symbol_tbks:
            try:
                client.destroy(tbk)
                click.echo(f"Deleted: {tbk}")
                deleted += 1
            except Exception as e:
                errors.append((tbk, str(e)))

        # Summary
        click.echo(f"\nDeleted {deleted} time bucket key(s).")
        if errors:
            click.echo(f"Failed to delete {len(errors)} time bucket key(s):", err=True)
            for tbk, error in errors:
                click.echo(f"  - {tbk}: {error}", err=True)
            sys.exit(1)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--host",
    "-h",
    type=str,
    default="localhost",
    help="MarketStore server host",
)
@click.option(
    "--port",
    "-p",
    type=int,
    default=5993,
    help="MarketStore server port",
)
@click.option(
    "--grpc",
    "-g",
    is_flag=True,
    default=False,
    help="Use gRPC instead of JSON-RPC",
)
def version(host, port, grpc):
    """Show MarketStore server version."""
    endpoint = f"http://{host}:{port}/rpc"

    try:
        client = pymkts.Client(endpoint=endpoint, grpc=grpc)
        server_version = client.server_version()
        click.echo(f"Server version: {server_version}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def _output_dataframe(df, output_format):
    """Output a DataFrame in the specified format."""
    if output_format == "csv":
        click.echo(df.to_csv())
    elif output_format == "json":
        click.echo(df.to_json(orient="index", date_format="iso", indent=2))
    elif output_format == "verbose":  # table
        click.echo(tabulate(df, headers="keys", tablefmt="simple"))
    else:
        print(df)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
