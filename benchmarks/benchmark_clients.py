"""
Benchmark comparisons for pymarketstore clients (JsonRpcClient vs GRPCClient).

This module provides comprehensive benchmarks for comparing the performance of
different client implementations against a live MarketStore server, especially
for large datasets spanning multiple years of 1-minute OHLCV data.

Usage:
    # Run benchmarks against default endpoints
    python -m benchmarks

    # Specify endpoints
    python -m benchmarks --jsonrpc-endpoint http://localhost:5993/rpc --grpc-endpoint localhost:5995

    # Specify dataset sizes
    python -m benchmarks --sizes 1_month 1_year 3_years

    # Quick test with smaller datasets
    python -m benchmarks --quick
"""

import argparse
import gc
import json
import sys
import time

from datetime import datetime
from typing import Dict, List, Optional

import numpy as np

from .utils import (
    DATASET_SIZES,
    BenchmarkResult,
    compare_results,
    format_duration,
    format_size,
    generate_ohlcv_data,
    get_dataset_size,
    print_results_table,
    run_benchmark,
)


# Import pymarketstore components
try:
    from pymarketstore import GRPCClient, JsonRpcClient, Params
except ImportError as e:
    print(f"Error importing pymarketstore: {e}")
    print("Make sure pymarketstore is installed: pip install -e .")
    sys.exit(1)


class ClientBenchmark:
    """Benchmarks for comparing pymarketstore clients against a live server."""

    def __init__(
        self,
        jsonrpc_endpoint: str = "http://localhost:5993/rpc",
        grpc_endpoint: str = "localhost:5995",
    ):
        self.jsonrpc_endpoint = jsonrpc_endpoint
        self.grpc_endpoint = grpc_endpoint

        # Create clients
        self.jsonrpc_client = JsonRpcClient(self.jsonrpc_endpoint)
        self.grpc_client = GRPCClient(self.grpc_endpoint)

    def _check_connectivity(self) -> Dict[str, bool]:
        """Check if both clients can connect to the server."""
        results = {"jsonrpc": False, "grpc": False}

        try:
            version = self.jsonrpc_client.server_version()
            results["jsonrpc"] = True
            print(f"  JsonRPC connected (server version: {version})")
        except Exception as e:
            print(f"  JsonRPC connection failed: {e}")

        try:
            version = self.grpc_client.server_version()
            results["grpc"] = True
            print(f"  gRPC connected (server version: {version})")
        except Exception as e:
            print(f"  gRPC connection failed: {e}")

        return results

    def _setup_test_data(
        self,
        symbol: str,
        num_records: int,
    ) -> bool:
        """Write test data to the server. Returns True if successful."""
        tbk = f"{symbol}/1Min/OHLCV"

        print(f"  Generating {format_size(num_records)} records...")
        data = generate_ohlcv_data(num_records)

        print(f"  Writing to {tbk}...")
        try:
            self.jsonrpc_client.write(data, tbk)
            return True
        except Exception as e:
            print(f"  ERROR writing data: {e}")
            return False

    def _cleanup_test_data(self, symbol: str):
        """Remove test data from the server."""
        tbk = f"{symbol}/1Min/OHLCV"
        try:
            self.jsonrpc_client.destroy(tbk)
        except Exception:
            pass  # Ignore errors during cleanup

    def benchmark_query(
        self,
        dataset_sizes: List[int],
        iterations: int = 5,
        symbol_prefix: str = "BENCH",
    ) -> Dict[str, List[BenchmarkResult]]:
        """
        Benchmark query operations for both clients.

        This measures the full round-trip time: sending query, receiving data,
        and converting to DataFrame.
        """
        print("\n" + "=" * 80)
        print(" Benchmark: Query Performance")
        print("=" * 80)

        results = {"jsonrpc": [], "grpc": []}

        for size in dataset_sizes:
            symbol = f"{symbol_prefix}_{size}"
            tbk = f"{symbol}/1Min/OHLCV"
            print(f"\nDataset size: {format_size(size)} records ({size:,} 1Min bars)")

            # Setup test data
            if not self._setup_test_data(symbol, size):
                print("  Skipping due to setup failure")
                continue

            # Allow server to settle
            time.sleep(0.5)

            params = Params(symbol, "1Min", "OHLCV")

            # Benchmark JsonRPC
            def jsonrpc_query():
                reply = self.jsonrpc_client.query(params)
                df = reply.first().df()
                return df

            try:
                result = run_benchmark(
                    jsonrpc_query,
                    name=f"JsonRpc Query ({format_size(size)})",
                    dataset_size=size,
                    iterations=iterations,
                )
                results["jsonrpc"].append(result)
                print(
                    f"  JsonRPC: {format_duration(result.mean_time)} mean, "
                    f"{format_size(int(result.throughput))}/s throughput"
                )
            except Exception as e:
                print(f"  JsonRPC ERROR: {e}")

            gc.collect()
            time.sleep(0.2)

            # Benchmark gRPC
            def grpc_query():
                reply = self.grpc_client.query(params)
                df = reply.first().df()
                return df

            try:
                result = run_benchmark(
                    grpc_query,
                    name=f"gRPC Query ({format_size(size)})",
                    dataset_size=size,
                    iterations=iterations,
                )
                results["grpc"].append(result)
                print(
                    f"  gRPC:    {format_duration(result.mean_time)} mean, "
                    f"{format_size(int(result.throughput))}/s throughput"
                )
            except Exception as e:
                print(f"  gRPC ERROR: {e}")

            # Cleanup
            self._cleanup_test_data(symbol)
            gc.collect()

        return results

    def benchmark_write(
        self,
        dataset_sizes: List[int],
        iterations: int = 5,
        symbol_prefix: str = "BENCHW",
    ) -> Dict[str, List[BenchmarkResult]]:
        """
        Benchmark write operations for both clients.

        This measures the time to serialize and send data to the server.
        """
        print("\n" + "=" * 80)
        print(" Benchmark: Write Performance")
        print("=" * 80)

        results = {"jsonrpc": [], "grpc": []}

        for size in dataset_sizes:
            print(f"\nDataset size: {format_size(size)} records ({size:,} 1Min bars)")

            # Pre-generate test data
            data = generate_ohlcv_data(size)

            # Benchmark JsonRPC writes
            write_count = 0

            def jsonrpc_write():
                nonlocal write_count
                symbol = f"{symbol_prefix}_JR_{size}_{write_count}"
                tbk = f"{symbol}/1Min/OHLCV"
                write_count += 1
                self.jsonrpc_client.write(data, tbk)
                return tbk

            try:
                tbks_to_clean = []
                result = run_benchmark(
                    jsonrpc_write,
                    name=f"JsonRpc Write ({format_size(size)})",
                    dataset_size=size,
                    iterations=iterations,
                )
                results["jsonrpc"].append(result)
                print(
                    f"  JsonRPC: {format_duration(result.mean_time)} mean, "
                    f"{format_size(int(result.throughput))}/s throughput"
                )

                # Clean up written data
                for i in range(write_count):
                    symbol = f"{symbol_prefix}_JR_{size}_{i}"
                    self._cleanup_test_data(symbol)
            except Exception as e:
                print(f"  JsonRPC ERROR: {e}")

            gc.collect()
            time.sleep(0.2)

            # Benchmark gRPC writes
            write_count = 0

            def grpc_write():
                nonlocal write_count
                symbol = f"{symbol_prefix}_GR_{size}_{write_count}"
                tbk = f"{symbol}/1Min/OHLCV"
                write_count += 1
                self.grpc_client.write(data, tbk)
                return tbk

            try:
                result = run_benchmark(
                    grpc_write,
                    name=f"gRPC Write ({format_size(size)})",
                    dataset_size=size,
                    iterations=iterations,
                )
                results["grpc"].append(result)
                print(
                    f"  gRPC:    {format_duration(result.mean_time)} mean, "
                    f"{format_size(int(result.throughput))}/s throughput"
                )

                # Clean up written data
                for i in range(write_count):
                    symbol = f"{symbol_prefix}_GR_{size}_{i}"
                    self._cleanup_test_data(symbol)
            except Exception as e:
                print(f"  gRPC ERROR: {e}")

            gc.collect()

        return results

    def benchmark_query_raw(
        self,
        dataset_sizes: List[int],
        iterations: int = 5,
        symbol_prefix: str = "BENCHR",
    ) -> Dict[str, List[BenchmarkResult]]:
        """
        Benchmark query operations returning raw numpy arrays (no DataFrame conversion).

        This isolates the network/protocol performance from pandas overhead.
        """
        print("\n" + "=" * 80)
        print(" Benchmark: Query Performance (Raw Array, No DataFrame)")
        print("=" * 80)

        results = {"jsonrpc": [], "grpc": []}

        for size in dataset_sizes:
            symbol = f"{symbol_prefix}_{size}"
            print(f"\nDataset size: {format_size(size)} records ({size:,} 1Min bars)")

            # Setup test data
            if not self._setup_test_data(symbol, size):
                print("  Skipping due to setup failure")
                continue

            time.sleep(0.5)

            params = Params(symbol, "1Min", "OHLCV")

            # Benchmark gRPC (raw array)
            def grpc_query_raw():
                reply = self.grpc_client.query(params)
                arr = reply.first().array
                return arr

            try:
                result = run_benchmark(
                    grpc_query_raw,
                    name=f"gRPC Raw ({format_size(size)})",
                    dataset_size=size,
                    iterations=iterations,
                )
                results["grpc"].append(result)
                print(
                    f"  gRPC:    {format_duration(result.mean_time)} mean, "
                    f"{format_size(int(result.throughput))}/s throughput"
                )
            except Exception as e:
                print(f"  gRPC ERROR: {e}")

            gc.collect()
            time.sleep(0.2)

            # Benchmark JsonRPC (raw array)
            def jsonrpc_query_raw():
                reply = self.jsonrpc_client.query(params)
                arr = reply.first().array  # Skip DataFrame conversion
                return arr

            try:
                result = run_benchmark(
                    jsonrpc_query_raw,
                    name=f"JsonRpc Raw ({format_size(size)})",
                    dataset_size=size,
                    iterations=iterations,
                )
                results["jsonrpc"].append(result)
                print(
                    f"  JsonRPC: {format_duration(result.mean_time)} mean, "
                    f"{format_size(int(result.throughput))}/s throughput"
                )
            except Exception as e:
                print(f"  JsonRPC ERROR: {e}")

            # Cleanup
            self._cleanup_test_data(symbol)
            gc.collect()

        return results

    def benchmark_multi_symbol_query(
        self,
        num_symbols: int = 10,
        records_per_symbol: int = 10080,  # 1 week of 1Min data
        iterations: int = 5,
        symbol_prefix: str = "BENCHM",
    ) -> Dict[str, List[BenchmarkResult]]:
        """
        Benchmark querying multiple symbols in a single request.

        This tests batch query performance which is common in real-world usage.
        """
        print("\n" + "=" * 80)
        print(f" Benchmark: Multi-Symbol Query ({num_symbols} symbols)")
        print("=" * 80)

        total_records = num_symbols * records_per_symbol
        print(
            f"\nTotal records: {format_size(total_records)} "
            f"({num_symbols} symbols x {format_size(records_per_symbol)} each)"
        )

        results = {"jsonrpc": [], "grpc": []}

        # Setup test data for all symbols
        symbols = [f"{symbol_prefix}_{i}" for i in range(num_symbols)]
        print("  Setting up test data...")

        for symbol in symbols:
            if not self._setup_test_data(symbol, records_per_symbol):
                print(f"  Failed to setup {symbol}, aborting")
                for s in symbols:
                    self._cleanup_test_data(s)
                return results

        time.sleep(1.0)

        # Create params for all symbols
        params_list = [Params(symbol, "1Min", "OHLCV") for symbol in symbols]

        # Benchmark JsonRPC multi-query
        def jsonrpc_multi_query():
            reply = self.jsonrpc_client.query(params_list)
            # Get all DataFrames
            dfs = {key: ds.df() for key, ds in reply.all().items()}
            return dfs

        try:
            result = run_benchmark(
                jsonrpc_multi_query,
                name=f"JsonRpc Multi ({num_symbols} sym)",
                dataset_size=total_records,
                iterations=iterations,
            )
            results["jsonrpc"].append(result)
            print(
                f"  JsonRPC: {format_duration(result.mean_time)} mean, "
                f"{format_size(int(result.throughput))}/s throughput"
            )
        except Exception as e:
            print(f"  JsonRPC ERROR: {e}")

        gc.collect()
        time.sleep(0.5)

        # Benchmark gRPC multi-query
        def grpc_multi_query():
            reply = self.grpc_client.query(params_list)
            dfs = {key: ds.df() for key, ds in reply.all().items()}
            return dfs

        try:
            result = run_benchmark(
                grpc_multi_query,
                name=f"gRPC Multi ({num_symbols} sym)",
                dataset_size=total_records,
                iterations=iterations,
            )
            results["grpc"].append(result)
            print(
                f"  gRPC:    {format_duration(result.mean_time)} mean, "
                f"{format_size(int(result.throughput))}/s throughput"
            )
        except Exception as e:
            print(f"  gRPC ERROR: {e}")

        # Cleanup
        print("  Cleaning up test data...")
        for symbol in symbols:
            self._cleanup_test_data(symbol)

        return results

    def run_all_benchmarks(
        self,
        dataset_sizes: Optional[List[int]] = None,
        iterations: int = 5,
    ) -> Dict[str, Dict[str, List[BenchmarkResult]]]:
        """
        Run all benchmark suites.

        Args:
            dataset_sizes: List of dataset sizes to test. Defaults to large datasets.
            iterations: Number of iterations per benchmark

        Returns:
            Dictionary of all benchmark results
        """
        if dataset_sizes is None:
            # Default: focus on larger datasets (multi-year 1Min data)
            dataset_sizes = [
                DATASET_SIZES["1_month"],  # 43,200 records
                DATASET_SIZES["3_months"],  # 129,600 records
                DATASET_SIZES["1_year"],  # 525,600 records
                DATASET_SIZES["2_years"],  # 1,051,200 records
            ]

        print("\n" + "=" * 80)
        print(" Checking server connectivity...")
        print("=" * 80)
        connectivity = self._check_connectivity()

        if not connectivity["jsonrpc"] and not connectivity["grpc"]:
            print("\nERROR: Cannot connect to MarketStore server!")
            print(f"  JsonRPC endpoint: {self.jsonrpc_endpoint}")
            print(f"  gRPC endpoint: {self.grpc_endpoint}")
            print("\nPlease ensure MarketStore is running and try again.")
            return {}

        all_results = {}

        # Run query benchmarks (with DataFrame conversion)
        all_results["query"] = self.benchmark_query(dataset_sizes, iterations)

        # Run query benchmarks (raw array, no DataFrame)
        all_results["query_raw"] = self.benchmark_query_raw(dataset_sizes, iterations)

        # Run write benchmarks
        all_results["write"] = self.benchmark_write(dataset_sizes, iterations)

        # Run multi-symbol query benchmark
        all_results["multi_symbol"] = self.benchmark_multi_symbol_query(
            num_symbols=10,
            records_per_symbol=DATASET_SIZES["1_week"],
            iterations=iterations,
        )

        return all_results


def print_summary(all_results: Dict[str, Dict[str, List[BenchmarkResult]]]):
    """Print a summary of all benchmark results."""
    print("\n" + "=" * 90)
    print(" BENCHMARK SUMMARY")
    print("=" * 90)

    for benchmark_name, results in all_results.items():
        if not results:
            continue

        print(f"\n{benchmark_name.upper().replace('_', ' ')}")
        print("-" * 50)

        if "jsonrpc" in results and "grpc" in results:
            if results["jsonrpc"] and results["grpc"]:
                compare_results(
                    results["jsonrpc"],
                    results["grpc"],
                    "JsonRPC",
                    "gRPC",
                )


def export_results(
    all_results: Dict[str, Dict[str, List[BenchmarkResult]]],
    output_file: str,
):
    """Export benchmark results to JSON."""
    export_data = {
        "timestamp": datetime.now().isoformat(),
        "benchmarks": {},
    }

    for benchmark_name, results in all_results.items():
        export_data["benchmarks"][benchmark_name] = {
            client: [r.to_dict() for r in client_results]
            for client, client_results in results.items()
        }

    with open(output_file, "w") as f:
        json.dump(export_data, f, indent=2)

    print(f"\nResults exported to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark pymarketstore clients against a live server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full benchmark suite
  python -m benchmarks

  # Quick test with smaller datasets
  python -m benchmarks --quick

  # Test specific dataset sizes (multiple years of 1Min data)
  python -m benchmarks --sizes 1_year 2_years 3_years

  # Custom endpoints
  python -m benchmarks --jsonrpc-endpoint http://myhost:5993/rpc

  # Export results to JSON
  python -m benchmarks --output results.json

Available dataset sizes:
  1_hour    (60 records)
  1_day     (1,440 records)
  1_week    (10,080 records)
  1_month   (43,200 records)
  3_months  (129,600 records)
  6_months  (259,200 records)
  1_year    (525,600 records)
  2_years   (1,051,200 records)
  3_years   (1,576,800 records)
  5_years   (2,628,000 records)

Or specify a number directly (e.g., 100000)
        """,
    )

    parser.add_argument(
        "--jsonrpc-endpoint",
        default="http://localhost:5993/rpc",
        help="JSON-RPC endpoint URL (default: http://localhost:5993/rpc)",
    )
    parser.add_argument(
        "--grpc-endpoint",
        default="localhost:5995",
        help="gRPC endpoint address (default: localhost:5995)",
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        default=["1_month", "3_months", "1_year", "2_years"],
        help="Dataset sizes to benchmark (default: 1_month 3_months 1_year 2_years)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of iterations per benchmark (default: 5)",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file for JSON results",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick run with smaller datasets and fewer iterations",
    )

    args = parser.parse_args()

    # Parse dataset sizes
    if args.quick:
        dataset_sizes = [
            DATASET_SIZES["1_day"],
            DATASET_SIZES["1_week"],
            DATASET_SIZES["1_month"],
        ]
        iterations = 3
    else:
        dataset_sizes = [get_dataset_size(s) for s in args.sizes]
        iterations = args.iterations

    print("\n" + "=" * 80)
    print(" PyMarketStore Client Benchmarks")
    print("=" * 80)
    print(f" JsonRPC endpoint: {args.jsonrpc_endpoint}")
    print(f" gRPC endpoint:    {args.grpc_endpoint}")
    print(f" Dataset sizes:    {[format_size(s) for s in dataset_sizes]}")
    print(f" Iterations:       {iterations}")
    print("=" * 80)

    # Create benchmark runner
    benchmark = ClientBenchmark(
        jsonrpc_endpoint=args.jsonrpc_endpoint,
        grpc_endpoint=args.grpc_endpoint,
    )

    # Run benchmarks
    all_results = benchmark.run_all_benchmarks(
        dataset_sizes=dataset_sizes,
        iterations=iterations,
    )

    if all_results:
        # Print summary
        print_summary(all_results)

        # Export if requested
        if args.output:
            export_results(all_results, args.output)


if __name__ == "__main__":
    main()
