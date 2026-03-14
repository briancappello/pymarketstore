"""
Benchmarks for pymarketstore clients.

This module provides benchmarking tools to compare the performance of
JsonRpcClient and GRPCClient implementations against a live MarketStore
server, with a focus on large datasets (multiple years of 1-minute OHLCV data).

Usage:
    # Run benchmarks from command line
    python -m benchmarks --quick

    # Or import and use programmatically
    from benchmarks import ClientBenchmark, DATASET_SIZES

    benchmark = ClientBenchmark()
    results = benchmark.run_all_benchmarks()
"""

from .benchmark_clients import (
    ClientBenchmark,
    export_results,
    main,
    print_summary,
)
from .utils import (
    DATASET_SIZES,
    BenchmarkResult,
    compare_results,
    format_duration,
    format_size,
    generate_ohlcv_data,
    generate_ohlcv_dataframe,
    get_dataset_size,
    print_results_table,
    run_benchmark,
)


__all__ = [
    "BenchmarkResult",
    "ClientBenchmark",
    "DATASET_SIZES",
    "compare_results",
    "export_results",
    "format_duration",
    "format_size",
    "generate_ohlcv_data",
    "generate_ohlcv_dataframe",
    "get_dataset_size",
    "main",
    "print_results_table",
    "print_summary",
    "run_benchmark",
]
