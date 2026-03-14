"""
Utility functions for benchmarking pymarketstore clients.
"""

import gc
import statistics
import time
import tracemalloc

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, List, Optional

import numpy as np
import pandas as pd


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""

    name: str
    dataset_size: int
    iterations: int
    times: List[float] = field(default_factory=list)
    memory_peak_mb: Optional[float] = None

    @property
    def mean_time(self) -> float:
        """Mean execution time in seconds."""
        return statistics.mean(self.times) if self.times else 0.0

    @property
    def median_time(self) -> float:
        """Median execution time in seconds."""
        return statistics.median(self.times) if self.times else 0.0

    @property
    def std_time(self) -> float:
        """Standard deviation of execution time."""
        return statistics.stdev(self.times) if len(self.times) > 1 else 0.0

    @property
    def min_time(self) -> float:
        """Minimum execution time in seconds."""
        return min(self.times) if self.times else 0.0

    @property
    def max_time(self) -> float:
        """Maximum execution time in seconds."""
        return max(self.times) if self.times else 0.0

    @property
    def throughput(self) -> float:
        """Records per second (based on mean time)."""
        if self.mean_time > 0:
            return self.dataset_size / self.mean_time
        return 0.0

    def to_dict(self) -> dict:
        """Convert to dictionary for reporting."""
        return {
            "name": self.name,
            "dataset_size": self.dataset_size,
            "iterations": self.iterations,
            "mean_time_s": round(self.mean_time, 4),
            "median_time_s": round(self.median_time, 4),
            "std_time_s": round(self.std_time, 4),
            "min_time_s": round(self.min_time, 4),
            "max_time_s": round(self.max_time, 4),
            "throughput_records_per_s": round(self.throughput, 2),
            "memory_peak_mb": round(self.memory_peak_mb, 2)
            if self.memory_peak_mb
            else None,
        }


@contextmanager
def memory_tracker():
    """Context manager to track peak memory usage."""
    gc.collect()
    tracemalloc.start()
    try:
        yield
    finally:
        pass  # We'll read the stats after


def get_memory_peak_mb() -> float:
    """Get peak memory usage in MB and stop tracking."""
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak / (1024 * 1024)


def generate_ohlcv_data(
    num_records: int,
    start_timestamp: int = 1420070400,  # 2015-01-01 00:00:00 UTC
    interval_seconds: int = 60,  # 1 minute intervals
    symbol: str = "TEST",
) -> np.recarray:
    """
    Generate synthetic OHLCV data for benchmarking.

    Args:
        num_records: Number of records to generate
        start_timestamp: Starting Unix timestamp
        interval_seconds: Time interval between records
        symbol: Symbol name (for reference)

    Returns:
        NumPy recarray with OHLCV data
    """
    # Generate timestamps
    timestamps = np.arange(
        start_timestamp,
        start_timestamp + (num_records * interval_seconds),
        interval_seconds,
        dtype="i8",
    )[:num_records]

    # Generate realistic OHLCV data
    # Start with a base price and random walk
    np.random.seed(42)  # For reproducibility
    base_price = 100.0
    returns = np.random.normal(0, 0.001, num_records)  # Small random returns
    close_prices = base_price * np.cumprod(1 + returns)

    # Generate OHLC from close prices with some noise
    high_prices = close_prices * (1 + np.abs(np.random.normal(0, 0.002, num_records)))
    low_prices = close_prices * (1 - np.abs(np.random.normal(0, 0.002, num_records)))
    open_prices = np.roll(close_prices, 1)
    open_prices[0] = base_price

    # Ensure OHLC relationships are valid
    high_prices = np.maximum(high_prices, np.maximum(open_prices, close_prices))
    low_prices = np.minimum(low_prices, np.minimum(open_prices, close_prices))

    # Generate volume
    volumes = np.random.randint(1000, 100000, num_records).astype("i8")

    # Create structured array
    dtype = [
        ("Epoch", "i8"),
        ("Open", "f8"),
        ("High", "f8"),
        ("Low", "f8"),
        ("Close", "f8"),
        ("Volume", "i8"),
    ]

    data = np.zeros(num_records, dtype=dtype)
    data["Epoch"] = timestamps
    data["Open"] = open_prices
    data["High"] = high_prices
    data["Low"] = low_prices
    data["Close"] = close_prices
    data["Volume"] = volumes

    return data.view(np.recarray)


def generate_ohlcv_dataframe(
    num_records: int,
    start_timestamp: int = 1420070400,
    interval_seconds: int = 60,
) -> pd.DataFrame:
    """
    Generate synthetic OHLCV data as a pandas DataFrame.

    Args:
        num_records: Number of records to generate
        start_timestamp: Starting Unix timestamp
        interval_seconds: Time interval between records

    Returns:
        DataFrame with OHLCV data and datetime index
    """
    data = generate_ohlcv_data(num_records, start_timestamp, interval_seconds)
    df = pd.DataFrame(data)
    df.index = pd.to_datetime(df["Epoch"], unit="s", utc=True)
    df = df.drop(columns=["Epoch"])
    return df


# Dataset size presets (number of 1-minute records)
DATASET_SIZES = {
    "1_hour": 60,
    "1_day": 60 * 24,  # 1,440
    "1_week": 60 * 24 * 7,  # 10,080
    "1_month": 60 * 24 * 30,  # 43,200
    "3_months": 60 * 24 * 90,  # 129,600
    "6_months": 60 * 24 * 180,  # 259,200
    "1_year": 60 * 24 * 365,  # 525,600
    "2_years": 60 * 24 * 365 * 2,  # 1,051,200
    "3_years": 60 * 24 * 365 * 3,  # 1,576,800
    "5_years": 60 * 24 * 365 * 5,  # 2,628,000
}


def get_dataset_size(name: str) -> int:
    """Get number of records for a named dataset size."""
    if name in DATASET_SIZES:
        return DATASET_SIZES[name]
    # Try to parse as integer
    try:
        return int(name)
    except ValueError:
        raise ValueError(
            f"Unknown dataset size: {name}. "
            f"Available: {list(DATASET_SIZES.keys())} or an integer"
        )


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f} us"
    elif seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    elif seconds < 60:
        return f"{seconds:.2f} s"
    else:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"


def format_size(num_records: int) -> str:
    """Format number of records in human-readable format."""
    if num_records >= 1_000_000:
        return f"{num_records / 1_000_000:.2f}M"
    elif num_records >= 1_000:
        return f"{num_records / 1_000:.1f}K"
    return str(num_records)


def run_benchmark(
    func: Callable,
    name: str,
    dataset_size: int,
    iterations: int = 5,
    warmup: int = 1,
    track_memory: bool = True,
) -> BenchmarkResult:
    """
    Run a benchmark function multiple times and collect statistics.

    Args:
        func: Function to benchmark (should take no arguments)
        name: Name of the benchmark
        dataset_size: Size of dataset being processed
        iterations: Number of timed iterations
        warmup: Number of warmup iterations (not timed)
        track_memory: Whether to track memory usage

    Returns:
        BenchmarkResult with timing statistics
    """
    result = BenchmarkResult(name=name, dataset_size=dataset_size, iterations=iterations)

    # Warmup
    for _ in range(warmup):
        func()
        gc.collect()

    # Memory tracking (on first timed iteration)
    if track_memory:
        gc.collect()
        tracemalloc.start()
        func()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        result.memory_peak_mb = peak / (1024 * 1024)
        gc.collect()
        # This was the first iteration, record its time too
        # We'll re-run to get proper timing

    # Timed iterations
    for _ in range(iterations):
        gc.collect()
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        result.times.append(end - start)

    return result


def print_results_table(results: List[BenchmarkResult], title: str = "Benchmark Results"):
    """Print benchmark results in a formatted table."""
    print(f"\n{'=' * 80}")
    print(f" {title}")
    print(f"{'=' * 80}")

    # Header
    headers = [
        "Name",
        "Size",
        "Mean",
        "Median",
        "Std Dev",
        "Throughput",
        "Memory",
    ]
    header_fmt = "{:<30} {:>10} {:>12} {:>12} {:>12} {:>15} {:>10}"
    print(header_fmt.format(*headers))
    print("-" * 80)

    # Rows
    for r in results:
        row = [
            r.name[:30],
            format_size(r.dataset_size),
            format_duration(r.mean_time),
            format_duration(r.median_time),
            format_duration(r.std_time),
            f"{format_size(int(r.throughput))}/s",
            f"{r.memory_peak_mb:.1f} MB" if r.memory_peak_mb else "N/A",
        ]
        print(header_fmt.format(*row))

    print(f"{'=' * 80}\n")


def compare_results(
    results_a: List[BenchmarkResult],
    results_b: List[BenchmarkResult],
    name_a: str = "Client A",
    name_b: str = "Client B",
):
    """Print a comparison table between two sets of results."""
    print(f"\n{'=' * 90}")
    print(f" Comparison: {name_a} vs {name_b}")
    print(f"{'=' * 90}")

    headers = ["Dataset Size", f"{name_a} Mean", f"{name_b} Mean", "Speedup", "Winner"]
    header_fmt = "{:<15} {:>18} {:>18} {:>12} {:>15}"
    print(header_fmt.format(*headers))
    print("-" * 90)

    # Match results by dataset size
    results_b_by_size = {r.dataset_size: r for r in results_b}

    for ra in results_a:
        rb = results_b_by_size.get(ra.dataset_size)
        if rb:
            speedup = ra.mean_time / rb.mean_time if rb.mean_time > 0 else float("inf")
            if speedup > 1:
                winner = name_b
                speedup_str = f"{speedup:.2f}x faster"
            elif speedup < 1:
                winner = name_a
                speedup_str = f"{1 / speedup:.2f}x faster"
            else:
                winner = "Tie"
                speedup_str = "1.00x"

            row = [
                format_size(ra.dataset_size),
                format_duration(ra.mean_time),
                format_duration(rb.mean_time),
                speedup_str,
                winner,
            ]
            print(header_fmt.format(*row))

    print(f"{'=' * 90}\n")
