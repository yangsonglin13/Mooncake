#!/usr/bin/env python3
"""
Benchmark script for batch_get_into_multi_buffers and batch_put_from_multi_buffers.

This script measures the performance of multi-buffer batch operations in Mooncake Store.
It supports both PUT and GET operations with configurable parameters.

Usage:
    python multi_buffer_bench.py --metadata-server <addr> --local-hostname <hostname> \\
        --protocol <tcp|rdma|ascend> --batch-size <N> --value-size <bytes> \\
        --rounds <N> --operation <put|get|both>
"""

import argparse
import time
import statistics
import logging
import sys
import os
import ctypes
from typing import List, Tuple
from dataclasses import dataclass

try:
    import numpy as np
except ImportError:
    print("Error: numpy is required. Install with: pip install numpy")
    sys.exit(1)

try:
    from mooncake.store import MooncakeDistributedStore, ReplicateConfig
except ImportError:
    print("Error: mooncake.store module not found. Make sure Mooncake is installed.")
    sys.exit(1)

# Disable memcpy optimization to force network transfer
os.environ["MC_STORE_MEMCPY"] = "0"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('multi_buffer_bench')


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""
    operation: str
    total_ops: int
    successful_ops: int
    failed_ops: int
    total_bytes: int
    total_time_sec: float
    qps: float
    bandwidth_gbps: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latency_avg_ms: float

    def print_summary(self):
        """Print a formatted summary of the benchmark results."""
        print("\n" + "="*80)
        print(f"Benchmark Results: {self.operation.upper()}")
        print("="*80)
        print(f"Total Operations:      {self.total_ops}")
        print(f"Successful Operations:  {self.successful_ops}")
        print(f"Failed Operations:      {self.failed_ops}")
        print(f"Success Rate:           {100.0 * self.successful_ops / self.total_ops:.2f}%")
        print(f"\nTotal Data Transferred: {self.total_bytes / (1024**3):.2f} GB")
        print(f"Total Time:             {self.total_time_sec:.3f} seconds")
        print(f"\nThroughput:")
        print(f"  QPS:                  {self.qps:.2f} ops/s")
        print(f"  Bandwidth:             {self.bandwidth_gbps:.2f} GB/s")
        print(f"\nLatency (per batch):")
        print(f"  Average:               {self.latency_avg_ms:.2f} ms")
        print(f"  P50:                   {self.latency_p50_ms:.2f} ms")
        print(f"  P95:                   {self.latency_p95_ms:.2f} ms")
        print(f"  P99:                   {self.latency_p99_ms:.2f} ms")
        print("="*80 + "\n")


class MultiBufferBenchmark:
    """Benchmark runner for multi-buffer batch operations."""

    def __init__(self, args):
        self.args = args
        self.store = None
        self.put_buffers = None
        self.put_buffer_ptrs = None
        self.put_sizes = None
        self.get_buffers = None
        self.get_buffer_ptrs = None
        self.get_sizes = None
        self.keys = None

    def setup_store(self):
        """Initialize Mooncake Store."""
        logger.info("Initializing Mooncake Store...")
        logger.info(f"  Metadata Server: {self.args.metadata_server}")
        logger.info(f"  Local Hostname:  {self.args.local_hostname}")
        logger.info(f"  Protocol:        {self.args.protocol}")
        logger.info(f"  Master Server:   {self.args.master_server_addr}")

        self.store = MooncakeDistributedStore()
        
        retcode = self.store.setup(
            local_hostname=self.args.local_hostname,
            metadata_server=self.args.metadata_server,
            global_segment_size=self.args.global_segment_size,
            local_buffer_size=self.args.local_buffer_size,
            protocol=self.args.protocol,
            rdma_devices=self.args.rdma_devices if self.args.rdma_devices else "",
            master_server_addr=self.args.master_server_addr
        )

        if retcode != 0:
            logger.error(f"Store setup failed with return code {retcode}")
            sys.exit(1)

        logger.info("Store initialized successfully")

    def prepare_buffers(self):
        """Allocate and register buffers for PUT and GET operations."""
        logger.info("Preparing buffers...")
        logger.info(f"  Batch Size:  {self.args.batch_size}")
        logger.info(f"  Value Size:  {self.args.value_size / (1024**2):.2f} MB per key")

        num_keys = self.args.batch_size
        value_size = self.args.value_size

        # Generate keys
        self.keys = [f"bench_key_{i}" for i in range(num_keys)]

        # Allocate PUT buffers (source data)
        self.put_buffers = []
        self.put_buffer_ptrs = []
        self.put_sizes = []
        
        for i in range(num_keys):
            # Allocate buffer for this key
            buffer = np.zeros(value_size, dtype=np.uint8)
            # Fill with pattern data (different for each key)
            pattern = (i % 256).astype(np.uint8)
            buffer.fill(pattern)
            
            self.put_buffers.append(buffer)
            # Get buffer pointer as integer
            buffer_ptr = int(buffer.ctypes.data)
            self.put_buffer_ptrs.append([buffer_ptr])
            self.put_sizes.append([value_size])

        # Allocate GET buffers (destination)
        self.get_buffers = []
        self.get_buffer_ptrs = []
        self.get_sizes = []
        
        for i in range(num_keys):
            buffer = np.zeros(value_size, dtype=np.uint8)
            self.get_buffers.append(buffer)
            buffer_ptr = int(buffer.ctypes.data)
            self.get_buffer_ptrs.append([buffer_ptr])
            self.get_sizes.append([value_size])

        # Register all buffers
        logger.info("Registering buffers...")
        all_ptrs = [ptrs[0] for ptrs in self.put_buffer_ptrs] + [ptrs[0] for ptrs in self.get_buffer_ptrs]
        
        for ptr in all_ptrs:
            retcode = self.store.register_buffer(ptr, value_size)
            if retcode != 0:
                logger.error(f"Failed to register buffer at {ptr}, return code: {retcode}")
                sys.exit(1)

        logger.info(f"Registered {len(all_ptrs)} buffers successfully")

    def cleanup_buffers(self):
        """Unregister buffers."""
        if self.store is None:
            return

        logger.info("Unregistering buffers...")
        all_ptrs = [ptrs[0] for ptrs in self.put_buffer_ptrs] + [ptrs[0] for ptrs in self.get_buffer_ptrs]
        
        for ptr in all_ptrs:
            self.store.unregister_buffer(ptr)

        logger.info("Buffers unregistered")

    def run_put_benchmark(self) -> BenchmarkResult:
        """Run PUT benchmark."""
        logger.info(f"\nStarting PUT benchmark: {self.args.rounds} rounds, "
                   f"{self.args.batch_size} keys per round")

        config = ReplicateConfig()
        config.prefer_alloc_in_same_node = self.args.prefer_same_node

        latencies = []
        total_ops = 0
        successful_ops = 0
        failed_ops = 0
        total_bytes = 0

        # Warmup
        if self.args.warmup_rounds > 0:
            logger.info(f"Warming up with {self.args.warmup_rounds} rounds...")
            for _ in range(self.args.warmup_rounds):
                self.store.batch_put_from_multi_buffers(
                    self.keys, self.put_buffer_ptrs, self.put_sizes, config)

        # Actual benchmark
        start_time = time.perf_counter()
        
        for round_num in range(self.args.rounds):
            round_start = time.perf_counter()
            
            return_codes = self.store.batch_put_from_multi_buffers(
                self.keys, self.put_buffer_ptrs, self.put_sizes, config)
            
            round_end = time.perf_counter()
            round_latency = (round_end - round_start) * 1000  # Convert to ms
            
            # Count successes (0 = success for PUT)
            round_success = sum(1 for code in return_codes if code == 0)
            round_failed = len(return_codes) - round_success
            
            successful_ops += round_success
            failed_ops += round_failed
            total_ops += len(return_codes)
            total_bytes += round_success * self.args.value_size
            latencies.append(round_latency)

            if (round_num + 1) % max(1, self.args.rounds // 10) == 0:
                logger.info(f"  Round {round_num + 1}/{self.args.rounds}: "
                           f"{round_success} succeeded, {round_failed} failed, "
                           f"latency: {round_latency:.2f} ms")

        end_time = time.perf_counter()
        total_time = end_time - start_time

        # Calculate statistics
        qps = successful_ops / total_time if total_time > 0 else 0
        bandwidth_gbps = (total_bytes / total_time) / (1024**3) if total_time > 0 else 0

        latencies_sorted = sorted(latencies)
        p50 = statistics.median(latencies_sorted) if latencies_sorted else 0
        p95 = latencies_sorted[int(len(latencies_sorted) * 0.95)] if latencies_sorted else 0
        p99 = latencies_sorted[int(len(latencies_sorted) * 0.99)] if latencies_sorted else 0
        avg_latency = statistics.mean(latencies) if latencies else 0

        return BenchmarkResult(
            operation="PUT",
            total_ops=total_ops,
            successful_ops=successful_ops,
            failed_ops=failed_ops,
            total_bytes=total_bytes,
            total_time_sec=total_time,
            qps=qps,
            bandwidth_gbps=bandwidth_gbps,
            latency_p50_ms=p50,
            latency_p95_ms=p95,
            latency_p99_ms=p99,
            latency_avg_ms=avg_latency
        )

    def run_get_benchmark(self) -> BenchmarkResult:
        """Run GET benchmark."""
        logger.info(f"\nStarting GET benchmark: {self.args.rounds} rounds, "
                   f"{self.args.batch_size} keys per round")

        prefer_same_node = self.args.prefer_same_node

        latencies = []
        total_ops = 0
        successful_ops = 0
        failed_ops = 0
        total_bytes = 0

        # Warmup
        if self.args.warmup_rounds > 0:
            logger.info(f"Warming up with {self.args.warmup_rounds} rounds...")
            for _ in range(self.args.warmup_rounds):
                self.store.batch_get_into_multi_buffers(
                    self.keys, self.get_buffer_ptrs, self.get_sizes, prefer_same_node)

        # Actual benchmark
        start_time = time.perf_counter()
        
        for round_num in range(self.args.rounds):
            round_start = time.perf_counter()
            
            return_codes = self.store.batch_get_into_multi_buffers(
                self.keys, self.get_buffer_ptrs, self.get_sizes, prefer_same_node)
            
            round_end = time.perf_counter()
            round_latency = (round_end - round_start) * 1000  # Convert to ms
            
            # Count successes (positive = success for GET, represents bytes read)
            round_success = sum(1 for code in return_codes if code > 0)
            round_failed = len(return_codes) - round_success
            
            successful_ops += round_success
            failed_ops += round_failed
            total_ops += len(return_codes)
            # For GET, return code is the number of bytes read
            total_bytes += sum(code for code in return_codes if code > 0)
            latencies.append(round_latency)

            if (round_num + 1) % max(1, self.args.rounds // 10) == 0:
                logger.info(f"  Round {round_num + 1}/{self.args.rounds}: "
                           f"{round_success} succeeded, {round_failed} failed, "
                           f"latency: {round_latency:.2f} ms")

        end_time = time.perf_counter()
        total_time = end_time - start_time

        # Calculate statistics
        qps = successful_ops / total_time if total_time > 0 else 0
        bandwidth_gbps = (total_bytes / total_time) / (1024**3) if total_time > 0 else 0

        latencies_sorted = sorted(latencies)
        p50 = statistics.median(latencies_sorted) if latencies_sorted else 0
        p95 = latencies_sorted[int(len(latencies_sorted) * 0.95)] if latencies_sorted else 0
        p99 = latencies_sorted[int(len(latencies_sorted) * 0.99)] if latencies_sorted else 0
        avg_latency = statistics.mean(latencies) if latencies else 0

        return BenchmarkResult(
            operation="GET",
            total_ops=total_ops,
            successful_ops=successful_ops,
            failed_ops=failed_ops,
            total_bytes=total_bytes,
            total_time_sec=total_time,
            qps=qps,
            bandwidth_gbps=bandwidth_gbps,
            latency_p50_ms=p50,
            latency_p95_ms=p95,
            latency_p99_ms=p99,
            latency_avg_ms=avg_latency
        )

    def run(self):
        """Run the benchmark."""
        try:
            self.setup_store()
            self.prepare_buffers()

            results = []

            if self.args.operation in ['put', 'both']:
                put_result = self.run_put_benchmark()
                results.append(put_result)
                put_result.print_summary()

            if self.args.operation in ['get', 'both']:
                # For GET, we need data to exist first
                if self.args.operation == 'get':
                    logger.info("Pre-populating data for GET benchmark...")
                    config = ReplicateConfig()
                    config.prefer_alloc_in_same_node = self.args.prefer_same_node
                    self.store.batch_put_from_multi_buffers(
                        self.keys, self.put_buffer_ptrs, self.put_sizes, config)
                    logger.info("Data pre-populated")

                get_result = self.run_get_benchmark()
                results.append(get_result)
                get_result.print_summary()

            return results

        finally:
            self.cleanup_buffers()


def main():
    parser = argparse.ArgumentParser(
        description='Benchmark batch_get_into_multi_buffers and batch_put_from_multi_buffers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Benchmark PUT operations with default settings
  python multi_buffer_bench.py --metadata-server 127.0.0.1:50051 \\
      --local-hostname 127.0.0.1:50052 --protocol tcp --operation put

  # Benchmark GET operations with custom batch size
  python multi_buffer_bench.py --metadata-server 127.0.0.1:50051 \\
      --local-hostname 127.0.0.1:50052 --protocol rdma --operation get \\
      --batch-size 64 --value-size 1048576

  # Benchmark both PUT and GET with Ascend protocol
  python multi_buffer_bench.py --metadata-server 127.0.0.1:50051 \\
      --local-hostname 127.0.0.1:50052 --protocol ascend --operation both \\
      --rounds 1000 --batch-size 128
        """
    )

    # Required arguments
    parser.add_argument('--metadata-server', type=str, required=True,
                       help='Metadata server address (e.g., 127.0.0.1:50051)')
    parser.add_argument('--local-hostname', type=str, required=True,
                       help='Local hostname and port (e.g., 127.0.0.1:50052)')

    # Optional arguments
    parser.add_argument('--protocol', type=str, default='tcp',
                       choices=['tcp', 'rdma', 'ascend'],
                       help='Transport protocol (default: tcp)')
    parser.add_argument('--rdma-devices', type=str, default='',
                       help='RDMA device names (comma-separated, required for RDMA)')
    parser.add_argument('--master-server-addr', type=str, default='127.0.0.1:50051',
                       help='Master server address (default: 127.0.0.1:50051)')
    parser.add_argument('--global-segment-size', type=int, default=16*1024*1024,
                       help='Global segment size in bytes (default: 16MB)')
    parser.add_argument('--local-buffer-size', type=int, default=16*1024*1024,
                       help='Local buffer size in bytes (default: 16MB)')

    # Benchmark parameters
    parser.add_argument('--operation', type=str, default='both',
                       choices=['put', 'get', 'both'],
                       help='Operation to benchmark (default: both)')
    parser.add_argument('--batch-size', type=int, default=32,
                       help='Number of keys per batch (default: 32)')
    parser.add_argument('--value-size', type=int, default=1024*1024,
                       help='Size of each value in bytes (default: 1MB)')
    parser.add_argument('--rounds', type=int, default=100,
                       help='Number of benchmark rounds (default: 100)')
    parser.add_argument('--warmup-rounds', type=int, default=10,
                       help='Number of warmup rounds (default: 10)')
    parser.add_argument('--prefer-same-node', action='store_true',
                       help='Prefer allocation in same node')

    args = parser.parse_args()

    # Validate arguments
    if args.protocol == 'rdma' and not args.rdma_devices:
        logger.warning("RDMA protocol specified but no devices provided. "
                      "This may fail if auto-discovery is disabled.")

    # Run benchmark
    benchmark = MultiBufferBenchmark(args)
    benchmark.run()


if __name__ == '__main__':
    main()
