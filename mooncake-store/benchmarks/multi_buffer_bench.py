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


def set_ascend_device(device_id):
    """Set Ascend NPU device ID."""
    if device_id is None:
        return False
    
    try:
        # Try to set device via CANN Python API if available
        try:
            import acl
            has_acl = True
        except ImportError:
            try:
                from ascend import acl
                has_acl = True
            except ImportError:
                has_acl = False
        
        if has_acl:
            try:
                if hasattr(acl, 'rt') and hasattr(acl.rt, 'set_device'):
                    ret = acl.rt.set_device(device_id)
                    if ret == 0:
                        logger.info(f"Set Ascend device to {device_id} via CANN API")
                        return True
                    else:
                        logger.warning(f"Failed to set device via CANN API, ret={ret}, "
                                     f"will use environment variable instead")
            except (AttributeError, TypeError) as e:
                logger.warning(f"CANN API set_device not available: {e}, "
                             f"will use environment variable instead")
    except Exception as e:
        logger.warning(f"Error setting device via CANN API: {e}, "
                      f"will use environment variable instead")
    
    # Fall back to environment variable
    os.environ["ASCEND_RT_VISIBLE_DEVICES"] = str(device_id)
    logger.info(f"Set ASCEND_RT_VISIBLE_DEVICES={device_id}")
    return True


def get_ascend_device_info():
    """Get current Ascend NPU device information."""
    device_info = {}
    
    # Check environment variables
    rt_visible_devices = os.getenv("ASCEND_RT_VISIBLE_DEVICES")
    if rt_visible_devices:
        device_info["ASCEND_RT_VISIBLE_DEVICES"] = rt_visible_devices
        # Parse device list
        device_list = [d.strip() for d in rt_visible_devices.split(',')]
        device_info["visible_devices"] = device_list
    
    # Try to get current device via CANN Python API if available
    try:
        # Try different CANN Python API import paths
        try:
            import acl
            has_acl = True
        except ImportError:
            try:
                from ascend import acl
                has_acl = True
            except ImportError:
                has_acl = False
        
        if has_acl:
            # Try to get device ID using CANN API
            try:
                if hasattr(acl, 'rt') and hasattr(acl.rt, 'get_device'):
                    device_id = ctypes.c_int()
                    ret = acl.rt.get_device(ctypes.byref(device_id))
                    if ret == 0:
                        device_info["current_device_id"] = device_id.value
                        device_info["device_info_source"] = "CANN API"
                    else:
                        device_info["device_info_source"] = "Environment variables only"
                else:
                    device_info["device_info_source"] = "Environment variables only"
            except (AttributeError, TypeError):
                device_info["device_info_source"] = "Environment variables only"
        else:
            device_info["device_info_source"] = "Environment variables only"
    except Exception:
        # Any error, fall back to environment variables
        device_info["device_info_source"] = "Environment variables only"
    
    # Try to infer from ASCEND_RT_VISIBLE_DEVICES if no API info available
    if device_info.get("current_device_id") is None and rt_visible_devices:
        # If only one device is visible, that's likely the current one
        device_list = [d.strip() for d in rt_visible_devices.split(',')]
        if len(device_list) == 1:
            try:
                device_info["inferred_device_id"] = int(device_list[0])
            except ValueError:
                pass
    
    return device_info


def print_device_info(protocol):
    """Print device information for the given protocol."""
    if protocol == "ascend":
        logger.info("=" * 60)
        logger.info("Ascend NPU Device Information:")
        logger.info("=" * 60)
        device_info = get_ascend_device_info()
        
        if device_info.get("current_device_id") is not None:
            logger.info(f"  Current Device ID (逻辑ID): {device_info['current_device_id']}")
        
        if device_info.get("inferred_device_id") is not None:
            logger.info(f"  Inferred Device ID (逻辑ID): {device_info['inferred_device_id']}")
            logger.info("    (Note: This is inferred from ASCEND_RT_VISIBLE_DEVICES)")
        
        if device_info.get("ASCEND_RT_VISIBLE_DEVICES"):
            logger.info(f"  ASCEND_RT_VISIBLE_DEVICES: {device_info['ASCEND_RT_VISIBLE_DEVICES']}")
            if device_info.get("visible_devices"):
                logger.info(f"  Visible Devices: {', '.join(device_info['visible_devices'])}")
        
        logger.info(f"  Info Source: {device_info.get('device_info_source', 'Unknown')}")
        logger.info("")
        logger.info("  Note: Device ID will be automatically detected by ADXL engine.")
        logger.info("        Check C++ logs for 'deviceLogicId' to see the actual device used.")
        logger.info("=" * 60)
        logger.info("")
    elif protocol == "rdma":
        logger.info("=" * 60)
        logger.info("RDMA Device Information:")
        logger.info("=" * 60)
        rdma_devices = os.getenv("RDMA_DEVICES", "")
        if rdma_devices:
            logger.info(f"  RDMA_DEVICES: {rdma_devices}")
        logger.info("=" * 60)
        logger.info("")


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
        self._ascendcl = None
        self._acl_host_ptrs = []

    def _load_ascendcl(self):
        if self._ascendcl is not None:
            return self._ascendcl
        for lib_name in ("libascendcl.so", "libascendcl.so.1", "libascendcl.so.0"):
            try:
                self._ascendcl = ctypes.CDLL(lib_name)
                break
            except OSError:
                continue
        if self._ascendcl is None:
            return None
        self._ascendcl.aclrtMallocHost.argtypes = [
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_size_t,
        ]
        self._ascendcl.aclrtMallocHost.restype = ctypes.c_int
        self._ascendcl.aclrtFreeHost.argtypes = [ctypes.c_void_p]
        self._ascendcl.aclrtFreeHost.restype = ctypes.c_int
        return self._ascendcl

    def _alloc_ascend_host_buffer(self, size):
        ascendcl = self._load_ascendcl()
        if ascendcl is None:
            return None, None
        ptr = ctypes.c_void_p()
        ret = ascendcl.aclrtMallocHost(ctypes.byref(ptr), ctypes.c_size_t(size))
        if ret != 0 or not ptr.value:
            logger.error("aclrtMallocHost failed, ret=%s", ret)
            return None, None
        buf_type = ctypes.c_uint8 * size
        buf = buf_type.from_address(ptr.value)
        arr = np.ctypeslib.as_array(buf)
        self._acl_host_ptrs.append(ptr)
        return arr, ptr.value

    def _free_acl_host_buffers(self):
        if not self._acl_host_ptrs:
            return
        ascendcl = self._load_ascendcl()
        if ascendcl is None:
            logger.warning("ascendcl not available; skip aclrtFreeHost cleanup")
            return
        for ptr in self._acl_host_ptrs:
            ret = ascendcl.aclrtFreeHost(ptr)
            if ret != 0:
                logger.warning("aclrtFreeHost failed for %s, ret=%s", ptr, ret)
        self._acl_host_ptrs.clear()

    def _allocate_buffer(self, size, fill_pattern=None):
        if self.args.protocol == "ascend":
            buffer, buffer_ptr = self._alloc_ascend_host_buffer(size)
            if buffer is not None:
                if fill_pattern is not None:
                    buffer.fill(fill_pattern)
                return buffer, buffer_ptr
            logger.warning(
                "Falling back to numpy buffer; Ascend register_buffer may fail "
                "if aclrtMallocHost is unavailable."
            )

        buffer = np.zeros(size, dtype=np.uint8)
        if fill_pattern is not None:
            buffer.fill(fill_pattern)
        return buffer, int(buffer.ctypes.data)

    def setup_store(self):
        """Initialize Mooncake Store."""
        logger.info("Initializing Mooncake Store...")
        logger.info(f"  Metadata Server: {self.args.metadata_server}")
        logger.info(f"  Local Hostname:  {self.args.local_hostname}")
        logger.info(f"  Protocol:        {self.args.protocol}")
        logger.info(f"  Master Server:   {self.args.master_server_addr}")
        
        # Set Ascend device if specified
        if self.args.protocol == 'ascend' and self.args.ascend_device_id is not None:
            logger.info(f"  Setting Ascend Device ID: {self.args.ascend_device_id}")
            set_ascend_device(self.args.ascend_device_id)
        
        # Print device information
        print_device_info(self.args.protocol)

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
            # Fill with pattern data (different for each key)
            pattern = np.uint8(i % 256)
            buffer, buffer_ptr = self._allocate_buffer(value_size, pattern)
            
            self.put_buffers.append(buffer)
            self.put_buffer_ptrs.append([buffer_ptr])
            self.put_sizes.append([value_size])

        # Allocate GET buffers (destination)
        self.get_buffers = []
        self.get_buffer_ptrs = []
        self.get_sizes = []
        
        for i in range(num_keys):
            buffer, buffer_ptr = self._allocate_buffer(value_size)
            self.get_buffers.append(buffer)
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
            self._free_acl_host_buffers()
            return

        # Check if buffers were initialized
        if self.put_buffer_ptrs is None and self.get_buffer_ptrs is None:
            self._free_acl_host_buffers()
            return

        logger.info("Unregistering buffers...")
        all_ptrs = []
        
        if self.put_buffer_ptrs is not None:
            all_ptrs.extend([ptrs[0] for ptrs in self.put_buffer_ptrs])
        
        if self.get_buffer_ptrs is not None:
            all_ptrs.extend([ptrs[0] for ptrs in self.get_buffer_ptrs])
        
        for ptr in all_ptrs:
            try:
                self.store.unregister_buffer(ptr)
            except Exception as e:
                logger.warning(f"Failed to unregister buffer at {ptr}: {e}")

        logger.info("Buffers unregistered")
        self._free_acl_host_buffers()

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
  
  # Benchmark with specific Ascend device ID
  python multi_buffer_bench.py --metadata-server 127.0.0.1:50051 \\
      --local-hostname 127.0.0.1:50052 --protocol ascend --ascend-device-id 0 \\
      --operation both
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
    parser.add_argument('--ascend-device-id', type=int, default=None,
                       help='Ascend NPU device ID (logical ID) to use (for ascend protocol)')
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
