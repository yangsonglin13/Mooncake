#!/usr/bin/python3.11
# -*- coding: utf-8 -*-
# 版权所有 (c) 华为技术有限公司 2026-2030
import argparse
import logging
import socket
import subprocess
import multiprocessing as mp
import os
import threading
import time
import csv
import asyncio
import concurrent.futures
import numpy as np

try:
    from yr.datasystem.kv_client import KVClient, SetParam, WriteMode
    from yr.datasystem.hetero_client import HeteroClient, Blob, DeviceBlobList
except ImportError:
    logging.warning("DataSystem module not found!")

try:
    from mooncake.store import MooncakeDistributedStore, ReplicateConfig
except ImportError:
    logging.warning("Mooncake module not found!")

try:
    import acl

    ACL_AVAILABLE = True
except ImportError:
    ACL_AVAILABLE = False
    pass

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# 环境变量配置
os.environ["MC_LOG_DIR"] = "./mc_log"
os.environ["MC_TE_METRIC"] = "1"
os.environ["MC_TE_METRIC_INTERVAL_SECONDS"] = "1"
os.environ["MC_STORE_CLIENT_METRIC"] = "1"
os.environ["MC_STORE_CLIENT_METRIC_INTERVAL"] = "1"
os.environ["DATASYSTEM_CLIENT_LOG_DIR"] = "./ds_log"
os.environ["DS_H2D_MEMCPY_POLICY"] = "huge_ffts"
os.environ["DS_D2H_MEMCPY_POLICY"] = "huge_ffts"
os.environ["DS_DEVICE_ACL_SIZE"] = "419430400"
os.environ["DS_HOST_ACL_SIZE"] = "10737418240"


# ==================== 1. 配置与命令行 ====================
def parse_args():
    """parse_args"""
    parser = argparse.ArgumentParser(description='Asyncio Benchmark for Mooncake/DataSystem')
    parser.add_argument('--role', type=str, choices=['put', 'get', 'local'], default='local',
                        help='local: 本地读写; put/get: 跨节点分布式测试')
    parser.add_argument('--engine', type=str, choices=['Mooncake', 'DataSystem'], default='Mooncake')
    parser.add_argument('--type', type=str, default="tcp", choices=['hero', 'tcp', 'rdma'],
                        help='hero=Device(NPU), tcp/rdma=Host(CPU)')
    parser.add_argument('--local_ip', type=str, default="127.0.0.1", help='本机 IP')
    parser.add_argument('--master_server', type=str, default="127.0.0.1:50054",
                        help='Master IP:Port (节点2必须指定为节点1的IP)')
    parser.add_argument('--ds_port', type=int, default=9094)
    parser.add_argument('--output', type=str, default="perf_results.csv")
    parser.add_argument('--run_id', type=str, default="bench_v3", help='跨节点逻辑组ID')
    parser.add_argument('--warmup', type=int, default=5)
    args, _ = parser.parse_known_args()
    return args


ARGS = parse_args()
LOG_TIMESTAMP = int(time.time())

# 提取 Master IP 用于 Python 层面的同步 (端口固定 9900)
MASTER_IP = ARGS.master_server.split(":")[0]
SYNC_PORT = 9900

# tcp网卡
DETECTED_NIC = "enp161s0f0np0"
# RDMA网卡
RDMA_NIC = "mlx5_0"

if ARGS.type in ['tcp', 'rdma']:
    config_map = {
        '1proc_1thread_1key_1MB': {
            'key_num': 1, 'blob_num': 1, 'blob_size': 1024 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        '1proc_1thread_1key_4MB': {
            'key_num': 1, 'blob_num': 1, 'blob_size': 4 * 1024 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        '1proc_1thread_1key_8MB': {
            'key_num': 1, 'blob_num': 1, 'blob_size': 8 * 1024 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        '1proc_1thread_4key_4MB': {
            'key_num': 4, 'blob_num': 1, 'blob_size': 1024 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        '1proc_1thread_16key_16MB': {
            'key_num': 16, 'blob_num': 1, 'blob_size': 1024 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        '1proc_1thread_32key_32MB': {
            'key_num': 32, 'blob_num': 1, 'blob_size': 1024 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        '4proc_1thread_16key_16MB': {
            'key_num': 16, 'blob_num': 1, 'blob_size': 1024 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},
    }
else:
    config_map = {
        '1process_1thread_72KB': {
            'key_num': 32, 'blob_num': 61, 'blob_size': 72 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        # '1process_1thread_144KB': {
        #     'key_num': 32, 'blob_num': 61, 'blob_size': 144 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        # '1process_1thread_1024KB': {
        #     'key_num': 32, 'blob_num': 61, 'blob_size': 1024 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},

        # '1process_1thread_8key_64KB': {
        #     'key_num': 8, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        # '1process_1thread_16key_64KB': {
        #     'key_num': 16, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        # '1process_1thread_32key_64KB': {
        #     'key_num': 32, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        # '1process_1thread_64key_64KB': {
        #     'key_num': 64, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},
        # '1process_1thread_128key_64KB': {
        #     'key_num': 128, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 1},

        # '4process_1thread_72KB': {
        #     'key_num': 32, 'blob_num': 61, 'blob_size': 72 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},
        # '4process_1thread_144KB': {
        #     'key_num': 32, 'blob_num': 61, 'blob_size': 144 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},
        # '4process_1thread_1024KB': {
        #     'key_num': 32, 'blob_num': 61, 'blob_size': 1024 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},

        '4process_1thread_8key_64KB': {
            'key_num': 8, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},
        # '4process_1thread_16key_64KB': {
        #     'key_num': 16, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},
        '4process_1thread_32key_64KB': {
            'key_num': 32, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},
        # '4process_1thread_64key_64KB': {
        #     'key_num': 64, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},
        '4process_1thread_128key_64KB': {
            'key_num': 128, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},

        # '4process_1thread_256key_64KB': {
        #     'key_num': 256, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},
        # '4process_1thread_512key_64KB': {
        #     'key_num': 512, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4},
        # '4process_1thread_1024key_64KB': {
        #     'key_num': 1024, 'blob_num': 128, 'blob_size': 64 * 1024, 'iterations': 50, 'threads': 1, 'procs': 4}
    }

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', force=True)


def write_perf_csv(mode, op, engine, cfg_name, cfg, total_ops, total_bytes, duration, lats, filename):
    """write_perf_csv"""
    file_exists = os.path.isfile(filename)
    try:
        with open(filename, mode='a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    'Mode', 'Operation', 'Engine', 'Config', 'Keys_per_Batch', 'Blob_num', 'Threads',
                    'Total_MB', 'Throughput_MBs', 'Avg_ms', 'P99_ms', 'Duration_sec'
                ])

            if duration <= 0:
                duration = 0.0001
            total_mb = total_bytes / (1024 * 1024)
            throughput_mb = total_mb / duration
            avg_lat = np.mean(lats) if lats else 0
            p99_lat = np.percentile(lats, 99) if lats else 0

            writer.writerow([
                mode, op, engine, cfg_name, cfg['key_num'], cfg['blob_num'], cfg['threads'],
                f"{total_mb:.2f}", f"{throughput_mb:.2f}",
                f"{avg_lat:.3f}", f"{p99_lat:.3f}", f"{duration:.4f}"
            ])
    except Exception as e:
        logging.error(f"Error writing CSV: {e}")


class BaseEngine:
    def setup(self):
        """setup"""
        pass

    def prepare_data(self):
        """prepare_data"""
        pass

    def run_op_blocking(self, op_type, tid, batch_idx):
        """run_op_blocking"""
        pass

    def close(self):
        """close"""
        pass


class MooncakeEngine(BaseEngine):
    def __init__(self, proc_idx, cfg_name, cfg, device_id):
        self.proc_idx = proc_idx
        self.cfg_name = cfg_name
        self.cfg = cfg
        self.device_id = device_id
        self.instance = None
        self.run_id = f"mc_{ARGS.run_id}"
        self.all_iteration_keys = []
        self.host_payload = None
        self.ptrs = []
        self.put_config = None

    def setup(self):
        """setup"""

        self.instance = MooncakeDistributedStore()
        mem_size = 100 * 1024 ** 3

        protocol = "tcp"
        nic = DETECTED_NIC
        if ARGS.type == 'rdma':
            protocol = "rdma"
            nic = RDMA_NIC
        elif ARGS.type == 'hero':
            protocol = "ascend"
            nic = str(self.device_id)

        logging.info(f"[Mooncake p{self.proc_idx}] Init: {protocol} on {nic}, Mem={mem_size // 1024 ** 3}GB")
        ret = self.instance.setup(ARGS.local_ip, "P2PHANDSHAKE", mem_size, 4 * 1024 ** 3, protocol, nic,
                                  ARGS.master_server)
        if ret != 0:
            raise RuntimeError(f"Mooncake Setup Failed with code: {ret}")
        self.put_config = ReplicateConfig()
        self.put_config.replica_num = 1
        self.put_config.preferred_segment = self.instance.get_hostname()
        logging.info(f"[Mooncake p{self.proc_idx}] PUT placement pinned to local segment: "
                     f"{self.put_config.preferred_segment}")

    def prepare_data(self):
        """prepare_data"""
        num_threads = self.cfg['threads']
        self.all_iteration_keys = []
        target_proc_idx = self.proc_idx

        for i in range(self.cfg['iterations']):
            iter_keys = []
            for tid in range(num_threads):
                keys = [f"{self.run_id}_{self.cfg_name}_b{i}_p{target_proc_idx}_t{tid}_k{k}"
                        for k in range(self.cfg['key_num'])]
                iter_keys.append(keys)
            self.all_iteration_keys.append(iter_keys)

        if ARGS.type in ['tcp', 'rdma']:
            total_len = self.cfg['blob_num'] * self.cfg['blob_size']
            self.host_payload = b'x' * total_len

        elif ARGS.type == 'hero' and ACL_AVAILABLE:
            total_size = self.cfg['key_num'] * self.cfg['blob_num'] * self.cfg['blob_size']
            for _ in range(num_threads):
                p, ret = acl.rt.malloc(total_size, 0)
                if ret != 0:
                    raise RuntimeError("ACL Malloc Failed")
                self.instance.register_buffer(p, total_size)
                struct = [[p + (k * self.cfg['blob_num'] + b) * self.cfg['blob_size']
                           for b in range(self.cfg['blob_num'])] for k in range(self.cfg['key_num'])]
                sizes = [[self.cfg['blob_size']] * self.cfg['blob_num'] for _ in range(self.cfg['key_num'])]
                self.ptrs.append({"ptr": p, "struct": struct, "sizes": sizes})

    def run_op_blocking(self, op_type, tid, batch_idx):
        """run_op_blocking"""
        keys = self.all_iteration_keys[batch_idx % len(self.all_iteration_keys)][tid]

        if ARGS.type in ['tcp', 'rdma']:
            if op_type == "PUT":
                vals = [self.host_payload for _ in keys]
                if len(keys) == 1:
                    ret = self.instance.put(keys[0], vals[0], self.put_config)
                else:
                    ret = self.instance.put_batch(keys, vals, self.put_config)
                if ret != 0:
                    raise RuntimeError(f"PUT failed: {ret}")
            else:
                if len(keys) == 1:
                    val = self.instance.get(keys[0])
                    if not val:
                        return False
                else:
                    vals = self.instance.get_batch(keys)
                    if not all(vals):
                        return False
        else:
            item = self.ptrs[tid]
            if op_type == "PUT":
                ret_list = self.instance.batch_put_from_multi_buffers(
                    keys, item["struct"], item["sizes"], self.put_config)
                if any(r != 0 for r in ret_list):
                    raise RuntimeError("Device PUT failed")
            else:
                ret_list = self.instance.batch_get_into_multi_buffers(keys, item["struct"], item["sizes"])
                if any(r < 0 for r in ret_list):
                    return False
            acl.rt.synchronize_device()
        return True

    def close(self):
        """close"""
        # 1. 先销毁 Mooncake 实例，确保它去注册(Deregister)内存
        if self.instance:
            del self.instance
            self.instance = None

        # 2. 等待一小会儿，让底层驱动完成去注册操
        time.sleep(0.5)

        # 3. 再释放 NPU 显存
        try:
            if ARGS.type == 'hero' and ACL_AVAILABLE:
                for item in self.ptrs:
                    if item.get("ptr"):
                        acl.rt.free(item["ptr"])
                self.ptrs = []
        except Exception as e:
            logging.error(f"ACL free failed: {e}")


class DataSystemEngine(BaseEngine):
    def __init__(self, proc_idx, cfg_name, cfg, device_id):
        self.proc_idx = proc_idx
        self.cfg_name = cfg_name
        self.cfg = cfg
        self.device_id = device_id
        self.instance = None
        self.run_id = f"ds_{ARGS.run_id}"
        self.all_iteration_keys = []
        self.raw_ptrs = []
        self.ptrs = []
        self.host_data = []
        self.lock = threading.Lock()

    def setup(self):
        """setup"""
        try:
            if ARGS.type in ['tcp', 'rdma']:
                self.instance = KVClient(ARGS.local_ip, ARGS.ds_port, enable_exclusive_connection=True)
            else:
                self.instance = HeteroClient(ARGS.local_ip, ARGS.ds_port, enable_exclusive_connection=True,
                                             enable_remote_h2d=True)

            if hasattr(self.instance, 'init'):
                self.instance.init()
            logging.info(f"[DataSystem p{self.proc_idx}] Init success")
            time.sleep(2)
        except ImportError:
            logging.error("DataSystem module not found!")

    def prepare_data(self):
        """prepare_data"""
        num_threads = self.cfg['threads']
        self.all_iteration_keys = []
        target_proc_idx = self.proc_idx

        for i in range(self.cfg['iterations']):
            iter_keys = []
            for tid in range(num_threads):
                keys = [f"{self.run_id}_{self.cfg_name}_b{i}_p{target_proc_idx}_t{tid}_k{k}"
                        for k in range(self.cfg['key_num'])]
                iter_keys.append(keys)
            self.all_iteration_keys.append(iter_keys)

        if ARGS.type in ['tcp', 'rdma']:
            total_len = self.cfg['blob_num'] * self.cfg['blob_size']
            self.host_data = [b'x' * total_len for _ in range(self.cfg['key_num'])]
        else:
            if not ACL_AVAILABLE:
                return
            total_size = self.cfg['key_num'] * self.cfg['blob_num'] * self.cfg['blob_size']
            for _ in range(num_threads):
                base_ptr, ret = acl.rt.malloc(total_size, 0)
                if ret != 0:
                    raise RuntimeError("ACL Malloc Failed")
                self.raw_ptrs.append(base_ptr)
                p_list = []
                for k in range(self.cfg['key_num']):
                    blobs = []
                    for b in range(self.cfg['blob_num']):
                        offset = (k * self.cfg['blob_num'] + b) * self.cfg['blob_size']
                        blobs.append(Blob(base_ptr + offset, self.cfg['blob_size']))
                    p_list.append(DeviceBlobList(self.device_id, blobs))
                self.ptrs.append(p_list)

    def run_op_blocking(self, op_type, tid, batch_idx):
        """run_op_blocking"""
        keys = self.all_iteration_keys[batch_idx % len(self.all_iteration_keys)][tid]
        param = SetParam()
        param.write_mode = WriteMode.NONE_L2_CACHE_EVICT
        with self.lock:
            if ARGS.type in ['tcp', 'rdma']:
                if op_type == "PUT":
                    if len(keys) == 1:
                        self.instance.set(keys[0], self.host_data[0])
                    else:
                        self.instance.mset(keys, self.host_data)
                else:
                    ret = self.instance.get(keys)
                    if ret is None or len(ret) != len(keys):
                        return False
            else:
                if op_type == "PUT":
                    self.instance.mset_d2h(keys, self.ptrs[tid], param)
                else:
                    ret = self.instance.mget_h2d(keys, self.ptrs[tid], 60000)
                    if ret:
                        raise RuntimeError(f"DS mget failed: {ret}")
                acl.rt.synchronize_device()
        return True

    def close(self):
        """close"""
        # 1. 先销毁 Client 引用
        if self.instance:
            del self.instance
            self.instance = None

        # 2. 再释放 ACL 内存
        if ACL_AVAILABLE and self.raw_ptrs:
            for p in self.raw_ptrs:
                try:
                    acl.rt.free(p)
                except Exception as e:
                    logging.warning(f"ACL free failed: {e}")
            self.raw_ptrs = []


class RestartServer:
    @staticmethod
    def _restart_service(name, kill_pattern, cmd_args, check_port, host=None, timeout=100, custom_env=None):
        """_restart_service"""
        host = host or ARGS.local_ip
        logging.info(f"[{name}] starting process...")
        subprocess.run(f"pkill -9 -f '{kill_pattern}'", shell=True)
        time.sleep(1)
        cmd = [str(x) for x in cmd_args]
        env = os.environ.copy()
        if custom_env:
            env.update(custom_env)
        try:
            log_file = open(f"/tmp/{name}_startup.log", "w")
            # 传入 env 参数
            subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, env=env)
        except Exception as e:
            raise RuntimeError(f"[{name}] Execution failed") from e

        start_time = time.time()
        while time.time() - start_time < timeout:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                if s.connect_ex((host, int(check_port))) == 0:
                    logging.info(f"[{name}] Start SUCCESS. Port {check_port} is up.")
                    return
            time.sleep(0.5)
        raise RuntimeError(f"[{name}] Start FAILED. Port {check_port} timeout after {timeout}s. Please check /tmp"
                           f"/{name}_startup.log for reasons.")

    @staticmethod
    def restart_etcd():
        """restart_etcd"""
        port = 30102
        peer_port = port + 1
        subprocess.run("rm -rf /tmp/etcd-api", shell=True)
        os.environ["ETCD_UNSUPPORTED_ARCH"] = "arm64"
        cmd = [
            "etcd", "--name", "etcd-api",
            "--data-dir", "/tmp/etcd-api",
            "--listen-client-urls", f"http://{ARGS.local_ip}:{port}",
            "--advertise-client-urls", f"http://{ARGS.local_ip}:{port}",
            "--listen-peer-urls", f"http://{ARGS.local_ip}:{peer_port}",
            "--initial-advertise-peer-urls", f"http://{ARGS.local_ip}:{peer_port}",
            "--initial-cluster", f"etcd-api=http://{ARGS.local_ip}:{peer_port}",
            "--initial-cluster-state", "new"
        ]
        logging.info(f"[Etcd] Starting with args: {' '.join(cmd)}")
        RestartServer._restart_service("Etcd", "etcd-api", cmd, port, host=ARGS.local_ip, custom_env={
            "ETCD_UNSUPPORTED_ARCH": "arm64"})

    @staticmethod
    def restart_mooncake():
        """restart_mooncake"""
        port = 50055
        cmd = [
            "mooncake_master",
            "--rpc_port", str(port),
            "--rpc_address", ARGS.local_ip,
            "--enable_http_metadata_server", "true",
            "--http_metadata_server_host", ARGS.local_ip,
            "--http_metadata_server_port", "8088",
            "--metrics_port", "9008",
            "--rpc_thread_num", "8",
            "--eviction_ratio", "0.05",
            "--eviction_high_watermark_ratio", "0.8"
        ]
        # 注意：这里如果需要像原代码那样重定向日志到文件，可在 _restart_service 中修改 Popen 的 stdout 参数
        logging.info(f"[MooncakeMaster] Starting with args: {' '.join(cmd)}")
        RestartServer._restart_service("MooncakeMaster", "mooncake_master", cmd, port)

    @staticmethod
    def restart_datasystem():
        """restart_datasystem"""
        etcd_ip = ARGS.master_server if ARGS.master_server else ARGS.local_ip
        port = ARGS.ds_port
        cmd = [
            'dscli', 'start',
            '--worker_args',
            '--worker_address', f'{ARGS.local_ip}:{port}',
            '--etcd_address', f'{etcd_ip}:30102',
            '--shared_memory_size_mb', "300000",
            # '--remote_h2d_device_ids', '0,1,2,3',
            '--enable_huge_tlb', 'true',
            '--arena_per_tenant', '1',
            '--enable_fallocate', 'false',
            '--shared_memory_populate', 'true'
        ]
        # kill pattern 使用端口特征来精确定位
        logging.info(f"[DataSystemWorker] Starting with args: {' '.join(cmd)}")
        RestartServer._restart_service("DataSystemWorker", f"{ARGS.local_ip}:{port}", cmd, port)
        time.sleep(30)


# ==================== 4. Sync Mechanism (Optimized) ====================

class SyncServer:
    def __init__(self, host, port):
        self.addr = (host, port)
        self.kv = {}
        self.lock = threading.Lock()
        self.running = True

    def start(self):
        """start"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.bind(self.addr)
            self.sock.listen(512)
            logging.info(f"[SyncServer] Listening on {self.addr}")
            threading.Thread(target=self._serve, daemon=True).start()
        except Exception as e:
            logging.error(f"[SyncServer] Failed to bind: {e}")
            raise e

    def _serve(self):
        """_serve"""
        while self.running:
            try:
                conn, _ = self.sock.accept()
                threading.Thread(target=self._handle, args=(conn,), daemon=True).start()
            except Exception as e:
                if self.running:
                    logging.warning(f"[SyncServer] Accept warn: {e}")

    def _handle(self, conn):
        """_handle"""
        try:
            conn.settimeout(600)
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                msg = data.decode().strip()
                if not msg:
                    break

                parts = msg.split(" ", 1)
                cmd = parts[0]
                key = parts[1] if len(parts) > 1 else ""

                if cmd == "SET":
                    with self.lock:
                        if " " in key:
                            real_key, val = key.split(" ", 1)
                            self.kv[real_key] = val
                        else:
                            self.kv[key] = True
                    conn.send(b"OK")

                elif cmd == "GET_VAL":
                    found_val = None
                    start_w = time.time()
                    while time.time() - start_w < 600:
                        with self.lock:
                            if key in self.kv:
                                found_val = self.kv[key]
                                break
                        time.sleep(0.05)
                    if found_val:
                        conn.send(f"OK {found_val}".encode())
                    else:
                        conn.send(b"TIMEOUT")

                elif cmd == "WAIT":
                    found = False
                    with self.lock:
                        if key in self.kv:
                            found = True
                    conn.send(b"OK" if found else b"NO")
                else:
                    break
        except Exception as e:
            logging.warning(f"[SyncServer] Handle warn: {e}")
        finally:
            try:
                conn.close()
            except Exception as e:
                logging.warning(f"[SyncServer] Close warn: {e}")


class SyncClient:
    def __init__(self, host, port):
        self.addr = (host, port)
        self.sock = None
        self.connected = False

    def _ensure_conn(self):
        """_ensure_conn"""
        if self.sock:
            return
        try:
            self.sock = socket.create_connection(self.addr, timeout=305)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.connected = True
        except Exception as e:
            logging.error(f"[SyncClient] Connect failed: {e}")
            self.sock = None

    def _send_cmd(self, cmd_bytes):
        """_send_cmd"""
        for i in range(5):
            self._ensure_conn()
            if not self.sock:
                time.sleep(1)
                continue
            try:
                self.sock.send(cmd_bytes)
                resp = self.sock.recv(1024)
                return resp
            except Exception as e:
                logging.warning(f"[SyncClient] Send failed (retry {i}): {e}")
                if self.sock:
                    try:
                        self.sock.close()
                    except Exception as e2:
                        logging.info(f"[SyncClient] Close failed: {e2}")
                self.sock = None
                time.sleep(1)
        return None

    def set(self, key):
        """set"""
        self._send_cmd(f"SET {key}".encode())

    def set_val(self, key, val):
        """set_val"""
        self._send_cmd(f"SET {key} {val}".encode())

    def wait(self, key):
        """wait"""
        start_time = time.time()
        while True:
            resp = self._send_cmd(f"WAIT {key}".encode())
            if resp == b"OK":
                return
            time.sleep(0.05)
            if time.time() - start_time > 1200:
                raise TimeoutError(f"Wait {key} timeout")

    def get_val(self, key):
        """get_val"""
        resp = self._send_cmd(f"GET_VAL {key}".encode())
        if resp and resp.startswith(b"OK "):
            return resp.decode().split(" ", 1)[1]
        return None

    def barrier(self, key_prefix):
        """barrier"""
        if ARGS.role == 'local':
            return
        if ARGS.role == "put":
            self.set(f"{key_prefix}_PUT_READY")
            self.wait(f"{key_prefix}_GET_READY")
        elif ARGS.role == "get":
            self.set(f"{key_prefix}_GET_READY")
            self.wait(f"{key_prefix}_PUT_READY")


# ==================== 5. Asyncio Worker Logic ====================

def generate_sync_key(session_id, cfg_name, proc_idx, batch_idx, tag):
    """generate_sync_key"""
    return f"{session_id}_{cfg_name}_p{proc_idx}_b{batch_idx}_{tag}"


async def async_worker_task(executor, engine, op, tid, batch_idx):
    """async_worker_task"""
    loop = asyncio.get_running_loop()
    s = time.perf_counter()

    for attempt in range(10):
        try:
            res = await loop.run_in_executor(
                executor, engine.run_op_blocking, op, tid, batch_idx
            )
            if res:
                return (time.perf_counter() - s) * 1000
            if op == "GET":
                await asyncio.sleep(0.01)
        except Exception as e:
            logging.warning(f"[T{tid}] Batch {batch_idx} retry {attempt} error: {e}")
            await asyncio.sleep(0.05)
    return None


async def benchmark_entry(proc_idx, cfg_name, cfg, engine, result_queue, session_id):
    """benchmark_entry"""
    sync_cli = SyncClient(MASTER_IP, SYNC_PORT)
    executor = concurrent.futures.ThreadPoolExecutor(cfg['threads'])

    # 存储混合结果
    all_lats = []

    # 本地模式单独统计
    put_lats = []
    get_lats = []
    put_duration_acc = 0.0
    get_duration_acc = 0.0

    barrier_key = f"{session_id}_{cfg_name}_p{proc_idx}_START_BARRIER"
    try:
        sync_cli.barrier(barrier_key)
    except Exception as e:
        if ARGS.role != 'local':
            logging.error(f"[Proc {proc_idx}] Start barrier failed: {e}")
            return

    logging.info(f"[Proc {proc_idx}] Warming up...")
    try:
        if ARGS.role in ["put", "local"]:
            for _ in range(ARGS.warmup):
                engine.run_op_blocking("PUT", 0, 0)
    except Exception as e:
        logging.warning(f"[Proc {proc_idx}] Warmup error: {e}")

    logging.info(f"[Proc {proc_idx}] Start Benchmark Loop")
    total_pure_duration = 0.0

    try:
        for batch_idx in range(cfg['iterations']):
            put_done_key = generate_sync_key(session_id, cfg_name, proc_idx, batch_idx, "PUT_DONE")
            get_done_key = generate_sync_key(session_id, cfg_name, proc_idx, batch_idx, "GET_DONE")

            if ARGS.role == "put":
                batch_start = time.perf_counter()
                tasks = [async_worker_task(executor, engine, "PUT", tid, batch_idx)
                         for tid in range(cfg['threads'])]
                res = await asyncio.gather(*tasks)
                total_pure_duration += (time.perf_counter() - batch_start)
                all_lats.extend([r for r in res if r is not None])
                sync_cli.set(put_done_key)
                sync_cli.wait(get_done_key)

            elif ARGS.role == "get":
                sync_cli.wait(put_done_key)
                batch_start = time.perf_counter()
                tasks = [async_worker_task(executor, engine, "GET", tid, batch_idx)
                         for tid in range(cfg['threads'])]
                res = await asyncio.gather(*tasks)
                total_pure_duration += (time.perf_counter() - batch_start)
                all_lats.extend([r for r in res if r is not None])
                sync_cli.set(get_done_key)

            elif ARGS.role == "local":
                # PUT Phase
                t0 = time.perf_counter()
                tasks_put = [async_worker_task(executor, engine, "PUT", tid, batch_idx)
                             for tid in range(cfg['threads'])]
                res_put = await asyncio.gather(*tasks_put)
                t1 = time.perf_counter()

                # GET Phase
                tasks_get = [async_worker_task(executor, engine, "GET", tid, batch_idx)
                             for tid in range(cfg['threads'])]
                res_get = await asyncio.gather(*tasks_get)
                t2 = time.perf_counter()

                # Accumulate stats
                put_duration_acc += (t1 - t0)
                get_duration_acc += (t2 - t1)
                total_pure_duration += (t2 - t0)

                p_lat = [r for r in res_put if r is not None]
                g_lat = [r for r in res_get if r is not None]

                put_lats.extend(p_lat)
                get_lats.extend(g_lat)
                all_lats.extend(p_lat)
                all_lats.extend(g_lat)

    except Exception as e:
        logging.error(f"Benchmark Loop Error: {e}", exc_info=True)

    logging.info(f"[Proc {proc_idx}] Finished. Pure Duration: {total_pure_duration:.4f}s")

    result_queue.put({
        "status": "success",
        "lats": all_lats,
        "duration": total_pure_duration,
        # 本地模式专属数据
        "put_lats": put_lats,
        "get_lats": get_lats,
        "put_duration": put_duration_acc,
        "get_duration": get_duration_acc
    })


def worker_process(proc_idx, cfg_name, cfg, result_queue, session_id):
    """worker_process"""
    device_id = proc_idx % 8
    if ARGS.type == 'hero' and ACL_AVAILABLE:
        acl.init()
        acl.rt.set_device(device_id)
    engine_cls = MooncakeEngine if ARGS.engine == 'Mooncake' else DataSystemEngine
    engine = engine_cls(proc_idx, cfg_name, cfg, device_id)

    try:
        engine.setup()
        engine.prepare_data()
        asyncio.run(benchmark_entry(proc_idx, cfg_name, cfg, engine, result_queue, session_id))
    except Exception as e:
        logging.error(f"Worker {proc_idx} failed", exc_info=True)
        result_queue.put({"status": "failed", "error": str(e)})
    finally:
        time.sleep(2)
        try:
            engine.close()
        except Exception as e:
            logging.info(f"[Proc {proc_idx}] Engine close error: {e}")
        if ARGS.type == 'hero' and ACL_AVAILABLE:
            acl.rt.reset_device(device_id)
            acl.finalize()


def run_main_benchmark(cfg_name, cfg, perf_file, session_id):
    """run_main_benchmark"""
    logging.info(f"Starting config: {cfg_name}, Role: {ARGS.role}, Session: {session_id}")
    result_queue = mp.Queue()
    procs = []
    for i in range(cfg['procs']):
        p = mp.Process(target=worker_process, args=(i, cfg_name, cfg, result_queue, session_id))
        p.start()
        procs.append(p)

    collected = []
    failed = False
    for _ in range(cfg['procs']):
        res = result_queue.get()
        if res.get("status") == "success":
            collected.append(res)
        else:
            failed = True

    for p in procs:
        p.join()

    if not failed and collected:
        # 通用统计 (用于打印Log)
        total_dur = 0.0
        all_ops_lats = []

        # Local模式专属统计
        local_put_lats = []
        local_get_lats = []
        local_put_dur = 0.0
        local_get_dur = 0.0

        for r in collected:
            all_ops_lats.extend(r["lats"])
            total_dur = max(total_dur, r["duration"])

            if ARGS.role == 'local':
                local_put_lats.extend(r.get("put_lats", []))
                local_get_lats.extend(r.get("get_lats", []))
                local_put_dur = max(local_put_dur, r.get("put_duration", 0.0))
                local_get_dur = max(local_get_dur, r.get("get_duration", 0.0))

        if total_dur < 0.0001:
            total_dur = 0.0001

        total_ops = len(all_ops_lats)
        # 单次数据量 (Bytes)
        batch_bytes = cfg['key_num'] * cfg['blob_num'] * cfg['blob_size']
        # 总数据量 (如果是local模式，这里其实是 PUT总量 + GET总量)
        total_bytes = total_ops * batch_bytes

        # 写入 CSV
        if ARGS.role == 'local':
            # 1. 写入 Local PUT 统计
            put_ops_count = len(local_put_lats)
            put_total_bytes = put_ops_count * batch_bytes
            write_perf_csv(
                ARGS.type.upper(), "LOCAL_PUT", ARGS.engine, cfg_name, cfg,
                put_ops_count, put_total_bytes, local_put_dur, local_put_lats, perf_file
            )

            # 2. 写入 Local GET 统计
            get_ops_count = len(local_get_lats)
            get_total_bytes = get_ops_count * batch_bytes
            write_perf_csv(
                ARGS.type.upper(), "LOCAL_GET", ARGS.engine, cfg_name, cfg,
                get_ops_count, get_total_bytes, local_get_dur, local_get_lats, perf_file
            )

            # 3. 记录日志 (总带宽)
            # Local 模式下 total_ops 是 put_ops + get_ops
            # total_bytes 是 读写总量
            logging.info(f"Config {cfg_name} finished. "
                         f"Combined Throughput: {total_bytes / 1024 / 1024 / total_dur:.2f} MB/s "
                         f"(PUT: {put_total_bytes / 1024 / 1024 / local_put_dur:.2f} MB/s, "
                         f"GET: {get_total_bytes / 1024 / 1024 / local_get_dur:.2f} MB/s)")
        else:
            total_bytes = len(all_ops_lats) * batch_bytes
            write_perf_csv(
                ARGS.type.upper(), ARGS.role.upper(), ARGS.engine, cfg_name, cfg,
                len(all_ops_lats), total_bytes, total_dur, all_ops_lats, perf_file
            )
            logging.info(f"Config {cfg_name} finished. Throughput: {total_bytes / 1024 / 1024 / total_dur:.2f} MB/s")
    else:
        logging.error(f"Config {cfg_name} failed.")


if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    perf_csv_file = f"perf_{ARGS.engine}_{ARGS.role}_{LOG_TIMESTAMP}.csv"
    logging.info(f"Result File: {perf_csv_file}")
    logging.info(f"Master IP for Sync: {MASTER_IP}, Port: {SYNC_PORT}")

    if not os.path.exists("./mc_log"):
        os.makedirs("./mc_log")

    sync_srv = None
    session_id = None
    sync_cli = SyncClient(MASTER_IP, SYNC_PORT)

    if ARGS.role == "put":
        sync_srv = SyncServer("0.0.0.0", SYNC_PORT)
        sync_srv.start()
        time.sleep(1)
        session_id = f"RUN_{int(time.time())}"
        logging.info(f"Generated Session ID: {session_id}")
        sync_cli.set_val("CURRENT_SESSION_ID", session_id)

    elif ARGS.role == "get":
        logging.info(f"Waiting for Master ({MASTER_IP}) to assign Session ID...")
        while True:
            val = sync_cli.get_val("CURRENT_SESSION_ID")
            if val:
                session_id = val
                logging.info(f"Received Session ID: {session_id}")
                break
            time.sleep(1)

    elif ARGS.role == "local":
        session_id = f"LOCAL_{int(time.time())}"

    for name, config in config_map.items():
        logging.info(f"restart server to START ...")
        if ARGS.engine == 'Mooncake':
            if ARGS.role != "get":
                time.sleep(1)
                #RestartServer.restart_mooncake()
        else:
            if ARGS.role != "get":
                RestartServer.restart_etcd()
            RestartServer.restart_datasystem()
        logging.info(f"Waiting for global barrier to START {name}...")
        sync_cli.barrier(f"{session_id}_{name}_GLOBAL_START")

        run_main_benchmark(name, config, perf_csv_file, session_id)

        logging.info(f"Waiting for global barrier to FINISH {name}...")
        sync_cli.barrier(f"{session_id}_{name}_GLOBAL_END")

        logging.info("Sleeping 5s to clear resources...")
        time.sleep(5)

    if sync_srv:
        sync_srv.running = False
    
