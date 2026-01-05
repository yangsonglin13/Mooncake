## Multi-Buffer Batch Operations Benchmark

这个脚本用于测试 `batch_get_into_multi_buffers` 和 `batch_put_from_multi_buffers` 两个接口的性能。

### 功能特性

- **PUT/GET 性能测试**：支持单独测试 PUT、GET，或同时测试二者（both）。
- **参数可配置**：批次大小、值大小、轮数等均可配置。
- **详细性能指标**：输出 QPS、带宽（GB/s）、批次级 P50/P95/P99 延迟等。
- **多协议支持**：支持 `tcp`、`rdma` 和 `ascend`（ADXL）协议。
- **自动 buffer 管理**：脚本内部自动注册 / 注销 zero-copy buffer。

### 依赖要求

- Python 3.6+
- `numpy`
- 已安装并可在 Python 中 `import mooncake.store` 的 Mooncake 包

安装依赖示例：

```bash
pip install numpy
```

---

## 使用方法

脚本路径：`mooncake-store/benchmarks/multi_buffer_bench.py`

### 基本用法（TCP）

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py ^
  --metadata-server 127.0.0.1:50051 ^
  --local-hostname 127.0.0.1:50052 ^
  --protocol tcp ^
  --operation both
```

在 Linux / Mac 上可以写成：

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server 127.0.0.1:50051 \
  --local-hostname 127.0.0.1:50052 \
  --protocol tcp \
  --operation both
```

常见几种模式：

- **只测 PUT：**

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server 127.0.0.1:50051 \
  --local-hostname 127.0.0.1:50052 \
  --protocol tcp \
  --operation put
```

- **只测 GET：**

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server 127.0.0.1:50051 \
  --local-hostname 127.0.0.1:50052 \
  --protocol tcp \
  --operation get
```

---

### 使用 RDMA 协议

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server 127.0.0.1:50051 \
  --local-hostname 127.0.0.1:50052 \
  --protocol rdma \
  --rdma-devices mlx5_0,mlx5_1 \
  --operation both \
  --batch-size 64 \
  --value-size 1048576 \
  --rounds 1000
```

> 注意：RDMA 模式下需要正确配置 RDMA 设备，并在 `--rdma-devices` 中写设备名（例如 `mlx5_0`）。

---

### 使用 Ascend 协议（走 ADXL）

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server 127.0.0.1:50051 \
  --local-hostname 127.0.0.1:50052 \
  --protocol ascend \
  --operation both \
  --batch-size 128 \
  --value-size 2097152 \
  --rounds 500 \
  --prefer-same-node
```

---

### 自定义参数示例

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server 127.0.0.1:50051 \
  --local-hostname 127.0.0.1:50052 \
  --protocol tcp \
  --operation both \
  --batch-size 32 \            # 每批 key 数量
  --value-size 1048576 \       # 每个 value 大小（字节）
  --rounds 100 \               # 测试轮数
  --warmup-rounds 10 \         # 预热轮数
  --prefer-same-node \         # 优先同节点
  --global-segment-size 16777216 \  # 全局段大小
  --local-buffer-size 16777216      # 本地缓冲区大小
```

---

## 参数说明

- **`--metadata-server`**：Metadata 服务器地址，形如 `IP:PORT`，必填。
- **`--local-hostname`**：本机对外可见地址和端口，形如 `IP:PORT`，必填。
- **`--protocol`**：传输协议，`tcp` / `rdma` / `ascend`，默认 `tcp`。
- **`--rdma-devices`**：RDMA 设备名列表（逗号分隔），仅在 `protocol=rdma` 时需要。
- **`--master-server-addr`**：Master 服务器地址，默认 `127.0.0.1:50051`。
- **`--global-segment-size`**：全局段大小（字节），默认 `16MB`。
- **`--local-buffer-size`**：本地缓冲区大小（字节），默认 `16MB`。
- **`--operation`**：测试类型，`put` / `get` / `both`，默认 `both`。
- **`--batch-size`**：每批 key 数量，默认 `32`。
- **`--value-size`**：每个 value 大小（字节），默认 `1MB`。
- **`--rounds`**：测试轮数，默认 `100`。
- **`--warmup-rounds`**：预热轮数，默认 `10`。
- **`--prefer-same-node`**：是否优先同节点分配内存。

---

## 输出说明

每种操作（PUT / GET）都会输出类似的统计信息，包括：

- **Total Operations**：总操作数（按 key 计数）
- **Successful Operations / Failed Operations**：成功 / 失败数
- **Success Rate**：成功率
- **Total Data Transferred**：总传输数据量（GB）
- **Total Time**：总耗时（秒）
- **QPS**：每秒成功操作数
- **Bandwidth**：带宽（GB/s）
- **Latency (per batch)**：批次级延迟（平均 / P50 / P95 / P99，单位毫秒）

示例：

```text
================================================================================
Benchmark Results: PUT
================================================================================
Total Operations:      3200
Successful Operations: 3200
Failed Operations:     0
Success Rate:          100.00%

Total Data Transferred: 3.05 GB
Total Time:             12.345 seconds

Throughput:
  QPS:                  259.26 ops/s
  Bandwidth:            0.25 GB/s

Latency (per batch):
  Average:              38.56 ms
  P50:                  37.20 ms
  P95:                  45.10 ms
  P99:                  52.30 ms
================================================================================
```

---

## 注意事项

- **Buffer 注册**：脚本内部会自动为所有 numpy buffer 调用 `register_buffer`，结束时自动 `unregister_buffer`。
- **内存占用**：总内存约为 `batch_size × value_size × 2`，请预留足够内存。
- **网络与协议**：
  - RDMA / Ascend 模式下需要正确的网络与驱动配置；
  - Ascend 模式下会走 ADXL。
- **复制开关**：脚本内设置了 `MC_STORE_MEMCPY=0` 来禁止本地 memcpy 优化，尽量逼近真实网络传输性能。

