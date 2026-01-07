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

**使用 P2PHANDSHAKE（推荐，无需外部 metadata 服务器）：**

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server P2PHANDSHAKE \
  --local-hostname 127.0.0.1:50052 \
  --protocol tcp \
  --operation both
```

**使用外部 metadata 服务器：**

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server http://127.0.0.1:50051 \
  --local-hostname 127.0.0.1:50052 \
  --protocol tcp \
  --operation both
```

常见几种模式：

- **只测 PUT：**

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server P2PHANDSHAKE \
  --local-hostname 127.0.0.1:50052 \
  --protocol tcp \
  --operation put
```

- **只测 GET：**

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server P2PHANDSHAKE \
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

**基本用法：**

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

**指定 NPU 设备卡号：**

```bash
python mooncake-store/benchmarks/multi_buffer_bench.py \
  --metadata-server 127.0.0.1:50051 \
  --local-hostname 127.0.0.1:50052 \
  --protocol ascend \
  --ascend-device-id 0 \
  --operation both \
  --batch-size 128 \
  --value-size 2097152 \
  --rounds 500
```

> 注意：
> - `--ascend-device-id` 用于指定要使用的 NPU 设备逻辑 ID（例如 `0` 表示第 0 张卡）
> - 如果不指定 `--ascend-device-id`，脚本会自动检测当前环境中的设备
> - 脚本会在初始化时显示当前使用的设备信息，包括设备 ID 和环境变量配置
> - 在容器环境中，如果只挂载了部分卡，需要根据实际的逻辑 ID 映射关系设置

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

- **`--metadata-server`**：Metadata 服务器地址，必填。支持以下格式：
  - `P2PHANDSHAKE`：使用点对点握手模式，无需外部 metadata 服务器（推荐用于测试）
  - `etcd://IP:PORT`：etcd 服务器
  - `http://IP:PORT` 或 `https://IP:PORT`：HTTP metadata server
  - `redis://IP:PORT`：Redis 服务器
- **`--local-hostname`**：本机对外可见地址和端口，形如 `IP:PORT`，必填。
- **`--protocol`**：传输协议，`tcp` / `rdma` / `ascend`，默认 `tcp`。
- **`--rdma-devices`**：RDMA 设备名列表（逗号分隔），仅在 `protocol=rdma` 时需要。
- **`--ascend-device-id`**：Ascend NPU 设备逻辑 ID，仅在 `protocol=ascend` 时有效。如果不指定，会自动检测当前环境中的设备。
- **`--master-server-addr`**：Master 服务器地址，默认 `127.0.0.1:50051`。
- **`--global-segment-size`**：全局段大小（字节），默认 `16MB`。
- **`--local-buffer-size`**：本地缓冲区大小（字节），默认 `16MB`。
- **`--operation`**：测试类型，`put` / `get` / `both`，默认 `both`。
- **`--batch-size`**：每批 key 数量，默认 `32`。
- **`--value-size`**：每个 value 大小（字节），默认 `1MB`。
- **`--rounds`**：测试轮数，默认 `100`。
- **`--warmup-rounds`**：预热轮数，默认 `10`。
- **`--prefer-same-node`**：是否优先同节点分配内存。
- **`--key-prefix`**：生成 key 的前缀，默认 `bench_key`。
- **`--unique-keys`**：为 key 前缀追加 pid/时间戳，避免旧 key 冲突。
- **`--ready-timeout-ms`**：GET 前等待副本就绪的时间（毫秒，0 表示不等待）。
- **`--hold-after-run`**：测试结束后保持进程不退出，方便观察 master 日志。
- **`--hold-seconds`**：保持时长（秒，0 表示等待 Ctrl+C）。

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
  - Ascend 模式下会走 ADXL，设备 ID 会自动通过 `aclrtGetDevice()` 检测，或通过 `--ascend-device-id` 参数指定。
- **设备选择**：
  - 使用 `--ascend-device-id` 可以明确指定要使用的 NPU 卡号（逻辑 ID）
  - 脚本会在初始化时显示设备信息，包括当前设备 ID 和 `ASCEND_RT_VISIBLE_DEVICES` 环境变量
  - 可以通过查看 C++ 日志中的 `deviceLogicId` 字段确认实际使用的设备
- **复制开关**：脚本内设置了 `MC_STORE_MEMCPY=0` 来禁止本地 memcpy 优化，尽量逼近真实网络传输性能。

---

## 故障排除

### Metadata Server 连接失败

**推荐解决方案：使用 P2PHANDSHAKE**

如果遇到 metadata server 连接问题，最简单的方法是使用 `P2PHANDSHAKE` 模式，无需外部 metadata 服务器：

```bash
python multi_buffer_bench.py \
  --metadata-server P2PHANDSHAKE \
  --local-hostname 10.50.93.61:50055 \
  --protocol ascend \
  --operation both
```

**如果必须使用外部 metadata server：**

如果遇到类似错误：
```
Unable to find metadata storage plugin etcd with conn string: 10.50.93.61:8088
```

**原因**：metadata server 连接字符串缺少协议前缀。

**解决方法**：
- 如果使用 etcd：使用 `etcd://10.50.93.61:2379` 格式
- 如果使用 HTTP metadata server：使用 `http://10.50.93.61:8088` 格式
- 如果使用 Redis：使用 `redis://10.50.93.61:6379` 格式

示例：
```bash
python multi_buffer_bench.py \
  --metadata-server http://10.50.93.61:8088 \
  --local-hostname 10.50.93.61:50055 \
  --protocol ascend \
  --operation both
```

### 存储目录不存在

如果遇到警告：
```
Root directory does not exist: /vllm-workspace/mc_storage
Failed to initialize storage backend
```

**说明**：这是警告信息，不会影响 benchmark 运行。存储后端用于数据持久化，benchmark 测试通常不需要持久化存储。

**解决方法**（如果需要持久化）：
1. 创建存储目录：`mkdir -p /vllm-workspace/mc_storage`
2. 或者通过 master server 配置正确的存储路径

### Metadata Server 连接超时

如果遇到连接超时错误：
```
PUT https://10.50.93.61:8088?key=... curl: Timeout was reached
Connection timeout after 1501 ms
Failed to initialize transfer engine
```

**可能的原因和解决方法**：

1. **Metadata server 未运行或不可访问**：
   - 检查 metadata server 是否正在运行：`curl http://10.50.93.61:8088`
   - 确认网络连通性：`ping 10.50.93.61`

2. **防火墙或网络策略阻止连接**：
   - 检查防火墙规则
   - 确认端口是否开放

3. **Metadata server 地址或端口错误**：
   - 确认 metadata server 的实际地址和端口
   - 检查是否使用了正确的协议前缀（`http://`、`https://`、`etcd://` 等）

4. **超时时间过短**：
   - 某些网络环境下可能需要更长的超时时间
   - 检查环境变量或配置中的超时设置

**验证步骤**：
```bash
# 测试 HTTP metadata server
curl -v http://10.50.93.61:8088

# 测试 etcd（如果使用）
etcdctl --endpoints=http://10.50.93.61:2379 endpoint health
```
