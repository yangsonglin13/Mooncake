# Mooncake Master BatchPut Bottleneck Benchmark

本文档用于构造一个只压 Mooncake Store Master 控制面的场景，观察 `BatchPut` 路径在并发增加时的吞吐拐点。

## 目标

`BatchPut` 压测用于隔离 Master 的写控制面瓶颈。该场景不关注真实数据传输吞吐，而是重点压：

- RPC server worker 处理能力
- `PutStart` 空间分配路径
- metadata shard 写入和锁竞争
- `PutEnd` 对对象状态的更新

当并发继续增加但 `ops/s` 不再接近线性增长，或 Master CPU 接近打满、每秒完成数抖动明显时，即可认为接近 Master 写控制面的拐点。

## 前置条件

需要先完成构建，并确认以下二进制存在：

```bash
./build/mooncake-store/src/mooncake_master
./build/mooncake-store/benchmarks/master_bench
```

`master_bench` 是纯 Master 控制面 benchmark。它会先向 Master mount 一批假 segment，然后通过多个 client/thread 循环调用 `BatchPutStart` 和 `BatchPutEnd`。

## 1. 启动 Master

建议每一组并发测试前都重启 Master，避免上一轮累计的 key、lease、eviction 状态影响结果。

```bash
./build/mooncake-store/src/mooncake_master \
  --rpc_port=50051 \
  --rpc_thread_num=4 \
  --eviction_high_watermark_ratio=1.0
```

参数说明：

- `--rpc_thread_num=4`：先固定 RPC worker 线程数，便于观察并发变化带来的吞吐曲线。
- `--eviction_high_watermark_ratio=1.0`：尽量避免压测早期被 eviction 干扰。
- `--rpc_port=50051`：保持和 benchmark 默认地址一致。

后续可以把 `rpc_thread_num` 改成 `8/16/32` 再重复测试，用于判断瓶颈是否主要在 RPC worker。

## 2. 单并发 Baseline

先跑一组 `1 client x 1 thread`：

```bash
./build/mooncake-store/benchmarks/master_bench \
  --master_server=127.0.0.1:50051 \
  --operation=BatchPut \
  --num_segments=128 \
  --segment_size=68719476736 \
  --num_clients=1 \
  --num_threads=1 \
  --batch_size=128 \
  --value_size=4096 \
  --duration=60
```

参数说明：

- `--operation=BatchPut`：压 `BatchPutStart + BatchPutEnd`。
- `--num_segments=128`：向 Master 注册 128 个 segment，给 allocator 足够选择空间。
- `--segment_size=68719476736`：每个 segment 64 GiB，避免过早空间不足。
- `--batch_size=128`：每个 batch 128 个 key。
- `--value_size=4096`：每个对象 4 KiB，减少容量消耗，突出控制面开销。
- `--duration=60`：每轮运行 60 秒。

输出里重点看：

```text
Completed operations: <this-second-ops>
Operations per second: <average-ops>
```

## 3. 并发扫描

保持其他参数不变，只调整 `num_clients` 和 `num_threads`。

建议第一轮固定 `num_threads=1`，只增加 `num_clients`：

```text
1 x 1
2 x 1
4 x 1
8 x 1
16 x 1
32 x 1
64 x 1
128 x 1
```

如果吞吐还没有明显平台化，再增加每个 client 的线程数：

```text
64 x 2
64 x 4
128 x 2
128 x 4
```

示例：

```bash
./build/mooncake-store/benchmarks/master_bench \
  --master_server=127.0.0.1:50051 \
  --operation=BatchPut \
  --num_segments=128 \
  --segment_size=68719476736 \
  --num_clients=32 \
  --num_threads=1 \
  --batch_size=128 \
  --value_size=4096 \
  --duration=60
```

## 4. 结果记录表

每轮建议记录 Master CPU 使用率、平均吞吐、每秒完成数是否抖动、有无错误日志。

| Run | rpc_thread_num | num_clients | num_threads | concurrency | batch_size | value_size | ops/s | Master CPU | Error / Note |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 1 | 4 | 1 | 1 | 1 | 128 | 4096 | | | |
| 2 | 4 | 2 | 1 | 2 | 128 | 4096 | | | |
| 3 | 4 | 4 | 1 | 4 | 128 | 4096 | | | |
| 4 | 4 | 8 | 1 | 8 | 128 | 4096 | | | |
| 5 | 4 | 16 | 1 | 16 | 128 | 4096 | | | |
| 6 | 4 | 32 | 1 | 32 | 128 | 4096 | | | |
| 7 | 4 | 64 | 1 | 64 | 128 | 4096 | | | |
| 8 | 4 | 128 | 1 | 128 | 128 | 4096 | | | |

## 5. 如何判断拐点

可以用以下标准判断 Master 开始到达瓶颈：

- 并发翻倍，但 `ops/s` 只提升很少，例如低于 10% 到 20%。
- `ops/s` 开始下降。
- Master CPU 接近打满。
- `Completed operations` 每秒输出抖动明显。
- 日志出现 `NO_AVAILABLE_HANDLE`、RPC failure、timeout 或其他错误。

最典型的曲线形态：

```text
concurrency: 1    2    4    8    16   32   64   128
ops/s:       10k  20k  38k  70k  95k  102k 100k 88k
```

这里 `16 -> 32` 开始收益明显变小，`64 -> 128` 下降，说明拐点大概率在 `16` 到 `32` 并发附近。

## 6. 对比实验

### RPC 线程数对比

在找到初步拐点后，保持 benchmark 参数不变，只改 Master：

```bash
--rpc_thread_num=4
--rpc_thread_num=8
--rpc_thread_num=16
--rpc_thread_num=32
```

判断方法：

- 如果 `rpc_thread_num` 增加后吞吐继续明显上升，瓶颈主要在 RPC worker。
- 如果 `rpc_thread_num` 增加后吞吐提升很小，瓶颈更可能在 metadata shard、segment allocator、锁竞争或单进程 CPU。

### Batch Size 对比

保持总并发不变，测试：

```text
batch_size=1
batch_size=8
batch_size=32
batch_size=128
batch_size=512
```

判断方法：

- 小 batch 更容易暴露 RPC per-key 开销。
- 大 batch 会减少 RPC 次数，但单 RPC 内会循环处理更多 key，更容易压 Master CPU 和 allocator。

## 7. 注意事项

- 每轮最好重启 Master，保证不同并发点可比。
- `BatchPut` 会持续创建新 key，运行时间太久可能引入 metadata 规模影响。
- 如果出现 `NO_AVAILABLE_HANDLE`，说明 allocator 可用空间不足或 segment 设置不够，需要增大 `num_segments` 或 `segment_size`。
- 如果需要看 P95/P99 latency，现有 `master_bench` 需要补充 latency histogram；当前主要依赖平均 `ops/s` 和每秒完成数判断拐点。
