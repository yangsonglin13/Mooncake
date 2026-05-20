# Mooncake Conductor 订阅 Mooncake KV Event 说明

## 1. 结论概览

Mooncake conductor 订阅的是 Mooncake master 通过 ZeroMQ 发布的 **KV event 元数据**，不是 KV 数据本身。

这些事件用于让 conductor 维护一份缓存索引，回答类似下面的问题：

- 某个 KV block / Mooncake object 是否存在。
- 它当前有哪些副本。
- 副本位于 memory、disk、local_disk 等哪类存储介质。
- 这些信息是否足够支撑 prefix cache hit / cache-aware routing。

需要特别注意：**Mooncake 的设计文档中描述了 block 级 `BlockStoreEvent`，但当前代码实现里 Mooncake store 主要发布的是 object/replica 级 `BlockUpdateEvent` 和 `RemoveAllEvent`。这与 conductor 期望构建 prefix index 的需求还没有完全对齐。**

## 2. Conductor 订阅了 Mooncake 什么信息

conductor 通过 ZMQ SUB socket 订阅 Mooncake publisher。Mooncake 侧默认 topic 是：

```text
mooncake
```

ZMQ 消息是 multipart：

```text
[topic, seq, payload]
```

其中：

- `topic`：例如 `mooncake`。
- `seq`：publisher 生成的递增序号，用于发现丢事件并支持 replay。
- `payload`：msgpack 序列化后的 `EventBatch`。

Mooncake 的 `EventBatch` 结构是：

```text
[timestamp, [event1, event2, ...]]
```

## 3. 设计上 Mooncake 事件包含哪些类型

从 Mooncake conductor / kv-event 设计文档看，Mooncake G2/G3 事件设计为三类。

### 3.1 BlockStoreEvent

表示某个 Mooncake Store object 被新增或首次存储。

设计字段包括：

```text
BlockStoreEvent {
    "BlockStoreEvent",
    mooncake_key,
    replica_list,
    model_name,
    block_size,
    block_hash,
    parent_block_hash,
    token_ids
}
```

这些字段里，真正支撑 conductor 做 prefix cache 命中的关键字段是：

- `block_hash`
- `parent_block_hash`
- `token_ids`
- `block_size`
- `model_name`
- `lora_name`

因为 conductor 需要用这些字段把输入 token 切 block、计算 prefix hash，并判断最长连续命中。

### 3.2 BlockUpdateEvent

表示某个 Mooncake object 的副本位置发生变化，例如复制、迁移、驱逐、offload、删除部分副本等。

当前实现里的字段更接近：

```text
BlockUpdateEvent {
    "BlockUpdateEvent",
    mooncake_key,
    replicas
}
```

其中 `replicas` 是副本位置列表，可能包含：

```text
["memory", endpoint]
["disk", file_path]
["local_disk", transport_endpoint]
```

这类事件适合回答“这个 key 现在在哪些介质/节点上有副本”，但如果没有 block hash 和 token ids，就不能独立支撑 `longest_matched` 计算。

### 3.3 RemoveAllEvent

表示 Mooncake store 清理全部对象。

```text
RemoveAllEvent {
    "RemoveAllEvent"
}
```

这个事件用于通知订阅者清理本地索引。

## 4. 当前代码实际实现情况

当前代码存在一个重要差异。

### 4.1 Conductor 侧

conductor 的 ZMQ client 会订阅所有 topic：

```go
sock.SetSubscribe("")
```

收到消息后，如果 topic 是 `mooncake`，会调用：

```go
DecodeMooncakeEventBatch(payload)
```

Mooncake parser 声明支持：

```go
"BlockStoreEvent"  -> BlockStored
"BlockUpdateEvent" -> BlockUpdate
"RemoveAllEvent"   -> AllBlocksCleared
```

但实际 `ParseEvent` 目前只处理了 `BlockStoreEvent`：

```go
switch eventType {
case EventTypeBlockStored:
    return parseMooncakeBlockStored(raw, timestamp)
default:
    return nil, fmt.Errorf("unhandled event: %s", eventType)
}
```

也就是说，当前 conductor 对 Mooncake 的 `BlockUpdateEvent` / `RemoveAllEvent` 还没有完整消费逻辑。

### 4.2 Mooncake Store 侧

当前 Mooncake store 的 `kv_event.hpp` 实际定义了：

- `BlockUpdateEvent`
- `RemoveAllEvent`

当前没有实际定义 `BlockStoreEvent` 类。

`BlockUpdateEvent` 当前序列化字段是：

```cpp
[
    "BlockUpdateEvent",
    mooncake_key,
    replicas
]
```

`RemoveAllEvent` 当前序列化字段是：

```cpp
[
    "RemoveAllEvent"
]
```

因此，**当前 Mooncake store 实现主要提供的是 object 副本位置变化信息，而不是完整 block 级 prefix 匹配信息。**

## 5. Mooncake 怎么实现并提供这些信息

Mooncake master 通过启动参数开启 KV event publisher：

```bash
mooncake_master \
  -enable_kv_event_publish \
  -kv_event_publisher_endpoint tcp://*:19997 \
  -rpc_port 50051
```

相关参数包括：

```text
enable_kv_event_publish
kv_event_publisher_endpoint
kv_event_publisher_replay_endpoint
kv_event_publisher_hwm
kv_event_publisher_send_interval_ms
kv_event_publisher_max_batch_size
kv_event_publisher_auto_port
kv_event_publisher_topic
```

默认 topic 是：

```text
mooncake
```

### 5.1 初始化流程

`MasterService` 初始化时，如果 `enable_kv_event_publish` 为 true，会创建：

```cpp
publisher = std::make_unique<KVEventSystem>(config.kv_event_publisher_config);
```

`KVEventSystem` 内部包含：

```text
KVEventSystem
  -> KVEventProducer
  -> KVEventQueue
  -> KVEventConsumer
  -> ZMQ PUB socket
```

### 5.2 发布流程

发布链路是：

```text
MasterService 元数据变化
  -> publisher->publish<BlockUpdateEvent / RemoveAllEvent>
  -> KVEventProducer 异步入队
  -> KVEventQueue 缓冲
  -> KVEventConsumer 批量取出
  -> EventBatch msgpack 序列化
  -> ZMQ PUB 发送 [topic, seq, payload]
  -> conductor ZMQ SUB 接收
  -> conductor msgpack 解码
  -> conductor 更新本地索引
```

### 5.3 典型触发点

Mooncake master 在多个对象元数据变化点发布事件。

典型调用形式：

```cpp
publisher->publish<BlockUpdateEvent>(
    key, metadata.GetReplicasDescriptorList());
```

常见触发场景包括：

- 副本写入完成。
- replica clear。
- copy / move / migration 完成。
- remove object。
- remove by regex。
- remove all。
- offload 成功。
- eviction 后副本集合变化。
- stale handle cleanup 后副本集合变化。

当清空全部对象时，会发布：

```cpp
publisher->publish<RemoveAllEvent>();
```

## 6. Conductor 如何使用这些信息

conductor 的目标是维护一个 prefix cache table。

对 vLLM 这类 G1 事件，事件本身包含：

- `block_hashes`
- `parent_block_hash`
- `token_ids`
- `block_size`
- `medium`
- `dp_rank`

因此 conductor 可以直接构建：

```text
prefix hash -> medium / dp_rank / replica count
```

查询时，conductor 根据输入 token 重新计算 prefix hash，并输出：

```json
{
  "longest_matched": 256,
  "DP": {
    "0": 256
  },
  "GPU": 128,
  "CPU": 256,
  "DISK": 0
}
```

其中：

- `longest_matched`：输入 token 从开头开始连续命中的最长长度。
- `GPU` / `CPU` / `DISK`：命中的 block 分布在哪些介质上。
- `DP`：命中的 block 分布在哪些 data parallel rank 上。

但对 Mooncake store 当前实现来说，`BlockUpdateEvent` 只有 `mooncake_key + replicas`，缺少 block hash / token ids。因此它更适合维护“某个 object 在哪里”，还不足以独立维护“某个 token prefix 命中了多少”。

## 7. 当前实现的缺口

当前 Mooncake conductor 与 Mooncake store 的事件协议存在几个缺口：

1. conductor 设计上需要 `BlockStoreEvent`，但 Mooncake store 当前主要实现 `BlockUpdateEvent`。
2. conductor 的 Mooncake parser 声明支持 `BlockUpdateEvent` / `RemoveAllEvent`，但当前没有完整 handler。
3. Mooncake store 当前发布的 `BlockUpdateEvent` 不包含 `block_hash`、`parent_block_hash`、`token_ids`、`block_size`。
4. 如果只有 `mooncake_key + replicas`，conductor 只能做位置感知，不能完整做 prefix-aware routing。
5. Mooncake Store object 和 vLLM KV block 不是天然一一对应。一个 KV block 可能被切成多个 Mooncake object，也可能因 TP/CP 等并行策略有复杂映射。

## 8. 对 Yuanrong 的启发

如果 Yuanrong 要支持类似 Mooncake conductor 的 cache-aware scheduling，不能只提供 `exist/get/put` 这类数据面接口，还需要提供控制面事件。

至少需要明确两类事件。

### 8.1 Block 语义事件

用于 prefix 匹配和 longest matched 计算：

```text
BlockStored {
    instance_id,
    cache_instance_id,
    worker_id,
    tenant_id,
    model_name,
    lora_name,
    dp_rank,
    block_size,
    block_hash,
    parent_block_hash,
    token_ids,
    medium
}
```

### 8.2 Object / replica 位置事件

用于描述数据系统内部副本位置：

```text
ObjectReplicaUpdated {
    object_key,
    worker_id,
    replica_locations,
    medium,
    version
}
```

这两类事件最好不要混在一起。前者面向调度器的 prefix 命中判断，后者面向数据系统的副本位置管理。

## 9. 一句话总结

Mooncake conductor 订阅 Mooncake 发布的 KV event，目标是用事件维护缓存命中索引。Mooncake 当前代码已经有异步 ZMQ event publisher，并在 master 元数据变化时发布 replica update 信息；但当前实现主要是 object/replica 位置事件，尚未完整提供 conductor 做 `longest_matched` 所需的 block hash / token ids / block size 等 prefix 匹配信息。
