# Yuanrong Event Source Support for Mooncake Conductor

## 1. 背景

当前 Mooncake Store 已实现的 KV event 只覆盖对象副本位置变化：

```text
["BlockUpdateEvent", mooncake_key, replicas]
["RemoveAllEvent"]
```

其中 `BlockUpdateEvent` 的 `replicas` 是当前位置快照，不是增量。它只表达某个 object key 当前在哪些介质或节点上有副本。

本设计按当前已实现协议对齐 Yuanrong，不引入 Mooncake 文档里尚未落地的 prefix 字段：

- 不引入 `block_size`
- 不引入 `block_hashes`
- 不引入 `parent_block_hash`
- 不引入 `token_ids`
- 不引入 `model_name` / `lora_name`

因此本方案的目标是让 Mooncake Conductor 能订阅 Yuanrong 的对象副本事件，维护 `yuanrong_key -> replicas` 的位置视图。它不提供 prefix-aware routing，也不支持 `/query` 返回 `longest_matched`。

## 2. 目标

1. 在 Mooncake Conductor 中新增 `Yuanrong` 事件源类型。
2. Yuanrong Worker 提供 ZMQ PUB 事件发布。
3. Yuanrong 事件格式复用当前 Mooncake 已实现的最小 `BlockUpdateEvent` 形态。
4. Conductor 能解析 Yuanrong 事件，并维护对象副本位置索引。
5. 保持现有 vLLM/Mooncake prefix index 逻辑不受影响。

## 3. 非目标

1. 不改造 Yuanrong 数据面接口传递 token metadata。
2. 不要求 Yuanrong 计算 vLLM block hash。
3. 不让 `BlockUpdateEvent` 参与 Conductor 现有 prefix index。
4. 不让 Yuanrong 事件直接服务 `/query` 的 longest-prefix cache hit 计算。

## 4. Yuanrong 事件协议

### 4.1 ZMQ 帧格式

对齐 Mooncake Store 当前 publisher：

```text
[topic, seq_be, payload]
```

- `topic`: 建议固定为 `"yuanrong"`，用于 Conductor 区分事件源。
- `seq_be`: 8 字节 big-endian `uint64`，单 publisher 内单调递增。
- `payload`: msgpack 编码的 event batch。

### 4.2 Payload 格式

```text
[
  timestamp,
  [
    event1,
    event2,
    ...
  ]
]
```

- `timestamp`: Unix timestamp，建议用秒级 `float64` 或 `int64`。
- `events`: 事件数组。

### 4.3 BlockUpdateEvent

```json
[
  "BlockUpdateEvent",
  "yuanrong_key",
  [
    ["memory", "tcp://worker-ip:port"],
    ["disk", "/path/or/object/location"],
    ["local_disk", "tcp://worker-ip:port"]
  ]
]
```

字段含义：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| event type | string | 固定为 `"BlockUpdateEvent"` |
| yuanrong_key | string | Yuanrong 对象 key，等价 Mooncake 的 `mooncake_key` |
| replicas | array | 当前 key 的完整副本位置快照 |

replica 二元组：

| 字段 | 类型 | 允许值或示例 |
| --- | --- | --- |
| medium | string | `"memory"` / `"disk"` / `"local_disk"` |
| location | string | `tcp://worker-ip:port`、本地路径、对象路径等 |

`replicas` 必须表示当前完整状态，而不是新增或删除的 delta。Conductor 收到后直接覆盖该 key 的旧副本列表。

### 4.4 删除对象

删除某个 key 或该 key 已无副本时，发送空副本列表：

```json
[
  "BlockUpdateEvent",
  "yuanrong_key",
  []
]
```

Conductor 收到后删除该 key 的位置视图。

### 4.5 全量清空

```json
[
  "RemoveAllEvent"
]
```

Conductor 收到后清空对应 Yuanrong instance/source 的对象副本位置视图。

## 5. Conductor 侧设计

### 5.1 配置新增 Yuanrong 类型

当前 `main.go` 的 `mapServiceType` 只接受 `"vLLM"` 和 `"Mooncake"`。新增：

```go
const ServiceTypeYuanrong string = "Yuanrong"
```

并在 `mapServiceType` 中接受：

```go
case "Yuanrong":
    return common.ServiceTypeYuanrong, true
```

配置示例：

```json
{
  "kvevent_instance": {
    "yuanrong-worker-1": {
      "endpoint": "tcp://127.0.0.1:19997",
      "replay_endpoint": "",
      "type": "Yuanrong",
      "modelname": "",
      "lora_name": "",
      "tenant_id": "default",
      "instance_id": "yuanrong-worker-1",
      "block_size": 0,
      "dp_rank": 0,
      "additionalsalt": ""
    }
  },
  "http_server_port": 13333
}
```

`modelname`、`lora_name`、`block_size` 对 Yuanrong 当前位置事件无业务作用，只是兼容现有配置结构。

### 5.2 ZMQ client 支持 Yuanrong topic

当前 `ZMQClient.processMessage` 通过 topic 选择 decoder：

```go
switch string(topic) {
case "mooncake":
    batch, err = DecodeMooncakeEventBatch(payload)
default:
    batch, err = DecodeVllmEventBatch(payload)
}
```

新增：

```go
case "yuanrong":
    batch, err = DecodeYuanrongEventBatch(payload)
```

这样 Yuanrong publisher 只需发送 topic `"yuanrong"`，不会被误按 vLLM 的 3 元素 batch 解码。

### 5.3 replay endpoint 改为可选

当前 `ZMQClient.Connect` 会无条件创建并连接 `ReplayEndpoint`。如果 Yuanrong 只实现 PUB，不实现 replay，Conductor 会连接失败。

建议改造：

- `ReplayEndpoint == ""` 时不创建 DEALER socket。
- `requestReplay` 在 `replaySocket == nil` 时直接返回 nil 或记录 warning。
- gap 检测仍保留日志，但无 replay endpoint 时不触发补拉。

这样 Yuanrong 第一阶段只需要实现 ZMQ PUB。后续如果需要可靠补偿，再补 replay endpoint。

### 5.4 Yuanrong parser

新增 `yuanrongParser`，格式与当前 Mooncake `EventBatch` 相同，都是 2 元素：

```go
func DecodeYuanrongEventBatch(data []byte) (*EventBatch, error) {
    return decodeCommonEventBatch(
        data,
        2,
        extractMooncakeStyleEvents,
        newYuanrongParser(),
    )
}
```

`yuanrongParser.EventMappings()`：

```go
map[string]EventType{
    "BlockUpdateEvent": EventTypeBlockUpdate,
    "RemoveAllEvent":   EventTypeAllCleared,
}
```

解析 `BlockUpdateEvent`：

```text
index 0: "BlockUpdateEvent"
index 1: key string
index 2: replicas [][]string
```

建议把 Mooncake 当前实际的 `BlockUpdateEvent` 也接到同一个 parser helper 上，避免 Mooncake 当前真实事件继续报 `unhandled event`。

### 5.5 Conductor 内部事件结构

当前 `zmq.BlockUpdateEvent` 结构里保留了 block hash/token 字段，但这些字段不是当前 Mooncake/Yuanrong `BlockUpdateEvent` 的真实内容。

建议调整为当前位置事件结构，或新增更准确的结构：

```go
type BlockUpdateEvent struct {
    Type        EventType
    Timestamp   time.Time
    Key         string
    ReplicaList [][]string
    PodName     string
}
```

如果为了减少改动保留原结构，也至少需要新增：

```go
MooncakeKey string
ReplicaList [][]string
```

### 5.6 位置索引

由于 `BlockUpdateEvent` 没有 `token_ids`，不能进入 `PrefixCacheTable.ProcessStoreEvent`。Conductor 需要单独维护对象副本位置索引：

```go
type ReplicaLocation struct {
    Medium   string `json:"medium"`
    Location string `json:"location"`
}

type ObjectReplicaIndex struct {
    mu sync.RWMutex
    data map[string]map[string][]ReplicaLocation
    // instanceID -> key -> replicas
}
```

更新规则：

- `BlockUpdateEvent` with non-empty replicas: `data[instanceID][key] = replicas`
- `BlockUpdateEvent` with empty replicas: `delete(data[instanceID], key)`
- `RemoveAllEvent`: `delete(data, instanceID)` or clear instance map

### 5.7 Event handler

`KVEventHandler.HandleEvent` 新增：

```go
case *zmq.BlockUpdateEvent:
    return h.handleBlockUpdate(ctx, e)
case *zmq.AllBlocksClearedEvent:
    return h.handleAllBlocksCleared(ctx, e)
```

`handleBlockUpdate` 只更新 `ObjectReplicaIndex`，不调用 `ProcessStoreEvent`。

### 5.8 HTTP 查询接口

现有 `/query` 仍用于 prefix cache hit，不改变语义。

建议新增轻量位置查询接口：

```text
POST /replica_query
```

请求：

```json
{
  "instance_id": "yuanrong-worker-1",
  "key": "yuanrong_key"
}
```

响应：

```json
{
  "instance_id": "yuanrong-worker-1",
  "key": "yuanrong_key",
  "replicas": [
    {"medium": "memory", "location": "tcp://10.0.0.12:31501"},
    {"medium": "disk", "location": "/data/yuanrong/spill/xxx"}
  ]
}
```

也可以提供调试接口：

```text
GET /replica_view
```

返回所有 instance 的 key -> replicas 视图。

## 6. Yuanrong 侧设计

### 6.1 新增组件

在 Yuanrong Worker 内新增 `KvEventPublisher` 组件：

职责：

- 维护 ZMQ PUB socket。
- 维护本地单调递增 `seq`。
- 将事件打包为 Mooncake-style msgpack payload。
- 在对象副本状态变化后异步发布事件。

建议配置：

```json
{
  "enable_kv_event_publish": true,
  "kv_event_publisher_endpoint": "tcp://*:19997",
  "kv_event_publisher_topic": "yuanrong",
  "kv_event_publisher_hwm": 100000,
  "kv_event_publisher_max_batch_size": 50,
  "kv_event_publisher_send_interval_ms": 0
}
```

### 6.2 发布时机

Yuanrong 需要在以下状态变化点发布 `BlockUpdateEvent`：

1. key 创建并在内存中可读。
2. key 副本新增。
3. key 副本删除或失效。
4. key 从内存 spill 到磁盘。
5. key 从磁盘恢复到内存。
6. key 迁移或恢复后副本位置变化。
7. key 删除。

重要约束：每次发布该 key 的完整副本列表，而不是只发布变化项。

### 6.3 事件构造示例

内存副本：

```json
[
  "BlockUpdateEvent",
  "kv_prefix/request_001/block_0001",
  [
    ["memory", "tcp://10.0.0.12:31501"]
  ]
]
```

内存 + 磁盘副本：

```json
[
  "BlockUpdateEvent",
  "kv_prefix/request_001/block_0001",
  [
    ["memory", "tcp://10.0.0.12:31501"],
    ["disk", "/data/yuanrong/spill/kv_prefix/request_001/block_0001"]
  ]
]
```

删除 key：

```json
[
  "BlockUpdateEvent",
  "kv_prefix/request_001/block_0001",
  []
]
```

全清：

```json
[
  "RemoveAllEvent"
]
```

### 6.4 Worker 代码接入点

基于 `yuanrong-datasystem` 当前 worker/object-cache 代码，Yuanrong 事件发布应该放在 worker 本地对象状态已经成功变化之后，而不是 client 层或 master 请求发起之前。

推荐新增 `YuanrongKvEventPublisher` 组件，放在 `src/datasystem/worker/object_cache/` 下：

- 负责维护 ZMQ PUB socket、单调递增 `seq`、topic、batch 打包和异步发送。
- 在 `WorkerOCServer::CreateWorkerServices()` 中创建，因为这里创建了 `ObjectTable`、`WorkerOcEvictionManager` 和 `WorkerOCServiceImpl`。
- 将 publisher 放入 `WorkerOcServiceCrudParam`，让 publish、multi-publish、delete、eviction、migration 等 object-cache handler 共享同一个 publisher。
- 使用 `FLAGS_worker_address` 或 `WorkerOCServer` 的 `hostPort_` 作为默认 memory replica location；这是对象访问地址，不是 Conductor 订阅的 PUB endpoint。

关键源码位置：

| 文件 | 建议挂点 | 事件语义 |
| --- | --- | --- |
| `src/datasystem/worker/worker.cpp` | `FLAGS_worker_address` 解析 | 提供本 worker 对象访问地址 |
| `src/datasystem/worker/worker_oc_server.cpp` | `WorkerOCServer::CreateWorkerServices()` | 初始化 publisher，并传给 `WorkerOCServiceImpl` |
| `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp` | `WorkerOCServiceImpl::InitServiceImpl()` | 通过 `WorkerOcServiceCrudParam` 传给各 CRUD handler |
| `src/datasystem/worker/object_cache/service/worker_oc_service_publish_impl.cpp` | `WorkerOcServicePublishImpl::PublishObject()` 成功返回前 | 单 key publish/seal 后，本 worker 有 memory replica |
| `src/datasystem/worker/object_cache/service/worker_oc_service_multi_publish_impl.cpp` | `UpdateObjectAfterCreatingMeta()` 更新成功 key 后 | 批量 publish 后，本 worker 有 memory replica |
| `src/datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.cpp` | `WorkerOcServiceCrudCommonApi::ClearObject()` 成功 erase 后 | 本 worker 删除该 key 的本地 replica |
| `src/datasystem/worker/object_cache/worker_oc_eviction_manager.cpp` | `EvictObject()`、`SpillImpl()`、`DeleteNoneL2CacheEvictableObject()` | eviction/spill 可能绕过 `ClearObject()`，也需要发布位置变化 |
| `src/datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.cpp` | migration 成功生成本地可读副本后 | 数据迁移或恢复后，本 worker 新增 memory replica |

单 key publish 的安全事件点在 `PublishObject()` 完成这些动作之后：

1. master metadata create/update 成功。
2. 非 shm payload 已保存到 memory。
3. object state 已设置为 `OBJECT_PUBLISHED` 或 `OBJECT_SEALED`。
4. `SetPrimaryCopy(true)` 和 `SetCacheInvalid(false)` 已完成。
5. key 已加入 `evictionManager_`。

此时发送：

```json
["BlockUpdateEvent", "key", [["memory", "tcp://worker-ip:port"]]]
```

删除的公共安全事件点是 `ClearObject()` 成功执行 `objectTable_->Erase()` 和 `evictionManager_->Erase()` 之后：

```json
["BlockUpdateEvent", "key", []]
```

注意：`DeleteCopyNotification()`、`DeleteAllCopyWithLock()`、`WorkerOCServiceImpl::DeleteObject()` 最终都会走到 `ClearObject()`，所以优先在公共函数中发删除事件，避免上层重复发布。

### 6.5 分布式 master 下的多 worker 发布

如果 Yuanrong 是 distributed master，并且每个 worker 都维护自己的本地对象驻留状态，那么每个 worker 都应该发布自己的事件。Conductor 侧可以订阅多个 worker 的 PUB endpoint 并汇总。

静态配置示例：

```json
{
  "kvevent_instance": {
    "yuanrong-worker-0": {
      "endpoint": "tcp://10.0.0.11:19997",
      "replay_endpoint": "",
      "type": "Yuanrong",
      "tenant_id": "default",
      "instance_id": "yuanrong-worker-0"
    },
    "yuanrong-worker-1": {
      "endpoint": "tcp://10.0.0.12:19997",
      "replay_endpoint": "",
      "type": "Yuanrong",
      "tenant_id": "default",
      "instance_id": "yuanrong-worker-1"
    }
  }
}
```

聚合规则必须保留 worker 维度：

```go
type ObjectReplicaIndex struct {
    mu sync.RWMutex
    data map[string]map[string][]ReplicaLocation
    // instanceID -> key -> replicas
}
```

或者：

```go
type ObjectReplicaIndex struct {
    mu sync.RWMutex
    data map[string]map[string][]ReplicaLocation
    // key -> instanceID -> replicas
}
```

关键约束：

- 某个 worker 发 `BlockUpdateEvent(key, [])` 时，只删除该 worker 的 key 视图。
- 不能因为某个 worker 删除 key，就删除其他 worker 已发布的同 key replica。
- `RemoveAllEvent` 也只清空当前 Yuanrong instance/source 的视图。
- 如果 worker endpoint 是动态的，需要由控制面更新 Conductor 注册，或让 worker 启停时调用 Conductor register/unregister。

### 6.6 不使用 Disk 的处理

如果 Yuanrong 不希望 Conductor 使用 Disk，那么事件中只发布 `memory`：

```json
["BlockUpdateEvent", "key", [["memory", "tcp://10.0.0.11:31501"]]]
```

不要发布：

```json
["disk", "/path/or/object/location"]
["local_disk", "tcp://worker-ip:port"]
```

状态变化规则：

- key 在本 worker memory 中可读：发布 memory replica。
- key 被删除：发布空 replicas。
- key 被 eviction 从 memory 移除：发布空 replicas。
- key spill 到不对 Conductor 暴露的存储层：发布空 replicas。
- key 从 spill/remote 重新恢复到 memory 且本 worker 可读：重新发布 memory replica。
- write-back 到 L2 但 memory 仍可读时，继续发布 memory replica；只有 memory 被释放或本 worker 不再可读时才发空 replicas。

这样 Conductor 只把 Yuanrong worker 当作 memory replica provider，不会把请求路由到它无法直接使用的磁盘位置。

## 7. 与现有 prefix index 的关系

Yuanrong `BlockUpdateEvent` 不包含 token metadata，因此不能构建：

```text
token_ids -> prefix_hash -> cache hit
```

所以：

- `/query` 的 `longest_matched` 不应该使用 Yuanrong `BlockUpdateEvent` 结果。
- Yuanrong 事件只能支撑对象位置感知。
- 如果未来要支持 prefix-aware routing，必须另起设计，补充 `token_ids`、`block_hashes`、`parent_block_hash`、`block_size` 等上层语义字段。

## 8. 测试计划

### 8.1 Conductor 单元测试

新增或更新：

1. `DecodeYuanrongEventBatch` 正常解析 `BlockUpdateEvent`。
2. `DecodeYuanrongEventBatch` 正常解析 `RemoveAllEvent`。
3. topic `"yuanrong"` 走 Yuanrong decoder。
4. 空 `replay_endpoint` 时 `ZMQClient.Start` 不失败。
5. `BlockUpdateEvent` 更新 `ObjectReplicaIndex`。
6. 空 replicas 删除 key。
7. `RemoveAllEvent` 清空 instance 视图。

### 8.2 Yuanrong 单元测试

1. msgpack payload 解码后等于 `[timestamp, events]`。
2. `BlockUpdateEvent` event array 长度固定为 3。
3. replica 必须是两个 string。
4. seq 使用 8 字节 big-endian。
5. 删除 key 时 replicas 是空数组。

### 8.3 集成测试

1. 启动 Conductor，配置一个 `Yuanrong` instance。
2. 启动 Yuanrong Worker，打开 ZMQ PUB。
3. 写入 key，确认 Conductor `/replica_query` 返回 memory replica。
4. 触发 spill，确认 `/replica_query` 返回 disk 或 memory+disk。
5. 删除 key，确认 `/replica_query` 返回空或 404。

## 9. 推荐落地顺序

1. Conductor 支持 `Yuanrong` service type 和 topic dispatch。
2. Conductor 实现 Yuanrong/Mooncake-current `BlockUpdateEvent` parser。
3. Conductor 将 replay endpoint 改为可选。
4. Conductor 增加 `ObjectReplicaIndex` 和 `/replica_query`。
5. Yuanrong Worker 增加 ZMQ PUB publisher，先不实现 replay。
6. Yuanrong 在 key 副本状态变化点发布完整 `BlockUpdateEvent`。
7. 根据稳定性需求再补 replay endpoint。

