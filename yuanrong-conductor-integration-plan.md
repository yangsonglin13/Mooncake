# Yuanrong KV Cache 亲和性调度方案

## 1. 背景与目标

### 1.1 背景

PyMotor（MindIE-PyMotor）在实现 KV Cache 亲和性调度时，依赖一个外部的 KV Conductor 服务提供三个 HTTP API（`/register`、`/unregister`、`/query`）。当前该服务由 Mooncake Conductor（Go 实现）提供，但出于技术栈统一和减少外部依赖的考虑，需要评估自建方案。

vllm-ascend 通过 `AscendStoreConnector` + `YuanrongBackend` 对接 yuanrong-datasystem 进行 KV Cache 传输。block_hash 和 token_ids 在 vllm-ascend 的 `kv_transfer.py` 层就已生成，与存储后端无关。

### 1.2 目标

- 实现 KV Cache 亲和性调度，将请求路由到缓存命中率最高的 Prefill 实例
- 兼容 PyMotor 的 `ConductorApiClient` API 契约（`/register`、`/unregister`、`/query`）
- 订阅 vLLM Prefill 实例发布的 KV Cache 事件，维护前缀索引
- 支持 KV Cache 亲和性查询，返回最长前缀匹配结果

### 1.3 约束

- PyMotor 的 `ConductorApiClient` 代码不可修改（或仅允许极小改动）
- `/query` 响应超时为 0.2 秒，索引查询必须在此时间内完成
- 需要支持 vLLM 的 ZMQ kv-events 事件格式

---

## 2. 架构现状

### 2.1 vllm-ascend + yuanrong 的集成架构

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 1: AscendStoreConnector (对接 vLLM KVConnectorBase_V1)  │
│           ├── Scheduler 侧: KVPoolScheduler                    │
│           └── Worker 侧: KVPoolWorker                          │
│                                                                 │
│  Layer 2: KVCacheStoreSendingThread / RecvingThread            │
│           ├── 生成 BlockStored 事件（含 block_hash + token_ids）│
│           ├── 构造 PoolKey（含 block_hash hex）                 │
│           └── 调用 Backend.put/get/exists                      │
│                                                                 │
│  Layer 3: Backend (抽象接口)                                    │
│           ├── MooncakeBackend → Mooncake Store                  │
│           ├── YuanrongBackend → Yuanrong HeteroClient           │
│           └── MemcacheBackend → Memcache                        │
└─────────────────────────────────────────────────────────────────┘
```

**关键发现**：`YuanrongBackend` 只在 Layer 3，仅负责数据传输（`put`/`get`/`exists`）。block_hash 和 token_ids 在 Layer 2（`kv_transfer.py`）就已经被提取出来用于生成 KV 事件，与 Backend 无关。

### 2.2 block_hash 和 token_ids 的数据来源

| 数据 | 来源 | 传递路径 |
|------|------|---------|
| `block_hashes` | vLLM KVCacheManager 计算（基于 token_ids + parent_hash 链式哈希） | `Request.block_hashes` → `ReqMeta.block_hashes` → `BlockStored` 事件 |
| `token_ids` | 请求 prompt 的 tokenization 结果 | `Request.prompt_token_ids` → `RequestTracker.token_ids` → `BlockStored` 事件 |
| `parent_block_hash` | 链式哈希的前驱 | `BlockStored` 事件中前一个 block 的 hash |

**核心结论**：block_hash 和 token_ids 由 vLLM 框架生成，yuanrong-datasystem 内部没有这些概念。

### 2.3 Conductor 对 block_hash / token_ids 的处理

Conductor **不直接使用** engine 传来的 block_hash，而是用 token_ids 重新计算自己的 prefix hash（xxhash 链式哈希），并建立映射：

```
engine_block_hash → conductor_prefix_hash
```

查询时也用 token_ids 重新计算哈希链，在 prefixMap 中查找最长前缀匹配。因此 **token_ids 是最核心的数据**，block_hash 只是辅助映射。

---

## 3. 方案对比

### 方案 A：修改 Mooncake Conductor，新增 Yuanrong 事件源支持

**原理**：在 Mooncake Conductor（Go 实现）中新增 `yuanrongParser`，使其能订阅 yuanrong-datasystem 发布的 ZMQ 事件。同时在 yuanrong-datasystem Worker 中新增 ZMQ PUB 事件发布模块。

```
vLLM Prefiller (vllm-ascend)
  │
  │  kv_transfer.py 生成 BlockStored 事件
  │
  ├──→ YuanrongBackend.put() → 数据存入 yuanrong
  │                           │
  │                           ▼
  │                    yuanrong Worker
  │                    ┌─────────────────────────┐
  │                    │  KVEventSystem (新增)     │
  │                    │  ├── KVEventProducer     │
  │                    │  ├── KVEventQueue        │
  │                    │  └── KVEventConsumer     │
  │                    │       ZMQ PUB :29997     │
  │                    │       Topic: "yuanrong"  │
  │                    └─────────────────────────┘
  │                                │
  └──→ vLLM ZMQ PUB :5557         │
        Topic: "kv-events"        │
              │                    │
              ▼                    ▼
        ┌─────────────────────────────────────┐
        │       Mooncake Conductor (Go)       │
        │  vllmParser      yuanrongParser     │
        │  (block_hash +    (key + replicas   │
        │   token_ids)      + block_hash?)    │
        │       │                │            │
        │       ▼                ▼            │
        │  PrefixCacheTable  位置感知索引     │
        └─────────────────────────────────────┘
```

#### 优势

- **全链路缓存感知**：L1（GPU）+ L2（DRAM）+ L3（Disk/L2 Storage）
- 可感知 yuanrong 内部的缓存驱逐、Spill、迁移事件
- 可根据 key 在 DRAM 还是磁盘来优化调度决策
- 跨 Worker 的副本位置感知
- 复用 Mooncake Conductor 已有的成熟框架（ZMQ 订阅、前缀索引、HTTP API）

#### 劣势

- **yuanrong-datasystem 需要新增 ZMQ PUB 模块**，工作量中等
- **Conductor 代码需合入 Mooncake 社区**，推动难度大：
  - 社区可能认为 yuanrong Parser 不属于 Mooncake 核心功能
  - 需要长期维护 yuanrong 事件格式的兼容性
  - 社区可能要求更通用的插件化方案而非硬编码新 Parser
- **block_hash / token_ids 数据来源问题**：yuanrong-datasystem 内部没有这些概念
  - 如果只发布 `BlockUpdateEvent`（key + replicas），Conductor 无法构建前缀索引
  - 如果要发布 `BlockStoreEvent`（含 block_hash + token_ids），需要扩展 KV 接口
- **技术栈不一致**：Conductor 是 Go 实现，yuanrong 团队维护成本高

#### block_hash / token_ids 的三种获取策略

| 策略 | 说明 | 代价 | 效果 |
|------|------|------|------|
| **A1. 扩展 KV 接口传入** | 在 `SetParam` 中新增 `token_ids` 和 `block_hashes` 字段，由上层调用方传入 | 修改 KVClient API + Worker 处理逻辑 | **完整前缀索引** |
| **A2. 仅发布位置事件** | 只发布 `BlockUpdateEvent`（key + replicas），不含 block_hash/token_ids | 最小改动 | **仅位置感知**，无前缀匹配 |
| **A3. Worker 端计算** | Worker 在 Publish 时根据对象数据计算 hash 和提取 token_ids | 需了解 KV cache 数据格式，增加计算开销 | 可行但侵入性强 |

**推荐 A1**：Conductor 的核心价值在于前缀哈希索引，如果只有位置信息（A2），Conductor 退化为简单的 key→location 查询，失去 KVCache-Aware 调度的核心能力。

#### 实现步骤

**yuanrong-datasystem 侧**：

1. 新增 KV Event 发布模块（参照 Mooncake Store 的 `kv_event/` 目录）：

```
src/datasystem/worker/object_cache/kv_event/
├── kv_event.h                  # 事件基类 + 具体事件类型
├── kv_event_types.h            # 队列项类型定义
├── kv_event_producer.h/.cpp    # 事件生产者（异步入队）
├── kv_event_consumer.h/.cpp    # 事件消费者（ZMQ 发布）
├── kv_event_publisher_config.h # 发布配置
└── kv_event_system.h/.cpp      # 门面类
```

2. 在 `WorkerOcEvictionManager` 的 `Add()` / `Erase()` 中注入事件发布

3. 在 `WorkerOcServicePublishImpl`、`WorkerOcServiceGetImpl` 等关键路径中注入事件

4. 新增启动参数（`-enable_kv_event_publish` 等）

5. （策略 A1）扩展 `SetParam` / `CreateParam` 新增 `token_ids` 和 `block_hashes` 字段

**Conductor 侧**（需合入 Mooncake 社区）：

| 文件 | 修改内容 |
|------|---------|
| `common/types.go` | 新增 `ServiceTypeYuanrong` 常量 |
| `zmq/event_type.go` | 新增 `SourceYuanrong` 常量 |
| `zmq/msg_decoder.go` | 新增 `yuanrongParser` + `DecodeYuanrongEventBatch` |
| `zmq/zmq_client.go` | Topic 路由新增 `"yuanrong"` 分支 |
| `kvevent/event_handler.go` | 新增 `BlockUpdateEvent` 处理 + `handleBlockUpdate` 方法 |
| `main.go` | `mapServiceType` 新增 `"Yuanrong"` 映射 |

> ⚠️ **合入风险**：Conductor 是 Mooncake 社区项目，yuanrong 相关代码需要向 Mooncake 社区提 PR 合入。由于 yuanrong 是外部系统，社区可能对合入第三方专有 Parser 存在顾虑，推动难度较大。可能的阻力包括：
> - 社区可能认为 yuanrong Parser 不属于 Mooncake 核心功能
> - 需要长期维护 yuanrong 事件格式的兼容性
> - 社区可能要求更通用的插件化方案而非硬编码新 Parser
>
> **替代方案**：如果社区不接受 yuanrong Parser，可考虑复用 `"mooncake"` Topic + `mooncakeParser`（即 yuanrong 发布的事件格式完全对齐 Mooncake Store），这样 Conductor 零改动。

#### 工作量

| 组件 | 改动量 |
|------|--------|
| yuanrong-datasystem | ~1000-1500 行新增代码（C++） |
| Mooncake Conductor | ~150 行（需合入社区，推动难度大） |
| vllm-ascend | 无（策略 A1 需扩展 YuanrongBackend 传参） |

---

### 方案 B：Yuanrong 自建 KV Conductor（Python 实现）— 推荐 ✅

**原理**：Yuanrong 自行实现一个轻量级 Python 版 KV Conductor 服务，兼容 PyMotor 的 `ConductorApiClient` API 契约。直接订阅 vLLM Prefill 实例的 ZMQ kv-events，无需修改 yuanrong-datasystem 的 C++ 代码，无需向 Mooncake 社区提 PR。

```
┌──────────────────────────────────────────────────────────────────┐
│              Yuanrong KV Conductor Service                        │
│              (独立 Python 进程)                                    │
│                                                                  │
│  ┌──────────────┐  ┌──────────────────┐  ┌───────────────────┐  │
│  │ HTTP Server   │  │ Prefix Indexer   │  │ ZMQ Subscriber    │  │
│  │ (FastAPI)     │  │ (前缀哈希索引)    │  │ (kv-events 订阅)  │  │
│  │              │  │                  │  │                   │  │
│  │ /register   │  │ - 链式哈希计算    │  │ - ZMQ SUB socket  │  │
│  │ /unregister │──│ - proxyHashMapping│──│ - 事件解码         │  │
│  │ /query      │  │ - CacheHitCompute │  │ - 断连重连         │  │
│  │              │  │ - LRU 淘汰       │  │ - 事件回放         │  │
│  └──────┬───────┘  └──────────────────┘  └────────┬──────────┘  │
│         │                                          │            │
└─────────┼──────────────────────────────────────────┼────────────┘
          │ HTTP                                     │ ZMQ SUB
          │                                          │
  ┌───────┴───────┐                        ┌─────────▼──────────┐
  │ PyMotor       │                        │ vLLM Prefill       │
  │ Coordinator   │                        │ (kv-events ZMQ PUB)│
  │ (Scheduler)   │                        │                    │
  └───────────────┘                        └────────────────────┘
```

#### 优势

- **不依赖 Mooncake Conductor**，无需向 Mooncake 社区提 PR，无合入风险
- **不修改 yuanrong-datasystem C++ 代码**，直接订阅 vLLM 的 ZMQ kv-events
- **技术栈统一**：Python 实现，与 PyMotor 一致，团队维护成本低
- **完整的前缀哈希索引**：block_hash + token_ids 由 vLLM 框架通过 kv-events 提供
- **兼容 PyMotor API 契约**：`/register`、`/unregister`、`/query` 接口完全兼容
- **开发效率高**：核心逻辑从 Go 移植到 Python，~1100 行代码

#### 劣势

- 只能感知 **L1（GPU/推理引擎层）** 的缓存状态
- 无法感知 yuanrong 内部的多级缓存状态：
  - 哪些 key 在 DRAM 中（快速可取）
  - 哪些 key 被 Spill 到磁盘（需要加载）
  - 哪些 key 在 L2 持久化存储中
  - 缓存驱逐/迁移事件
- 调度决策可能不够精确（不知道 key 在 yuanrong 中的实际存储层级）

#### 核心模块设计

##### 项目结构

```
yuanrong-kv-conductor/
├── conductor/
│   ├── __init__.py
│   ├── main.py              # FastAPI 入口，HTTP API 定义
│   ├── indexer.py            # 前缀索引引擎（核心）
│   ├── zmq_subscriber.py     # ZMQ 事件订阅管理
│   ├── event_decoder.py      # KV 事件解码器
│   └── config.py             # 配置管理
├── requirements.txt
└── start.sh
```

##### 前缀索引引擎 (`indexer.py`)

移植自 Mooncake Conductor 的 `prefix_indexer.go`，核心数据结构和算法保持一致：

```
PrefixCacheTable (顶层入口)
  └── context_map: dict[str → ContextData]
        ├── prefix_map: dict[int → CacheStoreInfo]    # conductor hash → 缓存信息
        ├── proxy_hash_mapping: dict[int → int]        # engine block hash → conductor prefix hash
        ├── dp_size: dict[int → set]                   # dp_rank 集合
        ├── seed: int                                  # 哈希种子
        └── LRU 双向链表 (用于 CPU 缓存淘汰)
```

核心算法 — 链式前缀哈希：

```python
def compute_hash(parent_hash: int, block_token_ids: list[int], seed: int) -> int:
    digest = xxhash.xxh64(seed=seed)
    digest.update(parent_hash.to_bytes(8, 'little'))
    for token_id in block_token_ids:
        digest.update(token_id.to_bytes(4, 'little'))
    return digest.intdigest()
```

##### HTTP API (`main.py`)

兼容 PyMotor `ConductorApiClient` 的三个端点：

| 端点 | 方法 | 超时 | 功能 |
|------|------|------|------|
| `/register` | POST | 2s | 注册 Prefill 实例，创建 ZMQ 订阅 |
| `/unregister` | POST | 2s | 注销实例，停止 ZMQ 订阅 |
| `/query` | POST | **0.2s** | 查询缓存命中率，返回最长前缀匹配结果 |

`/query` 响应格式（必须严格遵循）：

```json
{
  "default": {
    "vllm-prefill-1": {
      "longest_matched": 256,
      "DP": {"0": 256, "1": 128}
    }
  }
}
```

##### ZMQ 事件订阅器 (`zmq_subscriber.py`)

- 每个 Prefill 实例对应一个 `ZMQSubscriber`，在独立线程中运行
- 支持 ZMQ SUB + DEALER（Replay）双 Socket
- 断线自动重连 + 序列号间隙检测

##### 事件解码器 (`event_decoder.py`)

支持 vLLM、Mooncake、Yuanrong 三种事件格式，移植自 Mooncake Conductor 的 `msg_decoder.go`。

#### 技术选型

| 组件 | 选型 | 理由 |
|------|------|------|
| 语言 | Python 3.11+ | 与 PyMotor 技术栈一致；开发效率高；性能满足 0.2s 查询要求 |
| HTTP 框架 | FastAPI + Uvicorn | 异步高性能；自动 OpenAPI 文档；PyMotor 已使用 |
| ZMQ 客户端 | pyzmq | 成熟稳定；PyMotor 已使用 |
| 哈希算法 | xxhash (python-xxhash) | 与 Mooncake Conductor 一致；高性能非加密哈希 |
| 序列化 | msgpack | 与 vLLM kv-events 格式一致 |

#### 工作量

| 模块 | 代码量 | 说明 |
|------|--------|------|
| 前缀索引引擎 (`indexer.py`) | ~300 行 | 核心逻辑，移植 Go → Python |
| HTTP API (`main.py`) | ~100 行 | 3 个端点 |
| ZMQ 订阅器 (`zmq_subscriber.py`) | ~200 行 | SUB/DEALER 模式 + 重连 + 回放 |
| 事件解码器 (`event_decoder.py`) | ~150 行 | 移植 msg_decoder.go |
| 配置管理 (`config.py`) | ~30 行 | 环境变量读取 |
| PyMotor 侧适配 | ~20 行 | 部署脚本和配置模板 |
| 单元测试 | ~300 行 | 索引引擎 + API + 解码器 |
| **合计** | **~1100 行** | |

#### 依赖清单

```
# requirements.txt
fastapi>=0.100.0
uvicorn>=0.23.0
pyzmq>=25.0.0
python-xxhash>=3.0.0
msgpack>=1.0.0
pydantic>=2.0.0
```

---

### 方案 C：Yuanrong 自建全链路感知 KV Conductor（Python + yuanrong-events）

**原理**：在方案 B 的 Python Conductor 基础上，继续保留 vLLM `kv-events` 作为 `block_hash + token_ids` 的前缀索引来源；同时在 `yuanrong-datasystem` Worker 内新增 `yuanrong-events` 事件发布链路，把对象在 **内存 / Spill 磁盘 / L2 持久化 / 跨 Worker 迁移** 之间的状态变化实时推送给同一个 Conductor。Conductor 汇聚两路事件后，在不依赖 Mooncake Conductor 的前提下实现全链路缓存感知调度。

```
┌───────────────────────────────────────────────────────────────────────┐
│           Yuanrong Full-Path KV Conductor Service (Python)           │
│                                                                       │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐  │
│  │ Prefix Index    │  │ Block Binding  │  │ Tier State Index       │  │
│  │ - token_ids     │  │ - block_hash   │  │ - memory / spill / L2  │  │
│  │ - parent_hash   │  │ ↔ object_key   │  │ - primary / replica    │  │
│  │ - longest match │  │ ↔ prefix_hash  │  │ - migrate / evict      │  │
│  └────────┬───────┘  └────────┬───────┘  └────────────┬───────────┘  │
│           └────────────────────┴───────────────────────┘              │
│                                │                                      │
│                        Query Policy Engine                            │
│                - 计算 effective_longest_matched                       │
│                - 兼容 PyMotor /query 契约                             │
└───────────────┬───────────────────────────────┬───────────────────────┘
                │                               │
        vLLM kv-events                     yuanrong-events
                │                               │
┌───────────────▼──────────────┐   ┌────────────▼──────────────────────┐
│ vLLM Prefill / kv_transfer    │   │ yuanrong-datasystem Worker        │
│ - BlockStored / BlockRemoved  │   │ - Publish / Get / Spill / Migrate │
│ - block_hash + token_ids      │   │ - Evict / L2 fallback / Replica   │
└───────────────────────────────┘   └───────────────────────────────────┘
```

#### 优势

- **不依赖 Mooncake 社区**，仍然复用方案 B 的 Python Conductor、HTTP API 和 ZMQ 基础设施
- **保留完整前缀哈希索引**，继续使用 vLLM 原生 `block_hash + token_ids`
- **补齐全链路状态感知**，可区分对象当前处于内存、Spill 磁盘、L2 持久化还是迁移中
- **调度结果更贴近真实可用性**，避免“逻辑上命中，但实际只在 disk/L2，需要额外加载”的误判
- **可增量演进**，方案 B 的 Prefix Index、Subscriber、HTTP API 均可直接复用

#### 劣势

- 需要修改 `yuanrong-datasystem` 的 C++ 代码并长期维护事件协议
- 需要建立 `block_hash ↔ object_key ↔ prefix_hash` 关联，事件融合逻辑明显复杂于方案 B
- 为兼容 PyMotor 固定的 `/query` 契约，需要把多级介质状态折算成有效命中分值

#### 核心设计

##### 双事件源融合

1. **vLLM `kv-events`**：继续负责 `BlockStored / BlockRemoved / AllBlocksCleared`，提供前缀匹配所需的 `block_hash`、`parent_block_hash`、`token_ids`
2. **`yuanrong-events`**：新增对象生命周期事件，负责反映对象介质层级和副本位置
3. **关联键策略**：优先复用 `PoolKey` 中已有的 `block_hash hex` 反解 `object_key ↔ block_hash`；若后续 key 编码存在变动风险，再补充显式 `BlockBindingEvent`

##### Conductor 新增索引

| 模块 | 作用 |
|------|------|
| `PrefixIndex` | 延续方案 B，负责 `token_ids → prefix_hash → longest matched` |
| `BlockBindingIndex` | 维护 `engine_block_hash ↔ object_key ↔ conductor_prefix_hash` |
| `TierStateIndex` | 维护 `object_key → {worker, primary, replicas, medium, version, state}` |
| `PolicyEngine` | 将逻辑命中长度和介质层级折算为 `effective_longest_matched` |

##### 建议状态模型

| 状态 | 含义 | 典型来源 |
|------|------|---------|
| `MEMORY_READY` | 对象已在 Worker 内存中，可直接命中 | `PublishObject()`、`LoadSpilledObjectToMemory()`、L2 回填成功 |
| `SPILLED` | 对象已从内存降级到 Spill 文件 | `SpillImpl()` 成功后 |
| `L2_ONLY` | 对象不在本地内存，但可从 L2 持久化拉起 | `GetObjectFromPersistenceAndDumpWithoutCopyMeta()` 前后 |
| `MIGRATING` | 对象正在跨 Worker 迁移或切主副本 | `MigrateData()` / `ReplacePrimaryImpl()` |
| `INVALID` | 对象被驱逐、删除或副本失效 | `EvictObject()`、`RemoveLocation()`、回滚失败 |

##### yuanrong-datasystem 侧事件注入点

| 场景 | 代码落点 | 说明 |
|------|---------|------|
| Publish 成功后对象进入内存 | `WorkerOcServicePublishImpl::PublishObject()` | 对象设为 `PrimaryCopy=true`、`CacheInvalid=false` 后发布 ready 事件 |
| Spill 文件回填内存 | `LoadSpilledObjectToMemory()` | 对象从磁盘重新提升到内存 |
| L2 持久化回填内存 | `WorkerOcServiceGetImpl::GetObjectFromPersistenceAndDumpWithoutCopyMeta()` | 对象从 L2 拉起并重新加入 eviction list |
| 驱逐触发 spill | `WorkerOcEvictionManager::SpillImpl()` | 对象内存被释放，状态切换为 `SPILLED` |
| 驱逐删除 / EndLife | `WorkerOcEvictionManager::EvictObject()` | 对象副本被删除或生命周期结束 |
| 元数据摘除 | `WorkerOcServiceGetImpl::RemoveLocation()` | Worker 副本位置从元数据中移除 |
| 跨 Worker 迁移 / 切主 | `WorkerOcServiceMigrateImpl::FillOneObjectLocked()`、`ReplacePrimaryImpl()` | 更新 primary / replica / version 关系 |

##### `/query` 兼容策略

- 对外仍保持 PyMotor 现有响应结构：`tenant -> instance_id -> { longest_matched, DP }`
- 对内增加两套口径：
  - `raw_longest_matched`：纯前缀命中长度
  - `effective_longest_matched`：结合 `MEMORY_READY / SPILLED / L2_ONLY / MIGRATING` 折算后的有效命中长度
- 默认将 `effective_longest_matched` 回填到 `/query.longest_matched`，并可选附加调试字段（PyMotor 会忽略额外字段）

#### 工作量

| 组件 | 改动量 |
|------|--------|
| yuanrong-datasystem | ~800-1200 行新增代码（事件发布链路 + 注入点） |
| Yuanrong Conductor | ~400-600 行 Python 增量代码（Binding / TierState / Policy） |
| vllm-ascend | 0-100 行（仅当需要显式 `BlockBindingEvent` sideband 时） |

---

## 4. 决策矩阵

| 维度 | 方案 A（修改 Mooncake Conductor） | 方案 B（自建 Python Conductor）✅ | 方案 C（自建全链路感知 Conductor） |
|------|----------------------------------|--------------------------------|-----------------------------------|
| **Conductor 感知范围** | L1 + L2 + L3（全链路） | L1（GPU 层） | L1 + L2 + L3（全链路） |
| **前缀哈希索引** | 取决于策略（A1:有, A2:无） | ✅ 有（vLLM kv-events 提供 token_ids） | ✅ 有（复用方案 B 的 PrefixIndex） |
| **多级缓存位置感知** | ✅ 有 | ❌ 无 | ✅ 有 |
| **驱逐/Spill 感知** | ✅ 有 | ❌ 无 | ✅ 有 |
| **迁移 / 主副本感知** | ✅ 有 | ❌ 无 | ✅ 有 |
| **yuanrong C++ 改动量** | ~1000-1500 行新增代码 | **零改动** | ~800-1200 行新增代码 |
| **Mooncake 社区合入** | 需合入，推动难度大 | **无需合入** | **无需合入** |
| **技术栈一致性** | Go（与 yuanrong/PyMotor 不一致） | Python（与 PyMotor 一致） | Python + C++（与现有 Yuanrong 体系一致） |
| **实现周期** | 2-3 周 + 社区合入周期 | 1-2 周 | 2-4 周 |
| **风险** | 中（社区合入风险 + C++ 改动风险） | 低 | 中（事件融合 + C++ 改动风险） |
| **调度精度** | 高（知道多级缓存） | 中（只知道 GPU 缓存） | 高（知道多级缓存且不依赖 Mooncake） |
| **可扩展性** | 依赖 Mooncake 社区演进 | 自主可控，可按需扩展 | 自主可控，适合作为长期目标架构 |

---

## 5. 推荐路径：先方案 B，后演进到方案 C

### 5.1 近期先落方案 B 的理由

1. **零外部依赖**：不依赖 Mooncake Conductor，无需向社区提 PR，无合入风险
2. **零 C++ 改动**：不修改 yuanrong-datasystem 代码，能最快验证 PyMotor 亲和性调度闭环
3. **技术栈统一**：Python 实现，与 PyMotor 一致，团队维护成本低
4. **快速交付**：~1100 行代码，1-2 周可完成
5. **核心能力完整**：vLLM kv-events 已提供完整的 `block_hash + token_ids`，前缀索引功能与 Mooncake Conductor 等价

### 5.2 从方案 B 演进到方案 C 的理由

1. **演进成本可控**：方案 C 可直接复用方案 B 的 HTTP API、PrefixIndex、Subscriber 和部署方式
2. **代码落点明确**：`yuanrong-datasystem` 中已存在清晰的 Publish / Get / Spill / Migrate / Evict 生命周期入口
3. **精度收益明显**：可区分“内存已命中”和“仅在 disk/L2 可恢复”的差异，减少误选 Prefill 实例
4. **长期更合理**：既能摆脱 Mooncake 社区耦合，又能获得全链路缓存感知能力

### 5.3 实施路径

```
第一阶段（1-2 周）：落地方案 B
  ├── 实现 PrefixCacheTable（移植 Go → Python）
  ├── 实现 HTTP API（/register, /unregister, /query）
  ├── 实现 ZMQ Subscriber（订阅 vLLM kv-events）
  ├── 实现 Event Decoder（vLLM 格式）
  └── 单元测试 + 集成测试

第二阶段（2-4 周）：演进到方案 C
  ├── 在 worker_oc_service_publish_impl / worker_oc_service_get_impl / worker_oc_eviction_manager / worker_oc_service_migrate_impl 注入 yuanrong-events
  ├── Conductor 新增 BlockBindingIndex / TierStateIndex / PolicyEngine
  ├── 建立 object_key ↔ block_hash ↔ prefix_hash 关联
  └── /query 输出按层级折算后的 effective_longest_matched
```

### 5.4 PyMotor 侧适配

#### instance_id 命名约定（PyMotor 零改动）

PyMotor 的 `ConductorApiClient` 中 `instance_id` 硬编码为 `"vllm-prefill-{instance.id}"` 格式。Yuanrong Conductor 在 `/query` 响应中直接使用此格式，PyMotor **完全不需要改动**。

#### 部署脚本适配

将 `kv_conductor.sh` 中的 `mooncake_conductor` 替换为 Yuanrong Conductor：

```bash
# 替换为 Yuanrong KV Conductor
python3 -m conductor.main --config "$CONDUCTOR_CONFIG_PATH"
```

#### vLLM Prefill 实例配置

```json
{
  "motor_engine_prefill_config": {
    "engine_type": "vllm",
    "engine_config": {
      "kv-events-config": {
        "publisher": "zmq",
        "enable_kv_cache_events": true,
        "endpoint": "tcp://*:5557",
        "topic": "kv-events",
        "replay_endpoint": "tcp://*:6667"
      },
      "kv_transfer_config": {
        "kv_connector": "AscendStoreConnector",
        "kv_role": "kv_producer",
        "kv_connector_extra_config": {
          "backend": "yuanrong"
        }
      }
    }
  }
}
```

---

## 6. 风险与注意事项

### 6.1 性能风险

- `/query` 超时仅 0.2 秒，前缀索引查询必须在此时限内完成
- 对于长序列（数千 token_ids），链式哈希计算和索引查找需要优化
- 建议：对 `token_ids` 长度设上限（如只取前 N 个 block 的 token_ids 参与查询）

### 6.2 一致性风险

- ZMQ 事件可能丢失或乱序，导致索引与实际缓存不一致
- 缓解措施：ZMQ DEALER 回放机制 + 序列号间隙检测
- 影响：不一致时亲和性调度可能选择次优实例，但不会导致功能故障（最差情况退化为普通负载均衡）

### 6.3 instance_id 命名耦合

- PyMotor 硬编码了 `"vllm-prefill-"` 前缀，如果未来需要支持其他引擎类型，需要修改 PyMotor 代码
- 建议：长期推动 PyMotor 将 instance_id 格式改为可配置

### 6.4 内存管理

- 前缀索引可能随时间增长，需要 LRU 淘汰机制控制内存使用
- 需要监控 `prefix_map` 大小，设置合理的 `MAX_CPU_KEY_NUM`

### 6.5 方案 C 的事件融合风险

- 若 `PoolKey` 不能稳定反解出 `block_hash hex`，则必须补充显式 `BlockBindingEvent`，否则无法把前缀索引和 yuanrong 内部对象状态关联起来
- `yuanrong-events` 与 vLLM `kv-events` 存在天然异步，必须引入 `version / timestamp / seq` 去重和乱序处理
- `/query.longest_matched` 若切换为 `effective_longest_matched`，需要明确监控口径，避免与“纯原始命中长度”混淆

---

## 7. 附录

### A. PyMotor API 契约（不可变更）

#### POST `/register`

**请求体**：

```json
{
  "endpoint": "tcp://10.0.0.1:5557",
  "type": "vLLM",
  "modelname": "Qwen-72B",
  "block_size": 128,
  "instance_id": "vllm-prefill-1",
  "dp_rank": 0,
  "replay_endpoint": "tcp://10.0.0.1:6667",
  "tenant_id": "default"
}
```

**响应**：仅需 2xx 状态码。

#### POST `/unregister`

**请求体**：

```json
{
  "type": "vLLM",
  "modelname": "Qwen-72B",
  "block_size": 128,
  "instance_id": "vllm-prefill-1",
  "dp_rank": 0,
  "tenant_id": "default"
}
```

**响应**：仅需 2xx 状态码。

#### POST `/query`

**请求体**：

```json
{
  "model": "Qwen-72B",
  "block_size": 128,
  "token_ids": [1, 234, 5678, 90, 12, 3456]
}
```

**响应体（必须严格遵循）**：

```json
{
  "default": {
    "vllm-prefill-1": {
      "longest_matched": 256,
      "DP": {"0": 256, "1": 128}
    }
  }
}
```

**PyMotor 的选择逻辑**：
1. 通过 `rsp.get("default", None)` 获取租户数据
2. 遍历所有实例，选择 `longest_matched` 值最大的实例
3. 在选中实例的 `DP` 字典中，选择值最大的 endpoint（dp_rank）

### B. yuanrong-datasystem 事件注入点（方案 A / C 扩展用）

| 操作 | 注入位置 | 事件类型 | 说明 |
|------|---------|---------|------|
| Publish 成功 | `WorkerOcServicePublishImpl::PublishObject()` 后 | `BlockStoreEvent` / `ObjectStateReadyEvent` | 对象进入内存；若 write-through，可同时标记 L2 可用 |
| Spill 回填内存 | `LoadSpilledObjectToMemory()` 成功后 | `BlockUpdateEvent` / `ObjectStatePromoteEvent` | 对象从 Spill 文件重新提升到内存 |
| L2 回填内存 | `WorkerOcServiceGetImpl::GetObjectFromPersistenceAndDumpWithoutCopyMeta()` 成功后 | `BlockUpdateEvent` / `ObjectStatePromoteEvent` | 对象从 L2 拉起并重新加入 eviction list |
| 迁移数据写入 | `WorkerOcServiceMigrateImpl::FillOneObjectLocked()`、`ReplacePrimaryImpl()` 成功后 | `BlockStoreEvent` / `ObjectReplicaUpdateEvent` | 更新 primary / replica / version 关系 |
| 元数据摘除 | `WorkerOcServiceGetImpl::RemoveLocation()` | `BlockUpdateEvent` / `ObjectStateRemoveEvent` | 从 master 元数据中移除本 Worker 副本 |
| 驱逐删除 | `WorkerOcEvictionManager::EvictObject()` | `BlockUpdateEvent` / `ObjectStateRemoveEvent` | DELETE / END_LIFE 后副本失效 |
| 驱逐 Spill | `WorkerOcEvictionManager::SpillImpl()` | `BlockUpdateEvent` / `ObjectStateDemoteEvent` | 对象从内存降级到 Spill 文件 |
| 副本变更 | `OCNotifyWorkerManager` 通知时 | `BlockUpdateEvent` / `ObjectReplicaUpdateEvent` | 跨 Worker 副本集合变化 |

### C. ZMQ 消息协议规范

vLLM kv-events 发布的 ZMQ 消息格式：

```
三帧 ZMQ 消息：
[Frame 1] Topic: "kv-events" (bytes)
[Frame 2] Seq: 8 bytes big-endian 无符号整数（单调递增序列号）
[Frame 3] Payload: MsgPack 编码的事件批次
```

vLLM MsgPack Payload 格式（3 元素数组）：

```
[timestamp, [event1, event2, ...], dp_rank]
```

BlockStored 事件格式：

```
["BlockStored", block_hashes, parent_block_hash, token_ids, block_size, lora_id, medium, lora_name]
```

BlockRemoved 事件格式：

```
["BlockRemoved", block_hashes, medium]
```

AllBlocksCleared 事件格式：

```
["AllBlocksCleared"]
```

Replay 机制：

- ROUTER Socket 端口 = PUB 端口 + 1
- DEALER 发送 `[from_seq]`（8 字节大端）
- ROUTER 响应重放的事件序列
