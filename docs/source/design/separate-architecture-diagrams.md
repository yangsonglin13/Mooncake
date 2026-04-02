# Mooncake Store 与 Yuanrong Datasystem 架构图

## 1. Mooncake Store 架构图

```mermaid
flowchart LR
  subgraph App["推理应用 / Serving Engine"]
    A1["vLLM / SGLang"]
    A2["Hot Local Cache\nGPU / Host 热缓存"]
    A3["Mooncake Client"]
  end

  subgraph Control["控制面"]
    M["Master Service\n元数据 / 空间分配 / Lease / Eviction"]
  end

  subgraph Pool["Global Cache Pool"]
    C1["Store Client / Segment Node\nDRAM / CXL Segment"]
    C2["Store Client / Segment Node\nDRAM / CXL Segment"]
    C3["Store Client / Segment Node\nDRAM / CXL Segment"]
    SSD["SSD / DFS Offload"]
  end

  subgraph TE["Transfer Engine"]
    T1["RDMA / TCP / CXL / Ascend"]
    T2["Metadata Service\netcd / Redis / HTTP"]
  end

  A1 --> A2
  A1 --> A3
  A3 -->|"Query / PutStart / PutEnd / Ping"| M
  A3 -->|"cache miss 后读写远端对象"| C1
  A3 -->|"cache miss 后读写远端对象"| C2
  A3 -->|"cache miss 后读写远端对象"| C3
  C1 <--> T1
  C2 <--> T1
  C3 <--> T1
  T1 -.-> T2
  M -. "segment mount / replica metadata" .-> C1
  M -. "segment mount / replica metadata" .-> C2
  M -. "segment mount / replica metadata" .-> C3
  M -. "offload / eviction" .-> SSD
```

要点：

- `Mooncake Store` 是 `Hot Local Cache + Global Cache Pool` 的分层设计。
- 控制面由 `Master Service` 集中负责。
- 数据面通过 `Transfer Engine` 在各 `Store Client / Segment Node` 之间传输。
- `master` 不直接搬数据，但所有对象元数据、空间分配、lease、eviction 都先经过它。

## 2. Yuanrong Datasystem 架构图

```mermaid
flowchart LR
  subgraph SDK["应用进程 / SDK"]
    Y1["Yuanrong SDK\nKV / Object / Heterogeneous Object"]
    Y2["ReadOnlyBuffer / Buffer View"]
    Y3["MmapManager"]
  end

  subgraph Local["本地 Worker"]
    W1["Worker"]
    W2["Unified Memory Pool\nHBM / DRAM / SSD"]
    W3["Local Metadata Owner"]
    W4["Replica / Spill / Migration / Eviction"]
    W5["Device Object Manager\nH2D / D2H / DevPublish"]
  end

  subgraph Cluster["集群元数据与调度"]
    E["ETCD\n节点发现 / 健康检查"]
    H["Hash Ring\n元数据分布 / 扩缩容重分布"]
    R["Metadata Redirect\n迁移期重定向"]
  end

  subgraph Remote["远端 Worker 集群"]
    RW1["Remote Worker"]
    RW2["Remote Worker"]
    RW3["Remote Worker"]
  end

  Y1 --> W1
  W1 --> W3
  Y3 <-->|"shared memory mmap"| W2
  Y2 <-->|"zero-copy read / write"| W2
  W1 --- W2
  W1 --- W4
  W5 --- W2
  Y1 <--> W5
  W3 <--> H
  H --- E
  R --- H
  W4 <--> RW1
  W4 <--> RW2
  W4 <--> RW3
```

要点：

- `Yuanrong` 是统一内存池设计，`HBM / DRAM / SSD` 统一纳入 worker 管理。
- SDK 和同节点 worker 之间直接通过共享内存 `mmap` 做本地零拷贝读写。
- 元数据支持按 `Hash Ring` 分布式化，没有固定中心 `master` 成为热点读写瓶颈。
- 元数据迁移期通过 `Metadata Redirect` 把请求导向新的 owner。
