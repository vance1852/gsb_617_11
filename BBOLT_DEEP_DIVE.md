# etcd-io/bbolt 深度代码分析

> 基于 bbolt 源码（commit 对应 `internal/` 目录与顶层文件现状），以下所有引用均可按给定文件路径与函数名直接核对。

---

## 目录

1. [事务生命周期与 MVCC 并发模型](#1-事务生命周期与-mvcc-并发模型)
2. [磁盘页（Page）与元数据（Meta）布局及双 Meta 崩溃恢复](#2-磁盘页page与元数据meta布局及双-meta-崩溃恢复)
3. [读写事务 Commit 完整流程与崩溃一致性](#3-读写事务-commit-完整流程与崩溃一致性)
4. [B+ 树节点 spill（分裂落盘）与 rebalance（合并/再平衡）](#4-b-树节点-spill分裂落盘与-rebalance合并再平衡)
5. [Freelist 的两种实现：Array vs HashMap，以及 pending 页延迟释放](#5-freelist-的两种实现array-vs-hashmap以及-pending-页延迟释放)
6. [mmap 按需重映射与 Cursor 栈式遍历](#6-mmap-按需重映射与-cursor-栈式遍历)
7. [关键隐式不变量与风险点](#7-关键隐式不变量与风险点)

---

## 1. 事务生命周期与 MVCC 并发模型

### 1.1 DB 层的三把锁与全局状态

[db.go](file:///e:/gsb/617/gsb_11/Earth/db.go) 中 `DB` 结构体持有 4 把互斥/读写锁，构成了整个并发控制的骨架：

```go
rwlock   sync.Mutex   // 全局写锁：同一时刻至多一个读写事务
metalock sync.Mutex   // 保护 meta 页的读写
mmaplock sync.RWMutex // 保护 mmap 区域，重映射时需写锁
statlock sync.RWMutex // 统计数据
```

此外 `DB.rwtx *Tx` 记录当前活跃的写事务指针（只读事务不记录在此）。

### 1.2 只读事务（Begin(writable=false) → beginTx）

开启路径：[db.go:792-837](file:///e:/gsb/617/gsb_11/Earth/db.go#L792-L837) `beginTx()`。

完整生命周期：

| 阶段 | 代码位置 | 动作 |
|---|---|---|
| **加锁** | `db.metalock.Lock()` → `db.mmaplock.RLock()` | 先 metalock（与写事务同序），再加 mmap 读锁阻止 remap |
| **拷贝 meta** | [tx.go:47-65](file:///e:/gsb/617/gsb_11/Earth/tx.go#L47-L65) `Tx.init()` | 将 `db.meta()` 深拷贝到 `tx.meta`，复制 `root` bucket header。**只读 tx 不递增 txid**，直接使用当前已提交的 meta.txid 作为自己的快照 id |
| **注册只读 txid** | [db.go:822](file:///e:/gsb/617/gsb_11/Earth/db.go#L822) `db.freelist.AddReadonlyTXID(t.meta.Txid())` | 把自己的 txid 登记进 freelist，用于保护 pending 页 |
| **解锁 metalock** | `db.metalock.Unlock()` | 此后不阻塞新的写事务开启 |
| **读操作** | 通过 `tx.page(id)` 读页：优先查 `tx.pages`（只读 tx 的 pages 永远为 nil），否则直接返回 `db.page(id)`（即 mmap 指针） | 零拷贝直接读取 mmap，不加锁 |
| **结束（Rollback）** | [tx.go:302-308](file:///e:/gsb/617/gsb_11/Earth/tx.go#L302-L308) → `close()` → [db.go:875-896](file:///e:/gsb/617/gsb_11/Earth/db.go#L875-L896) `removeTx()` | 释放 mmap RLock；在 metalock 下调用 `freelist.RemoveReadonlyTXID(txid)` 注销自己 |

**要点：** 只读事务一旦通过 `beginTx` 完成 `meta` 拷贝，就持有该 meta 的一致快照。所有页指针直接指向 mmap 内存，所以它必须持有 `mmaplock.RLock()` 直到关闭——这就是为什么**写事务在 remap 时会被阻塞**，直到所有老读事务结束。

### 1.3 读写事务（Begin(writable=true) → beginRWTx）

开启路径：[db.go:839-872](file:///e:/gsb/617/gsb_11/Earth/db.go#L839-L872) `beginRWTx()`。

完整生命周期：

| 阶段 | 代码位置 | 动作 |
|---|---|---|
| **加 rwlock** | `db.rwlock.Lock()` | 获取全局互斥写锁，保证至多一个 writer |
| **加 metalock** | `db.metalock.Lock()` | 序列化 meta 访问 |
| **拷贝 meta 并递增 txid** | [tx.go:61-64](file:///e:/gsb/617/gsb_11/Earth/tx.go#L61-L64) | 拷贝当前 meta 后，`tx.meta.IncTxid()` → **txid = 已提交最新 txid + 1** |
| **建立脏页缓存** | `tx.pages = make(map[Pgid]*Page)` | COW 缓冲区：所有修改先写入此 map，而非直接写 mmap |
| **挂到 db.rwtx** | `db.rwtx = t` | |
| **释放 pending 页** | `db.freelist.ReleasePendingPages()` | 把所有已无读事务引用的 pending 页合入可分配 freelist（见 §5） |
| **解锁 metalock** | `defer db.metalock.Unlock()` | 锁只保护初始化阶段；Commit 阶段在 `writeMeta()` 内再次获取 metalock |
| **读/写操作** | 首次访问某页时若不在 `tx.pages` 中则从 mmap 反序列化为 `node`（见 [bucket.go:863-901](file:///e:/gsb/617/gsb_11/Earth/bucket.go#L863-L901) `Bucket.node()`），写入时修改 node 的 in-memory 副本；旧页从不原地修改 | 这就是 **COW（写时复制）** |
| **Commit** | [tx.go:170-283](file:///e:/gsb/617/gsb_11/Earth/tx.go#L170-L283) | 详见 §3 |
| **Rollback** | [tx.go:312-343](file:///e:/gsb/617/gsb_11/Earth/tx.go#L312-L343) | `freelist.Rollback(txid)` 把本次 tx 加到 pending 的页撤回；如果 fsync 失败触发 `rollback()`（physical），还会从磁盘重载 freelist |
| **close** | [tx.go:345-378](file:///e:/gsb/617/gsb_11/Earth/tx.go#L345-L378) | `db.rwtx = nil`，`db.rwlock.Unlock()` → 下一个写事务可进入 |

### 1.4 MVCC 并发模型

bbolt 不是经典意义上的多版本并发控制（没有保留多版本链），而是**基于单写者 + COW + 双 meta + 延迟页释放**的极简 MVCC：

- **为什么可以"多读一写"并发？**
  - 只读事务拷贝 meta 后直接读 mmap 中的旧页；
  - 写事务从不原地修改旧页（COW：修改过的 node 会分配新页，旧页在 spill 时加入 pending），所以旧页的内容直到写事务 commit 且 meta 切换后，仍然是只读 tx 打开那一刻的一致视图。
  - 两把锁保证这一点：`rwlock` 互斥写者，`mmaplock.RLock()` 阻止读事务期间 mmap 被 remap 导致旧指针失效。

- **只读事务为什么看到一致快照？**
  - 新 meta 页要在所有脏页 + freelist 页写完并 fsync 之后才会原子写盘（详见 §3）。
  - 一个新的只读事务在 `beginTx()` 中调用 `db.meta()`（[db.go:1141-1162](file:///e:/gsb/617/gsb_11/Earth/db.go#L1141-L1162)）总是取**校验通过且 txid 最大**的 meta。在 meta 原子切换之前，`db.meta()` 返回的仍是上一个已提交的 meta；切换之后才会看到新版本。因此只要读事务在 `metalock` 下完成 meta 拷贝，它之后访问的所有页都经由旧 meta 的 root 指针，内容由 COW 保证不被篡改。

- **txid 单调推进：**
  - 初始化两个 meta 的 txid 分别是 0、1（[db.go:662](file:///e:/gsb/617/gsb_11/Earth/db.go#L662)）。
  - 每次 `beginRWTx` 执行 `tx.meta.IncTxid()`（[tx.go:63](file:///e:/gsb/617/gsb_11/Earth/tx.go#L63)），即写事务的 txid 总是上一次已提交 txid + 1。
  - `Meta.Write` 通过 `p.id = Pgid(m.txid % 2)`（[meta.go:51](file:///e:/gsb/617/gsb_11/Earth/internal/common/meta.go#L51)）将新 meta 交替写入 page 0 和 page 1。

- **写事务之间如何串行化？**
  - `DB.rwlock sync.Mutex`（[db.go:145](file:///e:/gsb/617/gsb_11/Earth/db.go#L145)）在 `beginRWTx` 时获取、在 `Tx.close()` 时释放，构成进程内互斥；
  - 进程间通过 `flock(db, !db.readOnly, ...)`（[db.go:253](file:///e:/gsb/617/gsb_11/Earth/db.go#L253)）在写模式下加排他文件锁，读模式下加共享锁，防止多进程写坏数据库。

---

## 2. 磁盘页（Page）与元数据（Meta）布局及双 Meta 崩溃恢复

### 2.1 页头与页类型

定义在 [page.go:31-36](file:///e:/gsb/617/gsb_11/Earth/internal/common/page.go#L31-L36)：

```go
type Page struct {
    id       Pgid    // 页号
    flags    uint16  // 页类型标志
    count    uint16  // 元素计数
    overflow uint32  // 连续溢出页数量
}
```

页头固定 16 字节（`PageHeaderSize = unsafe.Sizeof(Page{})`）。紧跟页头的是页体，结构由 `flags` 决定（[page.go:18-23](file:///e:/gsb/617/gsb_11/Earth/internal/common/page.go#L18-L23)）：

| flags 值 | 类型 | 页体布局 |
|---|---|---|
| `0x01` BranchPageFlag | B+ 树内部（分支）节点 | `[branchPageElement(pos,ksize,pgid)] * count`，然后是各元素对应的 key 数据 |
| `0x02` LeafPageFlag | 叶子节点 | `[leafPageElement(flags,pos,ksize,vsize)] * count`，然后是 key/value 数据；其中 `flags & BucketLeafFlag` 标识该条目是一个子 bucket |
| `0x04` MetaPageFlag | 元数据 | 直接接一个 `Meta` 结构 |
| `0x10` FreelistPageFlag | 空闲页列表 | `[Pgid] * count`；当 count == 0xFFFF 时，第 1 个 Pgid 才是真实 count（用于支持超过 65535 个空闲页） |

**overflow 页的含义：** 当单个 logical 节点序列化后超过 1 页时，会分配连续 `overflow+1` 个物理页，起始页的 `overflow` 记录了额外的页数（[db.go:1173-1174](file:///e:/gsb/617/gsb_11/Earth/db.go#L1173-L1174)：`p.SetOverflow(uint32(count-1))`）。freelist 与 node 代码在 free/遍历时都会以 `[id, id+overflow]` 作为一个整体处理（例如 [shared.go:77](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L77)）。

### 2.2 Meta 结构

定义在 [meta.go:12-22](file:///e:/gsb/617/gsb_11/Earth/internal/common/meta.go#L12-L22)：

```go
type Meta struct {
    magic    uint32   // 0xED0CDAED
    version  uint32   // 当前为 2
    pageSize uint32
    flags    uint32
    root     InBucket // 根 bucket（包含 root 页号、sequence 等）
    freelist Pgid     // freelist 所在页号；PgidNoFreelist(0xFFFFFFFFFFFFFFFF) 表示未同步
    pgid     Pgid     // 下一个分配的页号高水位
    txid     Txid     // 本 meta 对应的事务 id
    checksum uint64   // 对 checksum 之前所有字段的 FNV-64a 校验和
}
```

磁盘初始化布局（[db.go:646-689](file:///e:/gsb/617/gsb_11/Earth/db.go#L646-L689) `init()`）：

```
页 0: meta0 (txid=0)
页 1: meta1 (txid=1)
页 2: freelist （空）
页 3: 空 leaf 页（根 bucket 的 root）
高水位 pgid = 4
```

### 2.3 为什么维护两个 meta 页

写事务通过 `txid % 2`（[meta.go:51](file:///e:/gsb/617/gsb_11/Earth/internal/common/meta.go#L51)）交替覆写两个 meta 页。其核心意义是：**任何时刻至少有一个 meta 页是完整、有效的**。

在 commit 流程中，写 meta 是**最后一步**（见 §3）。考虑两次提交 txid=5（写 meta0）和 txid=6（写 meta1）：
- 假设在写 meta1 时断电：meta1 校验和会不匹配，但 meta0 仍完整记录了 txid=5 的状态，所有属于 txid=5 的数据页在 meta0 被写入前已全部 fsync 落盘。
- 打开 DB 时只需要回退到 txid=5，数据库一致。

这是一种经典的"影子页 + 双根指针"崩溃恢复模式。

### 2.4 打开数据库时如何选择与校验

1. `Open()` 完成 mmap 后，保存两个 meta 指针：[db.go:539-540](file:///e:/gsb/617/gsb_11/Earth/db.go#L539-L540)。
2. 分别调用 `meta0.Validate()` 和 `meta1.Validate()`（[db.go:545-550](file:///e:/gsb/617/gsb_11/Earth/db.go#L545-L550)）。`Meta.Validate`（[meta.go:25-34](file:///e:/gsb/617/gsb_11/Earth/internal/common/meta.go#L25-L34)）依次检查：
   - `magic == 0xED0CDAED`
   - `version == 2`
   - `checksum == m.Sum64()`（FNV-64a 对 checksum 字段之前的所有字节求哈希）
   - 两者都失败才返回错误。
3. 后续每次通过 `db.meta()`（[db.go:1141-1162](file:///e:/gsb/617/gsb_11/Earth/db.go#L1141-L1162)）读取 meta：
   - 先把 txid 更大的作为 `metaA`；
   - 若 `metaA.Validate()` 通过则使用它，否则回退到 `metaB`。

**双 meta 如何服务崩溃恢复：** 在写 meta 页时（[tx.go:595-625](file:///e:/gsb/617/gsb_11/Earth/tx.go#L595-L625)），meta 内容先在临时 buffer 构造好、计算好 checksum，再一次 `writeAt` 整块写入，最后 `fdatasync`。如果这两个操作之间断电，对应 meta 页要么是整块未写入（仍是旧版本）、要么是整块已写入并通过校验。不会出现"写了一半"的 meta 被采纳——因为 checksum 覆盖整个定长 Meta，任何撕裂写都会导致 Validate 失败而自动回退到另一个 meta。

---

## 3. 读写事务 Commit 完整流程与崩溃一致性

入口：[tx.go:170-283](file:///e:/gsb/617/gsb_11/Earth/tx.go#L170-L283) `Tx.Commit()`。

### 3.1 步骤详解（按执行顺序）

```
Commit()
  │
  ├─ 1. rebalance()           ── tx.root.rebalance()
  │                              递归所有脏 bucket/node，合并低填充率节点
  │
  ├─ 2. spill()               ── tx.root.spill()
  │                              ① 递归处理子 bucket（inline 判断、小 bucket 就地序列化）
  │                              ② 从叶子到根 split+spill：
  │                                   - 旧页 (node.pgid > 0) → freelist.Free(txid, page) 加入 pending
  │                                   - tx.allocate(ceil(size/pageSize)) 分配新页（来自 freelist 或高水位）
  │                                   - node.write(p) 序列化到新页
  │                                   - 更新父节点中的分隔键与子指针
  │                              ③ 更新 meta.root = 新根页号
  │
  ├─ 3. 旧 freelist 页释放
  │     if meta.freelist != NoFreelist:
  │         freelist.Free(txid, db.page(meta.freelist))  // 上一次的 freelist 页加入 pending
  │
  ├─ 4. commitFreelist()       ── 新 freelist 序列化：
  │     p = tx.allocate(EstimatedWritePageSize/pageSize + 1)
  │     freelist.Write(p)                  // 写包含 free ∪ pending 的列表
  │     meta.freelist = p.id
  │     （若 NoFreelistSync：meta.freelist = NoFreelist，不写 freelist 页）
  │
  ├─ 5. grow()                 ── 若 meta.pgid 高水位越过原文件大小：
  │                              file.Truncate(newSize) + file.Sync()（非 NoGrowSync）
  │                              这里只扩展文件逻辑大小，不改变 mmap
  │
  ├─ 6. write()                ── 将 tx.pages 中所有脏页按 pgid 升序 writeAt 落盘
  │                              若 !NoSync: fdatasync(db)  第一次 fsync
  │
  ├─ 7. writeMeta()            ── 写 meta 页（写入 meta0 或 meta1）
  │                              加 metalock → writeAt → 解锁 →
  │                              若 !NoSync: fdatasync(db)  第二次 fsync
  │
  └─ 8. close()                ── db.rwtx = nil; rwlock.Unlock()
       OnCommit handlers...
```

代码对应：
- rebalance：[tx.go:195](file:///e:/gsb/617/gsb_11/Earth/tx.go#L195) → [bucket.go:853-860](file:///e:/gsb/617/gsb_11/Earth/bucket.go#L853-L860)
- spill：[tx.go:204](file:///e:/gsb/617/gsb_11/Earth/tx.go#L204) → [bucket.go:746-802](file:///e:/gsb/617/gsb_11/Earth/bucket.go#L746-L802) → [node.go:295-361](file:///e:/gsb/617/gsb_11/Earth/node.go#L295-L361)
- 旧 freelist 页 free：[tx.go:215-217](file:///e:/gsb/617/gsb_11/Earth/tx.go#L215-L217)
- commitFreelist：[tx.go:219-227](file:///e:/gsb/617/gsb_11/Earth/tx.go#L219-L227) → [tx.go:285-298](file:///e:/gsb/617/gsb_11/Earth/tx.go#L285-L298)
- grow：[tx.go:230-240](file:///e:/gsb/617/gsb_11/Earth/tx.go#L230-L240) → [db.go:1223-1261](file:///e:/gsb/617/gsb_11/Earth/db.go#L1223-L1261)
- write：[tx.go:244](file:///e:/gsb/617/gsb_11/Earth/tx.go#L244) → [tx.go:520-592](file:///e:/gsb/617/gsb_11/Earth/tx.go#L520-L592)
- writeMeta：[tx.go:267](file:///e:/gsb/617/gsb_11/Earth/tx.go#L267) → [tx.go:595-625](file:///e:/gsb/617/gsb_11/Earth/tx.go#L595-L625)

### 3.2 为什么必须是这个顺序才能保证崩溃一致性

崩溃恢复的正确性依赖以下顺序不变量：

1. **先 spill/rebalance 确定所有新页的内容与位置，再分配 freelist 页。**
   freelist 的内容必须反映 spill 阶段结束后的最终页状态（旧页入 pending、新页从 freelist 取走），否则 freelist 里会漏掉或重复页。

2. **先写所有脏页（数据页 + freelist 页），再 fsync，再写 meta。**
   这是影子页（shadow paging）的关键：meta 里的 `root`、`freelist`、`pgid` 指针指向新页；如果 meta 先落盘而数据页没写完，断电后 DB 打开会 meta 指向半写入的垃圾页。先 fsync 数据页 + freelist 页（[tx.go:566-572](file:///e:/gsb/617/gsb_11/Earth/tx.go#L566-L572)），再写 meta 并再次 fsync（[tx.go:613-619](file:///e:/gsb/617/gsb_11/Earth/tx.go#L613-L619)），则：
   - 若在第 1 次 fsync 前断电 → 老 meta 仍完好，所有本次改动都不可见（因为新 meta 从未写入）。
   - 若在第 1 次 fsync 后、写 meta 前断电 → 同上，新页已在盘上但无 meta 指向它们（孤儿页），不会破坏一致性，后续会被扫描识别为空闲页（仅在 NoFreelistSync 时）。
   - 若在写 meta 过程中断电 → meta 校验和失败，自动回退到老 meta（见 §2.4）。

3. **freelist 页必须在数据页一批里一起 fsync。**
   新 meta 指向新 freelist；如果 freelist 没落盘而 meta 先提交，下次打开会读到垃圾 freelist。

4. **旧页是在 spill 阶段就加入 pending，而非立即释放。**
   这样 commit 过程中若失败 rollback，freelist 的 Rollback() 能正确撤回（[shared.go:89-118](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L89-L118)）。而且旧页不被立即复用，保证了并发读事务的快照（见 §5.4）。

### 3.3 NoSync / NoFreelistSync / NoGrowSync 各自放松了什么

| 选项 | 代码 | 放松的保证 | 取舍 |
|---|---|---|---|
| `NoSync` | [db.go:55](file:///e:/gsb/617/gsb_11/Earth/db.go#L55)，跳过 [tx.go:566-572](file:///e:/gsb/617/gsb_11/Earth/tx.go#L566-L572) 与 [tx.go:613-619](file:///e:/gsb/617/gsb_11/Earth/tx.go#L613-L619) 的 `fdatasync` | 不再保证 commit 返回后数据已持久化到盘；OS 页缓存可能丢数据 | 写吞吐大幅提升；崩溃可能丢失**最近若干次**已确认提交的事务，且可能留下撕裂写（实际 bbolt 仍因双 meta + checksum 能回退到最后一个完整 fsync 的事务） |
| `NoFreelistSync` | [db.go:60](file:///e:/gsb/617/gsb_11/Earth/db.go#L60)，[tx.go:225-227](file:///e:/gsb/617/gsb_11/Earth/tx.go#L225-L227) 把 `meta.freelist = PgidNoFreelist` | 不再把 freelist 作为页写盘；打开 DB 时必须扫描整库通过 `freepages()`（[db.go:1277-1312](file:///e:/gsb/617/gsb_11/Earth/db.go#L1277-L1312)）遍历可达页来重建 freelist | 写时少写一页，小事务更快；打开 DB 时需要一次全库扫描（写模式下强制 PreLoadFreelist=true），重建是 O(DB size) |
| `NoGrowSync` | [db.go:75](file:///e:/gsb/617/gsb_11/Earth/db.go#L75)，[db.go:1239](file:///e:/gsb/617/gsb_11/Earth/db.go#L1239) 在 grow() 时跳过 Truncate+Sync | 文件扩展后不立即同步文件大小元数据；主要针对 ext3/ext4 上 truncate 引发的全量 fsync 问题 | 在 ext3/ext4 上写放大减小；在部分文件系统上若在 grow 后马上崩溃可能文件大小未更新（但仅建议非 ext3/ext4 打开） |

注意：OpenBSD 上 `IgnoreNoSync == true`（[types.go:24](file:///e:/gsb/617/gsb_11/Earth/internal/common/types.go#L24)），因为其 UBC 不统一，必须通过 msync 才能看到写入，NoSync 被强制忽略。

---

## 4. B+ 树节点 spill（分裂落盘）与 rebalance（合并/再平衡）

### 4.1 FillPercent 阈值

定义在 [bucket.go:21-27](file:///e:/gsb/617/gsb_11/Earth/bucket.go#L21-L27)：

```go
const (
    minFillPercent     = 0.1
    maxFillPercent     = 1.0
    DefaultFillPercent = 0.5
)
```

- 默认值 `0.5`：分裂时目标页填充率约 50%，为未来的写入预留空间。
- 用户可在 bucket 上设置 `FillPercent`（[bucket.go:43](file:///e:/gsb/617/gsb_11/Earth/bucket.go#L43)）；若是 append-only 工作负载，可设接近 1.0 减少分裂。
- 上下限：`splitTwo` 中 clamp 到 [0.1, 1.0]（[node.go:238-242](file:///e:/gsb/617/gsb_11/Earth/node.go#L238-L242)）。

### 4.2 spill 与分裂（split）

入口：`node.spill()` [node.go:295-361](file:///e:/gsb/617/gsb_11/Earth/node.go#L295-L361)。执行顺序：

1. **后序遍历**：先递归 spill 所有 children（按 first-key 排序后循环，因为 split 可能向 parent 添加新兄弟）。
2. **旧页交还 freelist**：若 `node.pgid > 0`（即该 node 对应一个旧磁盘页），调用 `freelist.Free(txid, tx.page(node.pgid))`，并把 `node.pgid = 0`。这就是 **COW** 的核心——旧页从不原地覆盖，而是被释放进 pending。
3. **split**：调用 `n.split(pageSize)`（[node.go:206-225](file:///e:/gsb/617/gsb_11/Earth/node.go#L206-L225)）。循环调用 `splitTwo`：
   - `splitTwo`（[node.go:229-266](file:///e:/gsb/617/gsb_11/Earth/node.go#L229-L266)）的拒绝条件：节点 inode 数 ≤ `MinKeysPerPage*2` (=4) 或者 `n.sizeLessThan(pageSize)`（能装在一页内）则不分裂。
   - 否则阈值 `threshold = int(float64(pageSize) * fillPercent)`，通过 `splitIndex`（[node.go:271-291](file:///e:/gsb/617/gsb_11/Earth/node.go#L271-L291)）在保证两侧都至少有 `MinKeysPerPage` (=2) 个键的前提下找到使得第一页不超过 threshold 的分割点。
   - 若当前节点无 parent（根分裂），创建新父节点作为新根。
   - 新节点 `next` 继承 `isLeaf`/`parent`/`bucket`，切分 inodes。
4. **分配新页并写入**：对 split 后的每个 node 调用 `tx.allocate((node.size()+pageSize-1)/pageSize)`（连续页数=向上取整），`node.write(p)` 序列化。新 pgid 赋给 `node.pgid`，并在父节点中 `put` 新的分隔键。
5. **新根提升**：若分裂导致根向上生长（`n.parent != nil && n.parent.pgid == 0`），递归 spill 新的父节点。

最终 `bucket.spill()` 将新根页号写回 `InBucket.root`（[bucket.go:793-799](file:///e:/gsb/617/gsb_11/Earth/bucket.go#L793-L799)）。

### 4.3 rebalance 触发条件与行为

入口：`node.rebalance()` [node.go:365-448](file:///e:/gsb/617/gsb_11/Earth/node.go#L365-L448)。只有 `n.unbalanced == true` 的节点才会被处理——该标志由 `node.del()` 在删除 inode 后设置（[node.go:158](file:///e:/gsb/617/gsb_11/Earth/node.go#L158)）。

判定阈值：

```go
threshold = int(float64(pageSize) * FillPercent) / 2
```

即默认 `0.5*pageSize/2 = 25% pageSize`。节点**同时**满足下面两条件才需要 rebalance：

- `n.size() <= threshold`，且
- `len(n.inodes) <= n.minKeys()`（叶节点 minKeys=1，分支节点 minKeys=2）。

不同情形的处理：

1. **根节点特殊处理**（`n.parent == nil`）：若根是分支且只剩一个子节点，把该子节点"提升"为新根，旧子节点 free。
2. **空节点**：从父节点删除该条目并递归 rebalance 父节点。
3. **一般情况——与兄弟合并**：
   - 若自己是父的第一个孩子，与右兄弟合并；否则与左兄弟合并。
   - 把右节点的 inodes 与 children（重父化）全部搬到左节点。
   - 从父节点中删除右节点的分隔键，把右节点的页 free。
   - 递归 rebalance 父节点（父节点刚失去一个孩子，可能也过瘦）。

> 对比经典 B+ 树"先尝试借兄弟、借不到才合并"的策略，bbolt 的 rebalance **总是合并**，靠下一次 spill 时重新 split 来实现再平衡。这是一种实现简化——在 COW 场景下反正都要重新分配新页写入，合并成本不高。

### 4.4 COW「脏节点分配新页、旧页交还 freelist」如何发生

关键节点是 `node.spill()` 的第 2 步（[node.go:318-321](file:///e:/gsb/617/gsb_11/Earth/node.go#L318-L321)）：

```go
if node.pgid > 0 {
    tx.db.freelist.Free(tx.meta.Txid(), tx.page(node.pgid))
    node.pgid = 0
}
```

- `node.pgid > 0` 代表该 node 原先来自磁盘上的某个页（在 `node.read(p)` [node.go:162-174](file:///e:/gsb/617/gsb_11/Earth/node.go#L162-L174) 时设置）。
- `freelist.Free(txid, page)` 不直接把它放回空闲池，而是放入 `pending[txid]`（见 §5.3）。
- 之后 `tx.allocate(n)` 为该 node 分配全新的 pgid，`node.write(p)` 把内容写入新页；新页内容直到 `tx.write()` 才落盘。
- 在 meta 切换之前，旧 meta 仍指向旧页，所以任何基于旧 meta 的只读事务仍然读到旧页。

子 bucket 的 free 也一样：[bucket.go:904-918](file:///e:/gsb/617/gsb_11/Earth/bucket.go#L904-L918) `Bucket.free()` 遍历 bucket 的所有页/节点，逐一调 freelist.Free；DeleteBucket 时调用（[bucket.go:320-322](file:///e:/gsb/617/gsb_11/Earth/bucket.go#L320-L322)）。

---

## 5. Freelist 的两种实现：Array vs HashMap，以及 pending 页延迟释放

公共接口：[freelist.go](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/freelist.go)。共享逻辑（pending 管理、readonly txid 跟踪、rollback、release 等）在 [shared.go](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go) 中，两种后端通过嵌入 `*shared` 并实现 `freePageIds / mergeSpans / Allocate / Init / FreeCount` 来提供后端差异。

### 5.1 shared：pending/allocs/cache/readonlyTXIDs 四个核心数据结构

[shared.go:18-25](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L18-L25)：

| 字段 | 作用 |
|---|---|
| `readonlyTXIDs []Txid` | 当前所有未结束的只读事务的 txid |
| `pending map[Txid]*txPending` | 每个写事务释放的页集合。`txPending.ids` 是 pgid 列表，`alloctx[i]` 是最初分配该页的事务 id（用于 rollback 与 releaseRange） |
| `allocs map[Pgid]Txid` | 记录当前哪些页被**本写事务**新分配出去了（在 Allocate 时写入、在 Free/Rollback 中清除）。Free 时会有 `panic` 校验释放页不能是同一 tx 分配的（[shared.go:68-72](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L68-L72)）——bbolt 要求本 tx 新分配的页若未写入任何 meta 则直接在 Rollback 丢弃即可，不进入 pending |
| `cache map[Pgid]struct{}` | 所有 free ∪ pending 页的快速查询集合，用于 `Freed(pgid)` |

### 5.2 Array 后端（默认）

[array.go](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/array.go)：
- `ids []Pgid`：已排序的可分配空闲页列表。
- `Init(ids)`：直接保存，排序，reindex 缓存。
- `Allocate(txid, n)`：[array.go:21-61](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/array.go#L21-L61) 顺序扫描 `ids`，寻找长度 ≥ n 的连续段：若 `id - initial + 1 == n` 就取出（拷贝删除），返回起始 pgid。
- `mergeSpans(ids)`：通过 `common.Mergepgids` 归并到已有有序列表（[array.go:71-99](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/array.go#L71-L99)）。
- 简单但在数据库大、碎片化严重时为 O(N) 扫描，性能退化明显。

### 5.3 HashMap 后端

[hashmap.go](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/hashmap.go)：
- `freemaps map[uint64]pidSet`：按"连续段长度 size"分组，value 是起始 pgid 集合。
- `forwardMap map[Pgid]uint64`：起始 pgid → 段长度。
- `backwardMap map[Pgid]uint64`：结束 pgid → 段长度。
- `Allocate(txid, n)`：[hashmap.go:61-106](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/hashmap.go#L61-L106)：先精确匹配 `freemaps[n]`；否则找第一个 size ≥ n 的段，切分后用 `addSpan` 把剩余部分加回。O(1) 均摊（map 迭代顺序不稳定，但不影响正确性）。
- `mergeSpans`：[hashmap.go:173-219](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/hashmap.go#L173-L219) 排序后合并相邻段，通过 `mergeWithExistingSpan` [hashmap.go:222-247](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/hashmap.go#L222-L247) 检查前后相邻并用 backwardMap/forwardMap 实现 O(1) 合并。
- 不保证分配最小可用 pgid，但通常更快。

### 5.4 pending 与真正可复用的区别——快照语义的呼应

`Free(txid, p)`（[shared.go:56-87](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L56-L87)）只是把页连同它的 overflow 页加入 `pending[txid].ids` 与 `cache`，**不会立即出现在 free 列表里**。

真正转入可分配池的时机是 `ReleasePendingPages()`（[shared.go:141-158](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L141-L158)），它在两个地方被调用：
1. `beginRWTx()` 中每次开启新写事务时调用（[db.go:870](file:///e:/gsb/617/gsb_11/Earth/db.go#L870)）。
2. （间接地通过 `release`/`releaseRange`）。

算法：
1. 排序 `readonlyTXIDs`，取最小值 `minid`。
2. `release(minid - 1)`：把所有 `tid ≤ minid - 1` 的 pending 桶整桶搬到 free 池。这些事务对应的是——在当前**最老仍活着的只读事务之前**就已提交的写事务所释放的页。
3. 然后对每个 `readonlyTXIDs[i]` 做 `releaseRange(minid, tid-1)`，意思是：如果某个页被 [minid, tid-1] 区间的事务释放、又在同一区间内被重新分配（记录在 `allocs`/`alloctx`），那么这个页在 tid 之前已经"被新版本覆盖"，不会再被 tid 这个老读事务访问，也可以释放。最后 `releaseRange(minid, MaxUint64)` 处理尾巴。

**为什么释放后不能立即复用？**

因为 bbolt 的只读事务直接指向 mmap 内存——`tx.page(id)` 返回的是 `db.data[pos]` 处的裸指针。若写事务 commit 后立即把旧页放回 freelist 并覆写，那么仍然活跃的老只读事务下一次遍历该页时就会读到被覆盖的新数据，**快照语义被破坏**（可能导致 key/value 错位、panic、甚至返回垃圾数据）。

所以页复用的必要条件是：**所有可能仍然引用旧页 pgid 的只读事务都已结束**。bbolt 通过以下机制联合保证：
- 只读事务开启时把自己的 txid 登记到 `readonlyTXIDs`（[db.go:822](file:///e:/gsb/617/gsb_11/Earth/db.go#L822)），关闭时移除（[db.go:883](file:///e:/gsb/617/gsb_11/Earth/db.go#L883)）；
- Meta 中的 root/freelist 等指针只能在 meta fsync 后才"切换"，而老 meta 指向的所有旧页都不会被释放进 free 池，直到最老的读事务的 txid 已经大于释放该页的写事务 txid。

这与第 1 问的快照语义直接呼应：**只读 tx 之所以能零拷贝读一致快照，正是因为 freelist 不会在旧读 tx 存活期间把旧页重新分发出去。**

---

## 6. mmap 按需重映射与 Cursor 栈式遍历

### 6.1 mmap 基础与容量增长策略

- 初始 mmap：在 `Open()` 中调用 `db.mmap(InitialMmapSize)`（[db.go:297](file:///e:/gsb/617/gsb_11/Earth/db.go#L297)），然后 `db.data` 指向 mmap 起点，`db.meta0 = db.page(0).Meta()`、`db.meta1 = db.page(1).Meta()`。
- 增长策略 `db.mmapSize(size)`（[db.go:581-613](file:///e:/gsb/617/gsb_11/Earth/db.go#L581-L613)）：
  - 32KB 到 1GB 之间：**按 2 的幂翻倍**（`1<<15` 到 `1<<30`）。
  - 超过 1GB：每次按 1GB 步进（`MaxMmapStep = 1<<30`）。
  - 最终对齐到 pageSize，且不超过 `common.MaxMapSize`。
- 触发时机：`db.allocate()`（[db.go:1165-1220](file:///e:/gsb/617/gsb_11/Earth/db.go#L1165-L1220)）在 freelist 没有空间、需要从高水位分配时，若 `minsz >= db.datasz` 就调用 `db.mmap(minsz)`。
- `mmap()` 流程（[db.go:456-553](file:///e:/gsb/617/gsb_11/Earth/db.go#L456-L553)）：
  1. 获取 `mmaplock.Lock()`（写锁）。
  2. 若存在活跃 rwtx，调用 `db.rwtx.root.dereference()` 让写事务持有的 node 的 key/value 从 mmap 指针复制到堆内存（[node.go:463-491](file:///e:/gsb/617/gsb_11/Earth/node.go#L463-L491)），避免 unmap 后悬挂指针。
  3. `munmap()` 旧映射，`mmap(db, size)` 建立新映射。
  4. 重新设置 `db.meta0 / db.meta1` 并校验。

### 6.2 为什么活跃事务期间不能随意 remap，如何协调

- 只读事务持有 `mmaplock.RLock()`（[db.go:801](file:///e:/gsb/617/gsb_11/Earth/db.go#L801)），整个生命周期不释放。
- `mmap()` 获取 `mmaplock.Lock()`——必须等待所有读事务结束才能进入。
- 写事务在 `beginRWTx` 时不持有 mmap 读锁，所以可以被 remap 阻塞；但写事务在持有 rwlock 期间若调用 `allocate()` 需要 remap，则等待所有**已有**读事务完成。代码注释 [db.go:756-763](file:///e:/gsb/617/gsb_11/Earth/db.go#L756-L763) 明确警告：
  > 在同一个 goroutine 先开读事务再开写事务可能死锁；长读事务会阻塞写事务的 remap。
- `dereference()` 是写事务侧的保护机制：remap 后旧的 mmap 地址被 munmap，但写事务通过 node 缓存访问的 inode Key/Value 指针都已经拷贝到堆上（[node.go:471-482](file:///e:/gsb/617/gsb_11/Earth/node.go#L471-L482)），因此不会悬挂。
- 只读事务从不缓存 mmap 外的副本，但它们在 mmap RLock 保护下，mmap 区域不会变，所以指针始终有效。

### 6.3 Cursor 栈式定位与顺序遍历

`Cursor`（[cursor.go](file:///e:/gsb/617/gsb_11/Earth/cursor.go)）维护一个 `stack []elemRef`（[cursor.go:412-416](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L412-L416)），每个 `elemRef{page, node, index}` 表示路径上一层中的某个分支/叶子项：

- **Search/Seek**：`seek(seekKey)` 调用 `search()`（[cursor.go:283-302](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L283-L302)），从 root 开始递归：
  1. 把当前页/节点压栈；
  2. 若为叶子，用 `nsearch`（[cursor.go:348-367](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L348-L367)）二分查找确定 index；
  3. 若为分支，通过 `searchNode`/`searchPage`（[cursor.go:304-345](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L304-L345)）二分找到第一个 key ≥ seekKey 的索引，非精确匹配时回退一格（`if !exact && index > 0 { index-- }`），进入对应子页 pgid 递归。
- **First**：清空栈、压入 root，调用 `goToFirstElementOnTheStack()`（[cursor.go:169-187](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L169-L187)）沿 index=0 一路压栈直到叶子。
- **Last**：从 root 起每层压入末尾元素，`c.last()`（[cursor.go:190-211](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L190-L211)）下钻到叶子最后一个元素。
- **Next**（[cursor.go:215-247](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L215-L247)）：从栈顶向上找到第一个 `index < count()-1` 的层级，index++ 然后截栈，再 `goToFirstElementOnTheStack()` 下钻到第一个叶子元素；空页则继续回溯。越过末尾时返回 nil。
- **Prev**（[cursor.go:251-280](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L251-L280)）：类似但向上找 `index > 0` 递减，然后 `c.last()` 下钻到该分支下最右叶子。
- **keyValue**（[cursor.go:370-387](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L370-L387)）：根据栈顶是 node 还是 page，分别从 `node.inodes[ref.index]` 或 `page.LeafPageElement(index)` 取出 key/value/flags。
- **node()**（[cursor.go:390-409](file:///e:/gsb/617/gsb_11/Earth/cursor.go#L390-L409)）：写操作调用，会把栈路径上的 page 物化为 `node`（通过 `Bucket.node()` 缓存），返回叶子 node 供 `put/del` 修改。

Cursor 返回的 key/value 切片**直接指向 mmap 内存或 node 中的 inode 缓冲**，文档明确说明只在事务生命周期内有效，且不得写入（[bucket.go:432](file:///e:/gsb/617/gsb_11/Earth/bucket.go#L432)）。

---

## 7. 关键隐式不变量与风险点

以下是该引擎中容易被忽视、但对正确性至关重要的隐式不变量，并附对应的代码依据。

### 7.1 不变量：mmap 重映射会使所有旧页/旧 key/value 裸指针失效

**风险：** 任何跨越 `db.mmap()` 调用对 mmap 指针的保留都会变成悬挂指针，导致 SEGV 或静默数据损坏。bbolt 本身通过两条规则保证安全：
1. 只读事务持有 `mmaplock.RLock()`，因此在其存活期间 `mmap()` 无法进入（[db.go:801](file:///e:/gsb/617/gsb_11/Earth/db.go#L801) + [db.go:457](file:///e:/gsb/617/gsb_11/Earth/db.go#L457)）。
2. 写事务在 remap 前调用 `rwtx.root.dereference()`（[db.go:504-506](file:///e:/gsb/617/gsb_11/Earth/db.go#L504-L506)），把所有 node 的 key/value 拷贝到堆（[node.go:463-491](file:///e:/gsb/617/gsb_11/Earth/node.go#L463-L491)）。

**使用者风险：**
- 用户从 `Bucket.Get`/`Cursor` 拿到的 `[]byte` 在事务结束后是悬空指针（文档有提示，但调用方常误以为是拷贝）。
- 同一个 goroutine 中先开只读 tx，再在它未关闭时开写 tx，会让写事务在 allocate → mmap() 时等待读 tx 的 RLock，构成自锁死锁（[db.go:756-760](file:///e:/gsb/617/gsb_11/Earth/db.go#L756-L760)）。

### 7.2 不变量：freelist 释放的页必须等待所有更老的只读事务结束才能复用

**风险：** 违背这一不变量就会让老读事务读到被覆写的页，出现随机数据错位、checksum 失败甚至 panic（例如 [page.go:90-98](file:///e:/gsb/617/gsb_11/Earth/internal/common/page.go#L90-L98) 的 `FastCheck` 可能因 page.id 被覆写为任意值而 panic）。

代码级保证：
- `Free()` 仅放入 `pending[txid]`（[shared.go:62-86](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L62-L86)）。
- `ReleasePendingPages()` 仅把 `tid ≤ minReadonlyTxid - 1` 的 pending 合入 free，并用 `releaseRange` 精细处理"已被同一区间新事务重新分配"的特殊情况（[shared.go:141-158](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L141-L158)）。
- `Free()` 中有断言不能 free 本 tx 自己分配的页（[shared.go:68-72](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L68-L72)）。

**使用方风险：** 长读事务（例如做快照备份时开启的只读 tx）会让所有被覆盖的旧页无法被复用，数据库文件大小在写入压力下快速增长——这不是 bug，是 MVCC 的自然结果，但常让新用户误解为"空间泄漏"。设置足够大的 `InitialMmapSize`、避免长时间持有读事务是缓解手段（[db.go:761-763](file:///e:/gsb/617/gsb_11/Earth/db.go#L761-L763)）。

### 7.3 不变量：meta 选择必须按"txid 更大且 Validate 通过"为准，绝不能反过来

**风险：** 若代码错误选择了 txid 较小的 meta，或忽略 checksum 错误的 meta 继续使用，会让数据库回退到旧状态，从而把新写入的大量数据页视为"孤儿"——在 NoFreelistSync 模式下这些页还能被 `freepages()` 扫描回收，但在普通模式下 freelist 也跟着回退，**会导致 freelist 把这些仍被旧根引用的活跃页标记为空闲并在后续写入中覆写**，彻底损坏数据库。

代码级保证：
- `db.meta()`（[db.go:1141-1162](file:///e:/gsb/617/gsb_11/Earth/db.go#L1141-L1162)）显式：先挑 txid 更大者作为 metaA，A 校验失败才回退 B。
- `mmap()` 中要求"两个 meta 都失败才报错"（[db.go:545-550](file:///e:/gsb/617/gsb_11/Earth/db.go#L545-L550)），不会因为单个 meta 损坏就拒绝打开。
- `Meta.Write` 在 `Meta.checksum = m.Sum64()` 之后才把 meta 拷贝到页（[meta.go:54-57](file:///e:/gsb/617/gsb_11/Earth/internal/common/meta.go#L54-L57)），`Sum64` 对 `checksum` 字段之前的所有字节（包括 magic/version/pageSize/root/freelist/pgid/txid）做 FNV-64a（[meta.go:61-65](file:///e:/gsb/617/gsb_11/Earth/internal/common/meta.go#L61-L65)），任何位翻转或撕裂写都能被检出。
- `writeMeta` 在写 meta 时持有 metalock，防止并发读者读到半写状态的 meta 指针（[tx.go:606-612](file:///e:/gsb/617/gsb_11/Earth/tx.go#L606-L612)）；注意这里写盘是写到 `db.ops.writeAt`（即 OS 文件），而不是直接写 mmap 内存（mmap 是只读映射，`dataref` 注释 [db.go:127](file:///e:/gsb/617/gsb_11/Earth/db.go#L127)："mmap'ed readonly, write throws SEGV"），因此并发读事务通过 mmap 看到的是旧 meta，meta 切换靠下次 `beginTx()` 重新读取 `db.meta()` 返回的指针（注意：mmap 到文件是共享映射，writeAt 写页缓存再 fsync 后 mmap 区也会看到新内容，但因为 meta 是按 txid%2 交替写在 page0/1，只读 tx 已经拷贝了自己的 meta 指针，不会在中途切换根）。

### 7.4 其他值得关注的细节

- **Commit 顺序严格性（不变量）：** spill → freelist 落盘 → write dirty pages & fsync → writeMeta & fsync（见 §3）。`Tx.Commit` 代码里任何调换都会破坏崩溃一致性。比如若把 `writeMeta` 放到 `write()` 之前，崩溃后 meta 可能指向未落盘的新页。
- **freelist.Write 只写 free ∪ pending 的"总快照"**（[shared.go:287-310](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go#L287-L310)），但 Allocate 只从 free 池取——pending 页不会被误分配，因为它不在后端的 free ids/span 中（`mergeSpans` 仅在 release 时调用）。
- **spill 递归 children 时采用索引循环而非 range**（[node.go:304-309](file:///e:/gsb/617/gsb_11/Earth/node.go#L304-L309)），因为 split 可能在 spill 过程中向 parent.children 追加新兄弟节点，range 会漏掉新节点。
- **rebalance 总是合并，而不是"借键"**，见 §4.3。这意味着删除后的节点可能低于 FillPercent 一半，但直到下一次 Commit 的 rebalance 阶段才会和兄弟合并，而不是像教科书 B+ 树那样做旋转。这是 bbolt 的一个实现简化，依赖 spill 阶段按 FillPercent 重新切分。
- **`mmapSize` 翻倍策略上限 1GB**（[db.go:582-587](file:///e:/gsb/617/gsb_11/Earth/db.go#L582-L587)），超过后按 1GB 步进。这是为了小 DB 启动快、大 DB 避免 remap 抖动，同时限制虚拟地址空间浪费。Windows 上 mmap 会把文件立即扩展到 mmap 大小（注释 [db.go:480-494](file:///e:/gsb/617/gsb_11/Earth/db.go#L480-L494)），因此 MaxSize 检查在 Windows 上提前到 mmap 前。
- **`NoSync` 并不完全等价于"无持久化保证"**：在 OpenBSD 上被强制忽略；在 Linux 上即使不开 NoSync，磁盘自身写缓存也可能丢数据（bbolt 的保证仅到"fsync 返回后已提交给 OS/磁盘"）。

---

## 附：主要文件索引

| 文件 | 作用 |
|---|---|
| [db.go](file:///e:/gsb/617/gsb_11/Earth/db.go) | 数据库打开/关闭、mmap/grow/allocate、事务入口、meta 选择 |
| [tx.go](file:///e:/gsb/617/gsb_11/Earth/tx.go) | Tx 结构、Commit/Rollback、脏页写回与 meta 写盘 |
| [bucket.go](file:///e:/gsb/617/gsb_11/Earth/bucket.go) | Bucket API、node 缓存、spill/rebalance 驱动、inline bucket |
| [node.go](file:///e:/gsb/617/gsb_11/Earth/node.go) | 内存节点、split/spill/rebalance/dereference/free |
| [cursor.go](file:///e:/gsb/617/gsb_11/Earth/cursor.go) | B+ 树遍历与定位 |
| [internal/common/page.go](file:///e:/gsb/617/gsb_11/Earth/internal/common/page.go) | 磁盘页结构、branch/leaf 元素布局 |
| [internal/common/meta.go](file:///e:/gsb/617/gsb_11/Earth/internal/common/meta.go) | Meta 结构、校验、Write、Sum64 |
| [internal/common/types.go](file:///e:/gsb/617/gsb_11/Earth/internal/common/types.go) | 全局常量、Txid/Pgid 类型定义 |
| [internal/freelist/freelist.go](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/freelist.go) | Freelist 接口定义 |
| [internal/freelist/shared.go](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/shared.go) | pending、readonly txid 跟踪、rollback、release 逻辑 |
| [internal/freelist/array.go](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/array.go) | 数组式 freelist |
| [internal/freelist/hashmap.go](file:///e:/gsb/617/gsb_11/Earth/internal/freelist/hashmap.go) | HashMap 式 freelist（span 双向索引） |
