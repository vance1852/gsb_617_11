# bbolt (etcd-io/bbolt) 深度代码分析

> 代码版本：基于当前工作目录的 bbolt 源码
> 分析目标：嵌入式 B+ 树键值存储引擎的核心实现机制

---

## 1. 事务生命周期与 MVCC 并发模型

### 1.1 事务的开启流程

bbolt 中事务分为两类：**只读事务 (Read-Only Tx)** 和 **读写事务 (Read-Write Tx)**，它们通过 `DB.Begin()` 开启：

```go
// db.go:767-783
func (db *DB) Begin(writable bool) (t *Tx, err error) {
    if writable {
        return db.beginRWTx()
    }
    return db.beginTx()
}
```

#### 只读事务开启（beginTx）

代码位置：[db.go#L792-L837](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L792-L837)

1. **获取 `metalock`**（meta 页锁）
2. **获取 `mmaplock.RLock()`**（mmap 读锁）——这保证在事务存活期间 mmap 不会被重映射
3. **校验数据库状态**（opened、data 映射有效）
4. **初始化事务对象**：
   - 调用 `tx.init(db)`：**拷贝当前 meta 页**到事务私有副本（关键！这是快照一致性的基础）
   - 拷贝 root bucket 信息
5. **将 txid 注册到 freelist**：`freelist.AddReadonlyTXID(t.meta.Txid())`——用于后续判断哪些 pending 页可以释放
6. **释放 `metalock`**（mmap 读锁一直持有到事务结束）
7. 更新统计信息

#### 读写事务开启（beginRWTx）

代码位置：[db.go#L839-L872](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L839-L872)

1. **获取 `rwlock`**（全局写锁）——这是**写事务串行化**的核心，保证至多一个写事务
2. **获取 `metalock`**（meta 页锁），且在函数返回后仍持有（在 commit/rollback 时才通过 `close()` 释放）
3. 校验数据库状态
4. **初始化事务对象**：
   - `writable: true`
   - 初始化脏页缓存 `tx.pages = make(map[Pgid]*Page)`
   - **关键**：`tx.meta.IncTxid()`——将 txid 加 1，这是 txid 单调推进的地方
5. 设置 `db.rwtx = t`
6. **调用 `freelist.ReleasePendingPages()`**——释放所有不再被任何旧只读事务引用的 pending 页到空闲列表

### 1.2 事务执行读写操作

- **只读事务**：所有读取直接通过 `tx.page(id)` 访问 mmap 区域（[tx.go#L629-L642](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L629-L642)）。由于 mmap 读锁保护 + COW 机制，mmap 中的旧页永远不会被覆盖，因此只读事务看到的是开启时刻的一致快照。
- **读写事务**：
  - 读：优先查 `tx.pages` 脏页缓存，否则读 mmap
  - 写：所有修改发生在内存中的 `node` 结构上（从 page 反序列化而来），修改会标记 `unbalanced` 或导致节点分裂
  - 新分配的页在 `tx.pages` 缓存中，直到 Commit 时才批量写入磁盘

### 1.3 事务收尾（Commit/Rollback）

#### 只读事务 Rollback（只读事务不能 Commit）

代码位置：[tx.go#L302-L320](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L302-L320)

调用 `tx.close()` → `db.removeTx(tx)`：

1. **释放 `mmaplock.RUnlock()`**（mmap 读锁）
2. 获取 `metalock`，调用 `freelist.RemoveReadonlyTXID(txid)` 从活跃只读事务列表移除该 txid
3. 更新统计信息

#### 读写事务 Commit

（详见第 3 节）Commit 成功后在 `close()` 中：

1. `db.rwtx = nil`
2. **释放 `rwlock.Unlock()`**——允许下一个写事务进入
3. 合并统计信息

#### 读写事务 Rollback

代码位置：[tx.go#L311-L343](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L311-L343)

1. 调用 `freelist.Rollback(txid)` 撤销该事务的所有 pending 释放和分配
2. 如果是物理回滚（系统错误后），还需要从磁盘重新加载 freelist
3. 同 Commit 一样释放 `rwlock`

### 1.4 MVCC 并发模型详解

#### 为什么允许多个只读事务与至多一个读写事务并发？

由三把锁的设计保证（[db.go#L145-L148](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L145-L148)）：

| 锁名       | 类型           | 作用                                                                      |
| ---------- | -------------- | ------------------------------------------------------------------------- |
| `rwlock`   | `sync.Mutex`   | 写互斥锁，**只在 beginRWTx 获取，Commit/Rollback 时释放**，保证写事务串行 |
| `metalock` | `sync.Mutex`   | 保护 meta 页访问，开启事务时短暂获取                                      |
| `mmaplock` | `sync.RWMutex` | mmap 重映射保护，只读事务持读锁，重映射时需写锁                           |

- **只读事务**只需要 `mmaplock.RLock()`（读共享），不阻塞其他只读事务，也不阻塞写事务的写操作（写操作写脏页不碰 mmap）
- **读写事务**需要 `rwlock`（独占），但不阻塞已开启的只读事务（因为 COW 让它们读旧页）

#### 只读事务为何能看到一致的数据快照？

两个关键机制：

1. **开启时拷贝 meta 页**：[tx.go#L52-L53](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L52-L53) 调用 `db.meta().Copy(tx.meta)`，将当前最新的 meta（包含 root bucket pgid、pgid 高水位等）拷贝到事务私有副本。之后无论写事务如何提交，这个私有 meta 不会变。

2. **COW（写时复制）**：写事务修改任何节点时，不会原地修改 mmap 中的旧页，而是分配新页、在新页上写入修改后的内容，最后提交时原子更新 meta 指向新的 root。mmap 中的旧页内容永远不会被覆盖（直到没有任何只读事务引用它们）。

3. **mmap 读锁保护**：只读事务持有 `mmaplock.RLock()` 直到结束，这期间 mmap 不能被重映射，保证指针有效性。

#### txid 如何单调推进？

- txid 存储在 meta 页中（`meta.txid`）
- **只在读写事务初始化时推进**：[tx.go#L63](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L63) `tx.meta.IncTxid()`，在 `beginRWTx()` → `tx.init(db)` 中调用
- 只读事务只是拷贝当前 meta 中的 txid，不会修改它
- meta 页通过 `p.id = Pgid(m.txid % 2)`（[meta.go#L51](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/meta.go#L51)）交替写入 page 0 和 page 1

#### 写事务之间如何串行化？

完全由 `db.rwlock sync.Mutex` 保证：

- `beginRWTx()` 第一行就是 `db.rwlock.Lock()`（[db.go#L847](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L847)）
- 这把锁在 `tx.close()` 中才释放（[tx.go#L357](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L357) `db.rwlock.Unlock()`）
- 因此第二个写事务的 Begin(true) 会阻塞在 rwlock.Lock()，直到前一个写事务 Commit/Rollback 完成

---

## 2. 磁盘页（Page）与元数据（Meta）布局

### 2.1 页头结构与页类型

页结构定义在 [page.go#L31-L36](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/page.go#L31-L36)：

```go
type Page struct {
    id       Pgid     // 8 bytes: 页ID
    flags    uint16   // 2 bytes: 页类型标志
    count    uint16   // 2 bytes: 元素计数
    overflow uint32   // 4 bytes: 溢出页数量（连续后续页数）
}
// PageHeaderSize = 16 bytes
```

页类型标志（[page.go#L18-L23](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/page.go#L18-L23)）：

| Flag               | 值   | 类型                                     |
| ------------------ | ---- | ---------------------------------------- |
| `BranchPageFlag`   | 0x01 | B+ 树分支节点（内部节点，存键+子页指针） |
| `LeafPageFlag`     | 0x02 | B+ 树叶子节点（存键+值）                 |
| `MetaPageFlag`     | 0x04 | 元数据页                                 |
| `FreelistPageFlag` | 0x10 | 空闲页列表页                             |

页头之后的数据区根据类型不同：

- **分支页**：`count` 个 `branchPageElement`（pos、ksize、pgid），后跟实际 key 数据
- **叶子页**：`count` 个 `leafPageElement`（flags、pos、ksize、vsize），后跟 key/value 数据
- **Meta 页**：`Meta` 结构
- **Freelist 页**：`count` 个 Pgid（空闲页ID列表）

#### Overflow 页的含义

`overflow uint32` 表示该页后紧跟的连续页数量。当单个逻辑节点大小超过一页时（比如大 value），就需要分配连续的多页，首页记录 `overflow = n-1`，数据跨页连续存放。分配时通过 `tx.allocate(count)` 一次性分配 count 个连续页（[tx.go#L500-L517](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L500-L517)）。

### 2.2 Meta 页结构

Meta 结构定义在 [meta.go#L12-L22](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/meta.go#L12-L22)：

```go
type Meta struct {
    magic    uint32   // 魔数 0xED0CDAED
    version  uint32   // 版本号（当前为 2）
    pageSize uint32   // 页大小
    flags    uint32   // 标志位
    root     InBucket // root bucket 的根页
    freelist Pgid     // freelist 所在页 ID
    pgid     Pgid     // 下一个分配的页 ID（高水位）
    txid     Txid     // 事务 ID
    checksum uint64   // 校验和（FNV-1a 64位）
}
```

关键常量：

- `Magic = 0xED0CDAED`（[types.go#L16](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/types.go#L16)）
- `Version = 2`（[types.go#L13](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/types.go#L13)）
- `PgidNoFreelist = 0xffffffffffffffff`（[types.go#L18](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/types.go#L18)）：表示 freelist 未落盘（NoFreelistSync 模式）

### 2.3 为什么维护两个 meta 页？

数据库初始化时创建 4 个页（[db.go#L646-L689](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L646-L689)）：

- Page 0: meta0
- Page 1: meta1
- Page 2: freelist
- Page 3: 空叶子页（root bucket）

**双 meta 的根本原因：崩溃恢复中的原子性保证**

写 meta 是一次单页写入，但单次页写入也可能在磁盘写入过程中（比如正在写第 512 字节时断电）出现 torn write（部分写），导致 meta 损坏。双 meta 设计使得：

1. 每次 Commit 时，txid 单调递增，交替写 meta0 和 meta1（`p.id = txid % 2`）
2. 写 meta 页 **之后** 才做 fsync
3. 如果在写 meta 过程中崩溃，重启时两个 meta 中必有一个是完整的、有合法校验和的

### 2.4 打开数据库时如何选择与校验 meta

代码位置：[db.go#L1140-L1162](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L1140-L1162)

选择逻辑：

```go
func (db *DB) meta() *common.Meta {
    // 第一步：按 txid 排序，txid 大的优先
    metaA := db.meta0
    metaB := db.meta1
    if db.meta1.Txid() > db.meta0.Txid() {
        metaA = db.meta1
        metaB = db.meta0
    }

    // 第二步：优先用高 txid 的，如果它校验失败则用低的
    if err := metaA.Validate(); err == nil {
        return metaA
    } else if err := metaB.Validate(); err == nil {
        return metaB
    }
    panic("invalid meta pages")
}
```

`Meta.Validate()` 校验（[meta.go#L25-L34](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/meta.go#L25-L34)）：

1. `magic == Magic`
2. `version == Version`
3. `checksum == m.Sum64()`——校验和是对除 checksum 字段外所有字节做 FNV-1a 哈希

**校验和计算方式**：[meta.go#L61-L65](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/meta.go#L61-L65)，只对 checksum 字段之前的内容计算（`unsafe.Offsetof(Meta{}.checksum)`）。

在 `mmap()` 初始化时也会做同样的双 meta 校验（[db.go#L545-L550](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L545-L550)）：只要有一个 meta 有效就可以启动；两个都无效才报错。

### 2.5 双 meta 如何服务于崩溃恢复

崩溃场景推演：

1. **在写数据页（非 meta）时崩溃**：meta 未被修改，旧 meta 仍指向完整一致的 B+ 树（上一次 commit 的状态），重启后恢复到上一次 commit 的状态，无损坏。
2. **在写 meta 页时崩溃（torn write）**：两个 meta 中，一个是上一次完整 commit 的（校验和正确、txid 较小），另一个可能损坏。重启选择校验通过、txid 较大的那一个（即上一次完整的）。
3. **在 fsync 前崩溃**：meta 可能在 OS 页缓存中但未刷盘，重启后磁盘上仍是上一次的 meta。

**关键不变量**：meta 页写入完成且 fsync 成功后，这一次 commit 才是持久的；在这之前任何点崩溃，数据库都回退到上一个有效 meta 对应的一致状态。

---

## 3. 读写事务 Commit 的完整流程

### 3.1 Commit 步骤总览

代码位置：[tx.go#L170-L283](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L170-L283)

严格按以下顺序执行：

| 顺序 | 步骤                                       | 代码位置                               |
| ---- | ------------------------------------------ | -------------------------------------- |
| 1    | **rebalance**                              | `tx.root.rebalance()`                  |
| 2    | **spill**                                  | `tx.root.spill()`                      |
| 3    | 更新 meta 中 root bucket 指针              | `tx.meta.SetRootBucket(...)`           |
| 4    | 释放旧 freelist 页                         | `freelist.Free(txid, oldFreelistPage)` |
| 5    | 落盘新 freelist（若 NoFreelistSync=false） | `tx.commitFreelist()`                  |
| 6    | grow 数据库文件（如果需要）                | `db.grow(...)`                         |
| 7    | write 所有脏页到磁盘 + fdatasync           | `tx.write()`                           |
| 8    | writeMeta 写 meta 页 + fdatasync           | `tx.writeMeta()`                       |
| 9    | close 事务、释放锁                         | `tx.close()`                           |
| 10   | 执行 commitHandlers                        | 在锁释放后执行                         |

### 3.2 逐步分析

#### Step 1: rebalance（再平衡）

[tx.go#L195](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L195) 对删除后节点数过少的节点进行合并/借用（详见第 4 节）。

#### Step 2: spill（分裂落盘）

[tx.go#L204](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L204) 将所有脏节点（被修改过的节点）分配新页、序列化、写入 `tx.pages` 缓存（此时还未写到磁盘），详见第 4 节。

#### Step 3-4: 更新 meta 指针、释放旧 freelist

[tx.go#L212-L217](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L212-L217)：

- 更新 root bucket 指向 spill 后的新根页
- 如果旧 freelist 存在（不是 NoFreelistSync 模式），将旧 freelist 页加入 freelist 的 pending 列表（因为新 freelist 已经写入了新的页）

#### Step 5: commitFreelist（落盘 freelist）

[tx.go#L219-L227](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L219-L227)、[tx.go#L285-L298](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L285-L298)：

- 估算 freelist 序列化大小，分配新页
- 调用 `freelist.Write(p)` 将空闲页列表序列化到新页
- 更新 `meta.freelist = p.Id()`
- 如果是 `NoFreelistSync` 模式，设置 `meta.freelist = PgidNoFreelist`，跳过 freelist 落盘

#### Step 6: grow 数据库文件

[tx.go#L230-L240](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L230-L240)：如果 `meta.pgid`（高水位）比 commit 前前进了，说明分配了新页，需要调用 `db.grow()` 确保文件大小足够。

#### Step 7: write（写脏页 + fdatasync）

[tx.go#L244](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L244) → [tx.go#L520-L592](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L520-L592)：

- 将 `tx.pages` 按页 ID 排序
- 按顺序调用 `writeAt()` 写入每一页
- **关键**：写完所有脏页后，调用 `fdatasync(db)` 将数据页刷到磁盘（除非 NoSync）
- 单页的大页循环写入，每块不超过 `MaxAllocSize`

#### Step 8: writeMeta（写 meta 页 + fdatasync）

[tx.go#L267](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L267) → [tx.go#L594-L625](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L594-L625)：

- 将 meta 序列化到 buffer
- 获取 `metalock`
- 写 meta 页到其交替位置（0 或 1）
- 释放 `metalock`
- **再次调用 `fdatasync(db)` 刷盘 meta 页**（除非 NoSync）

### 3.3 为什么必须是这个顺序才能保证一致性？

WAL（写前日志）的替代方案：bbolt 没有 WAL，而是通过** carefully ordered writes + double meta + fsync** 实现崩溃一致性。

顺序的核心逻辑是：**meta 是"commit 指针"，必须最后写入且必须是原子的（单页），且必须在它指向的所有页都持久化之后才写入。**

推理：

1. **先写数据页，后写 meta**：meta 中包含 root bucket pgid。如果先写 meta 再写数据页，崩溃后 meta 可能指向未写入或部分写入的页，数据库损坏。
2. **数据页写完必须 fsync，然后才能写 meta**：`write()` 中先 fsync 数据页，`writeMeta()` 中再 fsync meta。这保证在 meta 刷盘前，它指向的所有新页已经在磁盘上。
3. **freelist 在数据页之前写入**：freelist 本身也是数据页（记录哪些页是空闲的），新 meta 指向新 freelist，新 freelist 必须在 meta 之前持久化。
4. **spill 时旧页加入 pending（COW）**：旧页不会被立刻复用或覆写，而是保留在 pending 中直到没有旧只读事务引用。这保证了旧 meta 如果被恢复，它指向的旧页内容仍然完好。
5. **meta 交替写且校验和保护**：torn write 只会损坏一个 meta，另一个仍然完好，可以回退。

**断电后的恢复保证**：

- 如果在 write 数据页阶段断电：meta 没改 → 回到上一状态 ✅
- 如果在 fsync 数据页之后、写 meta 之前断电：meta 没改 → 回到上一状态 ✅
- 如果在写 meta 页过程中断电：该 meta 校验和失败，选另一个 txid 稍小但校验通过的 meta → 回到上一状态 ✅
- 如果在 fsync meta 后断电：新 meta 有效 → 新状态持久化 ✅

### 3.4 NoSync / NoFreelistSync 等选项的取舍

#### `DB.NoSync`（[db.go#L45-L55](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L45-L55)）

- **放松的保证**：跳过每次 commit 后的 `fdatasync()` 调用
- **风险**：操作系统崩溃或断电时，最近若干次 commit 的数据可能丢失（OS 页缓存中的数据未刷盘）。注意：应用进程崩溃不会丢（OS 会在进程退出后继续保留页缓存），但机器宕机会丢。
- **性能收益**：极高，省去了最慢的磁盘 I/O 等待
- **适用场景**：批量加载、临时缓存、可容忍最近数据丢失的场景
- 注意：OpenBSD 上 `IgnoreNoSync=true` 强制忽略此选项（因为 OpenBSD 没有统一缓冲缓存，必须用 msync）

#### `DB.NoFreelistSync`（[db.go#L57-L60](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L57-L60)）

- **放松的保证**：不将 freelist 页写入磁盘（`meta.freelist = PgidNoFreelist`）
- **恢复代价**：打开数据库时，如果检测到 freelist 未持久化，需要**完整扫描整个数据库**（遍历所有可达页）来重建 freelist（[db.go#L425-L427](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L425-L427) → `db.freepages()`）
- **性能收益**：每次 commit 省去了 freelist 页的写入和读取，写放大显著降低
- **风险**：**不影响 ACID 一致性**！因为即使 freelist 丢了，数据页和 meta 都在，扫描可以重建 freelist。只是启动恢复变慢。
- **适用场景**：写密集、重启不频繁的场景（如 etcd 通常开启这个）

#### `DB.NoGrowSync`（[db.go#L69-L75](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L69-L75)）

- **放松的保证**：数据库文件增长（truncate）时跳过 fsync
- **风险**：ext3/ext4 上 truncate 后如果不 fsync，可能出现文件大小元数据未持久化的问题
- 仅在非 ext3/ext4 文件系统上安全

---

## 4. B+ 树节点的 spill（分裂落盘）与 rebalance（合并/再平衡）

### 4.1 FillPercent 的默认值与上下限

常量定义在 [bucket.go#L20-L27](file:///e:/gsb/617/gsb_11/Jupiter/bucket.go#L20-L27)：

```go
const (
    minFillPercent     = 0.1   // 10%
    maxFillPercent     = 1.0   // 100%
    DefaultFillPercent = 0.5   // 50%
)
```

- **默认值 50%**：每个页默认填充到 50% 左右，留足空间给后续插入，避免频繁分裂
- **硬限制**：用户设置的 FillPercent 会被 clamp 到 [0.1, 1.0]（[node.go#L238-L242](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L238-L242)）
  - 低于 10%：浪费空间太严重
  - 高于 100%：不可能（页大小上限就是 100%）

#### FillPercent 如何决定页分裂阈值？

`splitTwo` 中的阈值计算（[node.go#L243](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L243)）：

```go
threshold := int(float64(pageSize) * fillPercent)
```

`splitIndex` 找分割点时（[node.go#L271-L291](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L271-L291)）：

- 第一个节点至少保留 `MinKeysPerPage = 2` 个键
- 累计大小超过 `threshold` 就停止，剩下的分到第二个节点
- 分裂条件（[node.go#L232](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L232)）：节点数 ≥ 4（MinKeysPerPage\*2）且 size > pageSize

**为什么默认是 50%？** B+ 树经典设计——分裂后两个节点各约半满，这样即使后续随机插入，也需要相当数量的插入才会导致再次分裂，摊销成本低。对于纯 append-only 工作负载，可以提高到 90%+ 来提高空间利用率。

### 4.2 rebalance：触发条件、合并与借用

代码位置：[node.go#L365-L448](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L365-L448)

#### 触发条件

节点在执行 `del()` 删除键时会被标记 `unbalanced = true`（[node.go#L158](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L158)）。Commit 时遍历所有节点，对 `unbalanced` 节点调用 rebalance。

#### rebalance 的判断阈值

```go
var threshold = int(float64(n.bucket.tx.db.pageSize)*n.bucket.FillPercent) / 2
if n.size() > threshold && len(n.inodes) > n.minKeys() {
    return  // 不需要再平衡
}
```

阈值是 FillPercent 对应大小的**一半**（默认是 25% 页大小）。如果节点 size > 阈值且键数足够（叶子 ≥1，分支 ≥2），跳过。

#### rebalance 执行逻辑

1. **根节点特殊处理**（[node.go#L381-L403](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L381-L403)）：如果是分支节点且只有一个子节点，直接用子节点替换根（树高度降低一层）。

2. **空节点删除**（[node.go#L407-L414](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L407-L414)）：节点没有任何键，直接从父节点删除自己，父节点递归 rebalance。

3. **与兄弟节点合并**（[node.go#L418-L447](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L418-L447)）：
   - 如果是第一个子节点，合并到右兄弟；否则合并到左兄弟
   - 将右兄弟的所有 inodes 追加到左节点
   - 从父节点删除右兄弟的分隔键
   - 删除右节点（旧页加入 freelist pending）
   - 父节点递归 rebalance（因为少了一个子节点）

**注意**：bbolt 的 rebalance **不做"向兄弟借用"（rotation）**！它只做完全合并。这是 bbolt 的简化设计，代价是删除后空间利用率可能在某些 workload 下不高，但实现简单、正确性容易保证。

### 4.3 写时复制（COW）下"脏节点分配新页、旧页交还 freelist"如何发生？

核心代码在 [node.go#L293-L361](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L293-L361)（spill 函数）：

```go
func (n *node) spill() error {
    // 1. 先 spill 子节点（后序遍历）
    for i := 0; i < len(n.children); i++ {
        n.children[i].spill()
    }

    // 2. 分裂当前节点为多个节点（如果需要）
    var nodes = n.split(uintptr(tx.db.pageSize))
    for _, node := range nodes {
        // 3. 关键：如果节点有旧 pgid（不是新创建的），将旧页加入 freelist
        if node.pgid > 0 {
            tx.db.freelist.Free(tx.meta.Txid(), tx.page(node.pgid))
            node.pgid = 0
        }

        // 4. 分配新页
        p, err := tx.allocate((node.size() + tx.db.pageSize - 1) / tx.db.pageSize)

        // 5. 写入新页
        node.pgid = p.Id()
        node.write(p)
        node.spilled = true

        // 6. 更新父节点指向新的 pgid
        if node.parent != nil {
            node.parent.put(key, node.inodes[0].Key(), nil, node.pgid, 0)
        }
    }

    // 7. 如果根节点分裂产生了新父节点，递归 spill
    if n.parent != nil && n.parent.pgid == 0 {
        return n.parent.spill()
    }
}
```

COW 的关键点：

1. **后序遍历**：从叶子向上 spill，子节点先获得新 pgid，父节点才能正确引用
2. **旧页不覆写**：所有被修改过的节点（有 pgid > 0）的旧页都通过 `freelist.Free(txid, page)` 放入 pending 列表，而不是被覆盖
3. **分配新页**：`tx.allocate()` 从 freelist（已释放的真正空闲页）或从高水位分配全新页
4. **递归向上**：当根节点分裂产生新父节点时，这个新父节点 pgid=0，也需要 spill 分配页；这就是树高度增长的时刻
5. **旧页暂存到 pending**：它们不会立刻回到 freelist 可分配池（而是挂在该 txid 下），直到 `ReleasePendingPages()` 确认没有任何更老的只读事务可能引用它们

---

## 5. Freelist 的两种实现对比

### 5.1 通用接口与共享状态

接口定义在 [freelist.go#L19-L82](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/freelist.go#L19-L82)。

共享状态在 [shared.go#L18-L25](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/shared.go#L18-L25)：

```go
type shared struct {
    readonlyTXIDs []Txid               // 所有活跃只读事务的 txid
    allocs        map[Pgid]Txid        // 页 -> 分配它的写事务 txid
    cache         map[Pgid]struct{}    // 所有 free + pending 页的快速查询集合
    pending       map[Txid]*txPending  // txid -> 该事务释放的页列表
}
```

### 5.2 数组式 freelist（array）

代码在 [array.go#L10-L108](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/array.go#L10-L108)。

#### 数据结构

```go
type array struct {
    *shared
    ids []Pgid  // 排序的、可用的空闲页 ID 列表
}
```

#### 分配（Allocate）

[array.go#L21-L61](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/array.go#L21-L61)：

- 线性扫描 `ids` 数组，找连续 n 个页（`id - initial + 1 == n`）
- 找到后从数组中移除这 n 个页，从 cache 删除，记录分配 txid
- 时间复杂度：O(free_pages)，大数据库下慢

#### 释放回空闲池（mergeSpans）

[array.go#L71-L99](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/array.go#L71-L99)：

- 将新释放的页排序后与已有 `ids` 做归并（`common.Pgids.Merge`）
- 保持 `ids` 数组始终有序

### 5.3 哈希表式 freelist（hashmap）

代码在 [hashmap.go#L14-L308](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/hashmap.go#L14-L308)。

#### 数据结构

```go
type hashMap struct {
    *shared
    freePagesCount uint64
    freemaps       map[uint64]pidSet  // span大小 -> 该大小span的起始pgid集合
    forwardMap     map[Pgid]uint64    // 起始pgid -> span大小
    backwardMap    map[Pgid]uint64    // 结束pgid -> span大小
}
```

这是一个**按 span（连续空闲区）组织**的哈希结构：

- `freemaps[size]` 快速定位特定大小的 span
- `forwardMap`/`backwardMap` 用于 O(1) 检查相邻页是否空闲以便合并

#### 分配（Allocate）

[hashmap.go#L61-L106](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/hashmap.go#L61-L106)：

1. 先精确匹配：`freemaps[n]` 中找，找到直接从该 span 分配
2. 不精确匹配：遍历 freemaps 找第一个 size ≥ n 的 span
3. 如果 span 比需要的大，分配前 n 页，剩余部分重新作为一个 span 加入
4. 平均 O(1)~O(log n) 时间复杂度

#### 连续多页分配与合并

合并发生在 `mergeWithExistingSpan`（[hashmap.go#L222-L247](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/hashmap.go#L222-L247)）：

- 释放 `[start, end]` 时检查 `prev = start-1` 和 `next = end+1` 是否在 backwardMap/forwardMap 中
- 如果有，合并成大 span，更新三个 map
- 这保证了 freelist 中始终是最大可能的连续空闲区

#### 性能对比

| 维度           | array                | hashmap                    |
| -------------- | -------------------- | -------------------------- |
| Allocate 时间  | O(N) 线性扫描        | O(1)~O(M) 按 span 大小查找 |
| 大数据库下性能 | 严重退化（碎片多时） | 稳定高效                   |
| 分配最小页号   | 保证（从头部扫描）   | 不保证                     |
| 默认类型       | 是（历史原因）       | 将来计划改为默认           |

### 5.4 pending 与真正可复用的关键区别

#### 什么是 pending 页？

当写事务调用 `freelist.Free(txid, page)` 时（[shared.go#L56-L87](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/shared.go#L56-L87)）：

```go
func (t *shared) Free(txid common.Txid, p *common.Page) {
    txp := t.pending[txid]  // 按释放它的事务 txid 分组
    if txp == nil {
        txp = &txPending{}
        t.pending[txid] = txp
    }
    for id := p.Id(); id <= p.Id()+Pgid(p.Overflow()); id++ {
        txp.ids = append(txp.ids, id)       // 加入 pending[txid]
        txp.alloctx = append(txp.alloctx, allocTxid)  // 记录分配该页的原始 txid
        t.cache[id] = struct{}{}
    }
}
```

pending 页**已经被标记为空闲但不能被立即分配**，它们只存在于 `pending[txid]` 中，不在 array.ids 或 hashmap 的 freemaps 里。

#### 为什么不能立即复用？——呼应第 1 问的快照语义

因为**可能存在比该写事务更早开启的只读事务仍在运行**，这些只读事务的 meta 是在旧 txid 上的，它们的 B+ 树遍历仍会引用到这些"已被释放"的旧页。

如果立即复用并覆写这些旧页：

1. 老只读事务遍历 B+ 树时可能读到被新事务覆写的页内容（键值变了或者页类型变了，直接解析错误）
2. 违反了快照隔离：只读事务应该看到它开启时刻一致的数据，而不是中途被其他事务篡改的数据

这就是 MVCC 中经典的"不能回收旧版本直到没有活跃事务引用它们"的问题。

#### 什么时候才能真正释放？ReleasePendingPages

代码在 [shared.go#L141-L158](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/shared.go#L141-L158)，在**每次写事务开始时调用**（[db.go#L870](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L870)）：

```go
func (t *shared) ReleasePendingPages() {
    // 1. 所有活跃只读事务排序
    sort.Sort(txIDx(t.readonlyTXIDs))
    minid := MaxUint64
    if len(t.readonlyTXIDs) > 0 {
        minid = t.readonlyTXIDs[0]  // 最老的活跃只读事务 txid
    }
    // 2. 释放所有 txid < minid 的 pending 页（这些事务已结束）
    if minid > 0 {
        t.release(minid - 1)
    }
    // 3. 处理事务 txid 区间（处理同一区间 alloc+free 的特殊情况）
    for _, tid := range t.readonlyTXIDs {
        t.releaseRange(minid, tid-1)
        minid = tid + 1
    }
    t.releaseRange(minid, MaxUint64)
}
```

核心逻辑：**找出当前最老的活跃只读事务 txid（minid），所有在 minid 之前的事务所释放的页都可以安全回到真正的空闲池**——因为任何还在运行的只读事务的 txid ≥ minid，它们的快照是在 minid 或之后创建的，而 minid 之前的事务修改并释放的旧版本不会被它们引用（它们看到的是新版本）。

`release(txid)` 将 `pending[tid]` 中所有 tid ≤ txid 的页收集起来，调用 `mergeSpans(m)` 合并到 array.ids 或 hashmap 的 span 结构中（[shared.go#L160-L171](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/shared.go#L160-L171)），这时这些页才真正可被分配。

**呼应问题 1**：这正是只读事务需要注册 txid（`AddReadonlyTXID`）、结束时注销（`RemoveReadonlyTXID`）的原因——freelist 需要知道哪些旧版本还能被看见。

**隐式风险点**：如果用户开启了只读事务但忘记 Rollback/Close，这些页永远不会释放，数据库文件会持续增长！这就是 [tx.go#L24-L26](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L24-L26) 注释警告的原因。

---

## 6. mmap 按需重映射机制与 cursor 遍历

### 6.1 mmap 初始映射

在 `Open()` 中调用 `db.mmap()`（[db.go#L297](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L297)），mmap 函数在 [db.go#L456-L553](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L456-L553)：

1. 获取 `mmaplock.Lock()`
2. 确定 mmap 大小（至少文件大小）
3. 如果有活跃写事务，先调用 `db.rwtx.root.dereference()`（让节点把 key/value 从指向 mmap 拷贝到堆内存，因为 mmap 地址会变）
4. `munmap()` 解除旧映射
5. `mmap(db, size)` 建立新映射（平台相关，在 bolt_windows.go / bolt_unix.go 中实现）
6. 重新设置 `db.meta0`、`db.meta1` 指针指向新映射中的页
7. 校验两个 meta

### 6.2 文件增长策略与 mmap 重映射容量策略

当写事务分配页超过当前 mmap 大小时触发 `db.mmap(minsz)`（[db.go#L1205-L1213](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L1205-L1213)，在 `allocate()` 中调用）。

`mmapSize()` 计算新映射大小（[db.go#L581-L613](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L581-L613)）：

```go
func (db *DB) mmapSize(size int) (int, error) {
    // 32KB 到 1GB 之间：每次翻倍
    for i := uint(15); i <= 30; i++ {
        if size <= 1<<i {
            return 1 << i, nil
        }
    }
    // 超过 1GB：每次加 1GB
    sz := int64(size)
    if remainder := sz % int64(MaxMmapStep); remainder > 0 {
        sz += int64(MaxMmapStep) - remainder
    }
    // 对齐到页大小
    // ...
    return int(sz), nil
}
```

增长策略：

- 小文件阶段（≤1GB）：指数增长，每次翻倍（32KB → 64KB → 128KB → ... → 1GB）
- 大文件阶段（>1GB）：线性增长，每次至少加 1GB（`MaxMmapStep = 1GB`）
- 总是对齐到系统页大小

`db.grow()` 还会根据需要扩展文件大小（truncate）（[db.go#L1223-L1261](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L1223-L1261)）。

### 6.3 为什么有事务活跃时不能随意 remap？

1. **只读事务持有 `mmaplock.RLock()`**：从 beginTx 到 Rollback 一直持有。而 `mmap()` 需要 `mmaplock.Lock()`（写锁）。因此有任何只读事务活跃时，mmap() 会阻塞在获取写锁上，直到所有只读事务结束。这就是 [db.go#L758-L759](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L758-L759) 注释警告的：在同一 goroutine 中开读事务和写事务会死锁——写事务需要 remap 时等待读事务释放 mmap 读锁，但读事务在同一 goroutine 永远不会结束。

2. **指针失效问题**：mmap 重映射后，`db.data` 指向新的内存地址，旧地址被 munmap。如果有任何代码（如 node 中的 inode key/value 指针、cursor 中的 elemRef）仍指向旧 mmap 地址，解引用会直接 SIGSEGV 或读到垃圾数据。为处理这个问题：
   - `mmap()` 前调用 `rwtx.root.dereference()`（[db.go#L504-L506](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L504-L506)）→ 递归调用 `node.dereference()`（[node.go#L463-L491](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L463-L491)）将所有 key/value 拷贝到堆内存，不再指向 mmap
   - 只读事务**不做**这个拷贝（因为只读事务不应该在 remap 期间存活，mmap 读锁保证了这一点）

3. **写事务如何协调重映射**：写事务**不持有** `mmaplock`（写事务 begin 时只拿 rwlock 和 metalock，不拿 mmaplock），所以写事务可以安全调用 mmap()（它内部获取 mmaplock.Lock()）。写事务的 node 在 dereference() 后用堆副本，mmap 完成后，后续页访问通过 `tx.page(id)` 重新使用新的 mmap 地址。

### 6.4 cursor 栈式定位与顺序遍历

Cursor 结构定义在 [cursor.go#L22-L25](file:///e:/gsb/617/gsb_11/Jupiter/cursor.go#L22-L25)：

```go
type Cursor struct {
    bucket *Bucket
    stack  []elemRef  // 栈：从 root 到 leaf 的路径
}

type elemRef struct {
    page  *common.Page
    node  *node
    index int  // 当前层级的元素索引
}
```

#### 搜索定位（Seek/search）

[ cursor.go#L282-L302](file:///e:/gsb/617/gsb_11/Jupiter/cursor.go#L282-L302)：

1. 从 root pgid 开始，将当前页/节点压栈
2. 如果是叶子节点，在叶子中做二分搜索定位 index（`nsearch`）
3. 如果是分支节点，二分搜索找最大的 ≤ 目标 key 的分隔键对应的子页，递归向下搜索
4. 最终栈顶是目标叶子页/节点，ref.index 是叶子内的索引

分支节点二分搜索逻辑（searchPage/searchNode）：注意 B+ 树分支节点索引的含义：它找到"第一个 ≥ key 的位置"，如果不是精确匹配则回退一个 index（即最右的 ≤ key 的子节点）。

#### 顺序遍历 Next

[cursor.go#L215-L247](file:///e:/gsb/617/gsb_11/Jupiter/cursor.go#L215-L247)：

1. 从栈顶开始向上找第一个 `index < count()-1` 的层
2. 把该层 index++
3. 截断栈到该层之上，然后调用 `goToFirstElementOnTheStack()`——从该层开始，沿每层 index=0 的子节点一直走到叶子
4. 这样就到达了下一个叶子的第一个元素（叶子间通过 B+ 树兄弟隐含连接，不需要兄弟指针，通过回溯父节点即可）

#### 反向遍历 Prev

[cursor.go#L251-L280](file:///e:/gsb/617/gsb_11/Jupiter/cursor.go#L251-L280)：

1. 从栈顶向上找第一个 `index > 0` 的层
2. index--
3. 然后调用 `last()` 沿每层最右子节点走到叶子

#### First/Last

- First：root 入栈，沿每层 index=0 一直走到最左叶子
- Last：root 入栈，沿每层 index=count-1 一直走到最右叶子

#### 关键设计

- 栈上每个 elemRef 既可能指向磁盘 page（只读时）也可能指向内存 node（写事务时已 materialize 的节点），`pageNode()` 函数返回任意一种，cursor 代码对两者统一处理
- 叶子节点间**没有显式兄弟链表指针**——遍历通过栈回溯实现，这简化了实现，代价是顺序遍历在跨页时有 O(log n) 的回溯开销

---

## 7. 容易被忽视但对正确性至关重要的隐式不变量与风险点

### 风险点 1：mmap 重映射使既有页指针失效的风险

**问题描述**：所有指向 mmap 内存的指针（page 指针、从 page 直接引用的 key/value 字节切片）在 `db.mmap()` 调用后全部失效（因为 munmap 了旧地址，映射了新地址）。在错误的时机持有这些指针会导致 use-after-unmap、SIGSEGV 或读到垃圾数据。

**代码依据与保护机制**：

1. [db.go#L801](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L801)：只读事务 begin 时获取 `mmaplock.RLock()`，整个事务期间持有，事务结束时（[db.go#L877](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L877)）才释放。而 `mmap()` 第一步就是 `mmaplock.Lock()`（[db.go#L457](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L457)）。这保证只读事务存活时不会发生 remap，它们的页指针始终有效。

2. [db.go#L504-L506](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L504-L506)：写事务调用 mmap() 之前先执行 `db.rwtx.root.dereference()`。dereference 递归将所有 inode 的 key/value 从指向 mmap 的 unsafe 切片复制到堆内存（[node.go#L463-L491](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L463-L491)）。

3. **Bucket.Get 返回值的生命周期警告**（[bucket.go#L430-L432](file:///e:/gsb/617/gsb_11/Jupiter/bucket.go#L430-L432)）注释明确说明："The returned value is only valid for the life of the transaction."——不能在 Rollback/Commit 后继续使用返回的 []byte，它要么指向 mmap（只读 tx），要么可能在后续 spill 中被重分配。

**如果违反会怎样**：用户把 Get 返回的 []byte 保存到事务结束后，下次写事务触发 remap，该指针指向的内存已被 munmap——Go 中表现为"有时候正常有时候 SIGSEGV"的诡异 bug，难以复现。

### 风险点 2：freelist 过早复用导致只读事务读到被覆盖页的风险

**问题描述**：如果一个页刚被释放就立刻被分配给新的写事务并覆写，而此时仍有老只读事务在运行并持有指向旧页的路径，只读事务的 B+ 树遍历会读到新写入的数据（可能是不相关的键值、错误的页类型），轻则 panic（page type check 失败、FastCheck 失败），重则静默返回错误数据。

**代码依据与保护机制**：

1. `Free()` 函数只把页放入 `pending[txid]`（[shared.go#L62-L86](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/shared.go#L62-L86)），**不直接放入可用空闲列表**。

2. 只有 `ReleasePendingPages()` 才把 pending 页搬到真正空闲池，而且只搬那些 txid < minid 的（[shared.go#L148-L149](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/shared.go#L148-L149)），其中 minid 是当前最老的活跃只读事务 txid：

   ```go
   minid = t.readonlyTXIDs[0]  // 最老的
   t.release(minid - 1)        // 只能释放比它老的事务所释放的页
   ```

3. 只读事务 begin 时必须调用 `AddReadonlyTXID(txid)`（[db.go#L822](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L822)），结束时调用 `RemoveReadonlyTXID(txid)`（[db.go#L883](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L883)）。

**容易被忽视的场景**：用户开了一个只读事务，做了一个长耗时操作（比如网络请求、复杂计算），期间写事务大量提交——所有被 COW 替换的旧页都堆积在 pending 里，文件大小持续增长。如果用户忘记 Rollback 只读事务（比如 panic 路径上漏了 Rollback），这些页永远不会释放，最终磁盘耗尽。这在文档中被反复警告（[tx.go#L23-L26](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L23-L26)、[db.go#L765-L766](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L765-L766)）。

### 风险点 3：meta 选择/校验错误导致回退到旧状态的风险

**问题描述**：两个 meta 页交替写入，重启时选择哪个 meta 决定了恢复到哪次 commit。如果 meta 选择逻辑出错（比如总是选 txid 大的而不校验，或者校验逻辑有 bug），可能选择到一个 torn write 的损坏 meta，或者在需要时不能正确回退到上一个有效 meta。

**代码依据与保护机制**：

1. 选择逻辑（[db.go#L1140-L1162](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L1140-L1162)）：先按 txid 排序，优先选高 txid 的，但如果高的 Validate 失败则选低的。Validate 必须同时检查 magic、version、checksum（[meta.go#L25-L34](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/meta.go#L25-L34)），缺一不可。

2. 校验和覆盖范围：[meta.go#L61-L65](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/meta.go#L61-L65) 只计算 `unsafe.Offsetof(Meta{}.checksum)` 之前的字段（即除 checksum 自身外所有字段）。如果有人错误地把 checksum 字段也纳入校验，或者反过来漏了某些字段，校验就失效了。

3. **meta 写入顺序的隐蔽依赖**：`writeMeta()` 中 meta 的 `p.id = Pgid(m.txid % 2)` 决定写哪个页（[meta.go#L51](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/meta.go#L51)）——txid 偶写 page0，奇写 page1。这保证了每次 commit 写的都是"上上次"用的那个页，而两个页中总有一个是完整的。如果这个算法错误（比如总是写 page0），那么双 meta 设计完全失效——任何一次 torn write 都会导致数据库损坏。

**如果违反会怎样**：如果双 meta 选了校验失败的那个（比如跳过 checksum 校验），meta 中的 root pgid、freelist pgid、pgid 高水位等字段可能全是垃圾数据，接下来所有操作都会基于错误的元数据，直接导致数据库损坏。bbolt 在 `mmap()` 初始化时检查两个 meta，如果**两个都**无效才报错；只有一个无效则使用另一个，这种容错是崩溃恢复的核心。

### 风险点 4：写事务在同一 goroutine 中等待自己（死锁风险）

**问题描述**：[db.go#L757-L760](file:///e:/gsb/617/gsb_11/Jupiter/db.go#L757-L760) 注释明确警告：

> "Opening a read transaction and a write transaction in the same goroutine can cause the writer to deadlock because the database periodically needs to re-mmap itself as it grows and it cannot do that while a read transaction is open."

**代码依据**：

- 只读事务持有 `mmaplock.RLock()`
- 写事务如果需要扩展文件分配新页，会调用 `db.mmap()` 需要 `mmaplock.Lock()`
- 如果两者在同一 goroutine，写事务阻塞等待读事务释放读锁，但读事务在等待写事务完成（同一 goroutine 不会继续执行到 Rollback）——经典的自死锁。

官方建议：如果必须在有长只读事务时写数据，设置足够大的 `InitialMmapSize` 避免写事务需要 remap。

### 风险点 5：spill/rebalance 中节点 pgid 管理的不变量

**问题描述**：spill 过程中，所有被修改节点的旧 pgid 必须在分配新页之前正确 Free 到 pending，且父节点引用必须更新到新 pgid。

**代码依据**：[node.go#L316-L321](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L316-L321) 的顺序**非常关键**——必须先 Free 旧页（放入 pending），再 allocate 新页。如果顺序反过来，allocate 可能从 freelist 分到同一个页（刚被 free 的但还没放入 pending？实际上 Free 会加 cache 阻止，但逻辑上这个顺序表达了 COW 语义）。更重要的是：Free 旧页时 `node.pgid = 0`，避免同一个节点被多次 spill 时重复 free 同一页（spilled 标志也在保护这一点）。

同样在 rebalance 中合并节点后，`child.free()` 必须在从 bucket.nodes map 中 delete 之后调用（[node.go#L399-L400](file:///e:/gsb/617/gsb_11/Jupiter/node.go#L399-L400)），防止节点泄漏或 double free。

### 风险点 6：write 脏页排序的 I/O 顺序假设

**代码依据**：[tx.go#L527-L529](file:///e:/gsb/617/gsb_11/Jupiter/tx.go#L527-L529) 中脏页写入前先 `sort.Sort(pages)` 按 page id 升序写入。

虽然 bbolt 不像某些数据库那样依赖"页号顺序对应磁盘物理位置"来保证崩溃一致性，但按页号升序写主要是为了：

1. 更好的 I/O 合并（连续页号对应可能连续的磁盘扇区）
2. 保证 meta 页最后写——但 meta 页是单独在 writeMeta 中写的，这里排序不影响 meta
3. freelist 页和数据页之间的顺序——但因为有 fsync 分隔，排序只是性能优化，不是正确性依赖

真正的正确性保证是**先 fsync 数据页，再写+fsync meta 页**这个两阶段，而不是页写入顺序。

---

## 附录：核心文件索引

| 文件                                                                                           | 职责                                                    |
| ---------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| [db.go](file:///e:/gsb/617/gsb_11/Jupiter/db.go)                                               | DB 结构、Open/Close、事务 begin、mmap、页分配           |
| [tx.go](file:///e:/gsb/617/gsb_11/Jupiter/tx.go)                                               | Tx 结构、Commit/Rollback、write/writeMeta               |
| [bucket.go](file:///e:/gsb/617/gsb_11/Jupiter/bucket.go)                                       | Bucket API、spill/rebalance 调度、inline bucket         |
| [node.go](file:///e:/gsb/617/gsb_11/Jupiter/node.go)                                           | B+ 树节点内存结构、split、spill、rebalance、dereference |
| [cursor.go](file:///e:/gsb/617/gsb_11/Jupiter/cursor.go)                                       | 游标遍历：seek、first/last、next/prev                   |
| [internal/common/page.go](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/page.go)           | Page 磁盘结构、branch/leaf element                      |
| [internal/common/meta.go](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/meta.go)           | Meta 磁盘结构、校验和、校验逻辑                         |
| [internal/common/types.go](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/types.go)         | 核心常量（Magic、Version、MaxMmapStep等）               |
| [internal/freelist/shared.go](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/shared.go)   | pending 管理、ReleasePendingPages、Free/Rollback        |
| [internal/freelist/array.go](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/array.go)     | 数组式 freelist                                         |
| [internal/freelist/hashmap.go](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/hashmap.go) | 哈希表/span 式 freelist                                 |
| [internal/freelist/array.go](file:///e:/gsb/617/gsb_11/Jupiter/internal/freelist/array.go)     | 数组式 freelist                                         |
| [internal/common/inode.go](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/inode.go)         | Inode 内存结构（反序列化后的键值对）                    |
| [internal/common/bucket.go](file:///e:/gsb/617/gsb_11/Jupiter/internal/common/bucket.go)       | InBucket 磁盘结构（bucket 头部）                        |

---

## 总结

bbolt 作为一个经典的嵌入式 B+ 树 KV 存储，其设计哲学可以归纳为：

1. **无 WAL 设计**：通过双 meta + 严格写入顺序 + fsync 实现崩溃一致性，而非传统的 WAL 方案，简化了实现但要求写入顺序极度小心
2. **COW + MVCC**：所有修改不原地覆写，旧版本通过 pending freelist 延迟回收，保证只读事务无锁快照
3. **mmap 直读**：只读路径零拷贝（直接引用 mmap 指针），写路径通过 tx.pages 缓存隔离
4. **极简并发控制**：三把锁（rwlock/metalock/mmaplock）覆盖全部并发场景，写事务完全串行，读事务无阻塞读
5. **性能权衡选项**：NoSync/NoFreelistSync 等开关允许用户在持久性和性能之间做取舍

所有关键正确性不变量都围绕"meta 是 commit 指针"这一核心——只要 meta 是原子写入的（单页 + 校验和 + 双备份），且 meta 指向的所有页在它之前持久化，那么数据库就能在任何崩溃点回退到一个一致状态。
