# bbolt 深度代码理解分析

> 本文基于对 etcd-io/bbolt 源码的系统性分析，所有论据均可通过标注的文件与函数直接核对。

---

## 1. 事务生命周期与 MVCC 并发模型

### 1.1 核心锁结构

bbolt 的并发控制依赖 [db.go](file:///e:/gsb/617/gsb_11/Saturn/db.go#L145-L148) 中定义的三把互斥锁：

```go
rwlock   sync.Mutex   // 全局写锁，保证至多一个读写事务
metalock sync.Mutex   // 保护 meta 页访问
mmaplock sync.RWMutex // 保护 mmap 重映射（读事务持读锁，重映射需写锁）
```

### 1.2 只读事务（RO Tx）生命周期

**开启阶段**：[db.go:beginTx()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L792-L837)

1. 加 `metalock.Lock()`（短暂持有，仅用于读取 meta 快照）
2. 加 `mmaplock.RLock()`（**持有整个事务生命周期**，阻止 mmap 重映射）
3. 校验 DB 状态后，创建 `Tx` 对象，调用 [tx.go:init()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L47-L65)：
   - **拷贝**当前 `db.meta()` 到 `tx.meta`（这是一致性快照的关键！）
   - 拷贝根 bucket 信息
   - 只读事务**不**递增 txid，也**不**创建 `tx.pages` 缓存
4. 调用 `db.freelist.AddReadonlyTXID(t.meta.Txid())` 登记此只读事务看到的快照版本
5. 释放 `metalock.Unlock()`

**执行阶段**：
- 通过 `Bucket.Get()`/`Cursor` 等读取数据
- 页访问通过 [tx.go:page()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L629-L642) 直接从 mmap 读取（因为只读事务无脏页缓存）
- **为什么能看到一致快照**：`tx.meta` 是事务开启时的 meta 拷贝，根指针固定指向该版本的 B+树根；mmap 读锁保证在此期间 mmap 不会被重映射导致指针失效；COW 保证旧版本页不会被原地修改。

**收尾阶段**：调用 `Rollback()`（只读事务不能 Commit）→ [tx.go:close()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L345-L378) → [db.go:removeTx()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L875-L896)：
1. 释放 `mmaplock.RUnlock()`
2. 加 `metalock.Lock()`，调用 `freelist.RemoveReadonlyTXID(txid)` 注销自身
3. 释放 `metalock.Unlock()`

### 1.3 读写事务（RW Tx）生命周期

**开启阶段**：[db.go:beginRWTx()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L839-L872)

1. 加 `rwlock.Lock()`（**持有整个事务生命周期**，串行化所有写事务）
2. 加 `metalock.Lock()`
3. 创建 `Tx{writable: true}`，调用 `init()`：
   - 拷贝 meta 到 `tx.meta`
   - 创建 `tx.pages = make(map[Pgid]*Page)` 脏页缓存
   - 创建 bucket/node 缓存
   - **调用 `tx.meta.IncTxid()`** —— txid 在此单调递增！
4. 设置 `db.rwtx = t`
5. 调用 `db.freelist.ReleasePendingPages()` —— 关键步骤：在新写事务开始时，将不再被任何老只读事务引用的 pending 页释放到可分配空闲列表
6. 注意：`metalock` 在 beginRWTx 返回时通过 `defer` 释放，但 `rwlock` 一直持有到事务结束

**执行阶段**：
- `Put/Delete/CreateBucket` 等操作仅修改内存中的 node 缓存（[node.go:put()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L117-L142)、[node.go:del()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L145-L159)）
- 首次访问某页时，通过 [bucket.go:node()](file:///e:/gsb/617/gsb_11/Saturn/bucket.go#L863-L901) 将页反序列化为 node 对象并加入 `b.nodes` 缓存
- 修改后 node 被标记（`unbalanced=true` 表示删除后需要 rebalance）
- 页访问优先查 `tx.pages` 脏页缓存，否则查 mmap（[tx.go:page()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L629-L642)）

**Commit 阶段**：详见第3节。

**Rollback 阶段**：[tx.go:rollback()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L323-L343)
1. 调用 `freelist.Rollback(tx.meta.Txid())` 撤销本事务的 pending 释放和分配
2. 若 freelist 是持久化的，从磁盘重载 freelist；否则全库扫描重建
3. 调用 `close()`：
   - 置 `db.rwtx = nil`
   - 释放 `db.rwlock.Unlock()`（给下一个写事务让路）

### 1.4 MVCC 并发模型总结

| 问题 | 答案 | 代码依据 |
|------|------|---------|
| 为何允许多个只读事务并发？ | 只读事务仅持有 `mmaplock.RLock()`（共享读锁），不修改任何数据，通过拷贝 meta 获取快照 | [db.go:beginTx()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L792-L837) |
| 为何至多一个读写事务？ | 写事务开启即获取 `rwlock`（互斥锁），直到 Commit/Rollback 才释放 | [db.go:beginRWTx()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L847) |
| 只读事务为何看到一致快照？ | 1) `tx.init()` 拷贝 meta（根指针、pgid、freelist指针固定）；2) COW：写不覆盖旧页；3) `mmaplock.RLock()` 阻止 mmap 重映射使指针失效 | [tx.go:init()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L52-L58) |
| txid 如何单调推进？ | 仅读写事务在 `init()` 时调用 `meta.IncTxid()`；只读事务使用其开启时的最新 meta txid | [tx.go:init()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L63) |
| 写事务间如何串行化？ | 全局 `rwlock sync.Mutex`，beginRWTx 时 Lock，close 时 Unlock | [db.go](file:///e:/gsb/617/gsb_11/Saturn/db.go#L145) |

---

## 2. 磁盘页布局与双 Meta 崩溃恢复

### 2.1 页（Page）结构

磁盘文件被划分为固定大小的页（默认 OS 页大小，通常 4KB）。页头定义在 [common/page.go](file:///e:/gsb/617/gsb_11/Saturn/internal/common/page.go#L31-L36)：

```go
type Page struct {
    id       Pgid    // 页号
    flags    uint16  // 页类型标志
    count    uint16  // 元素个数
    overflow uint32  // 后续连续 overflow 页数
}
```

**页类型**（[common/page.go:L18-L23](file:///e:/gsb/617/gsb_11/Saturn/internal/common/page.go#L18-L23)）：
- `BranchPageFlag = 0x01`：B+树内部（分支）节点
- `LeafPageFlag = 0x02`：B+树叶子节点
- `MetaPageFlag = 0x04`：元数据页
- `FreelistPageFlag = 0x10`：空闲页列表

**Overflow 页**：当一个节点的数据（key+value+element header）超过一页时，`overflow` 字段记录后续连续的物理页数。例如 `overflow=2` 表示该逻辑页占据 `p.id`, `p.id+1`, `p.id+2` 共3个物理页。分配时通过 [db.go:allocate()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L1165-L1220) 保证连续性。

**分支页元素**（[common/page.go:L225-L229](file:///e:/gsb/617/gsb_11/Saturn/internal/common/page.go#L225-L229)）：
```go
type branchPageElement struct {
    pos   uint32  // key 数据在页内的偏移
    ksize uint32  // key 长度
    pgid  Pgid    // 子节点页号
}
```
分支节点不存 value，仅存 key 和子页指针。

**叶子页元素**（[common/page.go:L261-L266](file:///e:/gsb/617/gsb_11/Saturn/internal/common/page.go#L261-L266)）：
```go
type leafPageElement struct {
    flags uint32  // 标志位（如 BucketLeafFlag 表示是子 bucket）
    pos   uint32  // key+value 数据在页内的偏移
    ksize uint32  // key 长度
    vsize uint32  // value 长度
}
```

### 2.2 Meta 结构

Meta 页定义在 [common/meta.go](file:///e:/gsb/617/gsb_11/Saturn/internal/common/meta.go#L12-L22)：

```go
type Meta struct {
    magic    uint32   // 魔数 0xED0CDAED
    version  uint32   // 版本号（当前为 2）
    pageSize uint32   // 页大小
    flags    uint32   // 标志位
    root     InBucket // 根 bucket（root=3）
    freelist Pgid     // freelist 页号（PgidNoFreelist 表示未持久化）
    pgid     Pgid     // 高水位线：下一个分配的页号
    txid     Txid     // 事务 ID
    checksum uint64   // 以上所有字段的 FNV-64a 校验和
}
```

校验和计算在 [common/meta.go:Sum64()](file:///e:/gsb/617/gsb_11/Saturn/internal/common/meta.go#L61-L65)：对 checksum 字段之前的所有字节做 FNV-64a 哈希。

### 2.3 双 Meta 页设计

数据库初始化时（[db.go:init()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L646-L689)），页0和页1都被初始化为 meta 页。初始布局：
- 页 0：meta0
- 页 1：meta1
- 页 2：空 freelist
- 页 3：空叶子页（根 bucket）

**为何需要两个 meta 页？**

因为 meta 页的写入是**单页、原子**的（一个页刚好是一个磁盘扇区/块大小的整数倍，一次 write 可原子完成）。交替写两个 meta 页可以保证：**任何时刻总有一个完整有效的 meta 页**。

写入选择规则（[common/meta.go:Write()](file:///e:/gsb/617/gsb_11/Saturn/internal/common/meta.go#L51)）：
```go
p.id = Pgid(m.txid % 2)  // txid 为偶写 meta0，奇写 meta1
```

**打开数据库时如何选择有效 meta**：[db.go:meta()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L1141-L1162)

1. 比较 `meta0.txid` 和 `meta1.txid`，较高的为 `metaA`（候选），较低的为 `metaB`
2. 先尝试 `metaA.Validate()`：校验 magic、version、checksum
3. 若 metaA 无效（写入中途崩溃，校验和不匹配），回退到 `metaB.Validate()`
4. 两者都无效则 panic（数据库严重损坏）

`Validate()` 逻辑（[common/meta.go:Validate()](file:///e:/gsb/617/gsb_11/Saturn/internal/common/meta.go#L25-L34)）：
- `magic` 必须等于 `Magic = 0xED0CDAED`
- `version` 必须等于 `Version = 2`
- `checksum` 必须匹配重新计算的值

### 2.4 崩溃恢复流程

断电/崩溃恢复由双 meta 的交替写入保证：

1. **写数据页阶段崩溃**：所有数据页写入后但 meta 未更新时，最新有效 meta 指向上一一致状态，新分配的页"泄漏"但不会被引用（因为 pgid 高水位线在 meta 中未更新）。重启后这些页不在 freelist 中也不在 B+ 树中，下次打开时若 freelist 未持久化（NoFreelistSync），全库扫描会发现它们是空闲的。

2. **写 meta 页阶段崩溃**：
   - 若 meta 页写了一半（checksum 不对）：Validate() 失败，自动选择另一个 txid 较低但完整的 meta
   - 若 meta 写完但 fsync 前崩溃：依赖 OS 页缓存可能丢失，但此时数据页已经 fsync 过（见 Commit 顺序），且 meta 是单页原子写，最坏情况回退到上一事务

3. **双 meta 是 Write-Ahead Logging (WAL) 的简化替代**：不需要 WAL，通过"先写所有数据页并 fsync，再原子更新 meta"实现一致性。

---

## 3. 读写事务 Commit 完整流程

Commit 入口在 [tx.go:Commit()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L170-L283)。以下按执行顺序分析：

### 3.1 步骤详解

**Step 1: Rebalance（再平衡）**——Commit:L195
```go
tx.root.rebalance()
```
递归遍历所有被修改过的 node（标记了 `unbalanced=true`），对删除后利用率过低的节点进行合并或向兄弟借元素。详见第4节。

**Step 2: Spill（分裂落盘）**——Commit:L204
```go
tx.root.spill()
```
递归将所有脏 node 分裂为合适大小并分配新页写入（COW 的核心！）。旧页在此时被加入 freelist 的 pending 队列。详见第4节。

**Step 3: 更新根指针**——Commit:L212
```go
tx.meta.RootBucket().SetRootPage(tx.root.RootPage())
```
spill 可能导致根节点分裂产生新根，所以更新 meta 中的根页号。

**Step 4: 释放旧 freelist 页**——Commit:L215-L217
```go
if tx.meta.Freelist() != common.PgidNoFreelist {
    tx.db.freelist.Free(tx.meta.Txid(), tx.db.page(tx.meta.Freelist()))
}
```
上一次 Commit 写的 freelist 页已过时，将其加入 pending 释放队列。

**Step 5: 写新 freelist**——Commit:L219-L227
```go
if !tx.db.NoFreelistSync {
    err = tx.commitFreelist()  // 分配新页、序列化 freelist、更新 meta.freelist
} else {
    tx.meta.SetFreelist(common.PgidNoFreelist)
}
```
分配页并写入新的 freelist（包含 free + pending），更新 meta 中的 freelist 指针。

**Step 6: 增长数据库文件**——Commit:L230-L240
```go
if tx.meta.Pgid() > opgid {
    tx.db.grow(int(tx.meta.Pgid()+1) * tx.db.pageSize)
}
```
如果高水位线 pgid 推进了，调用 `Truncate()` + `Sync()` 扩展文件大小并确保元数据落盘。

**Step 7: 写脏页到磁盘**——Commit:L244
```go
tx.write()
```
将 `tx.pages` 中的所有脏页按页号排序后调用 `writeAt` 写入，然后调用 `fdatasync`（除非 NoSync）。

**Step 8: 写 meta 页**——Commit:L267
```go
tx.writeMeta()
```
将 meta 写入 `txid % 2` 对应的页（0或1），计算 checksum，然后再次 `fdatasync`。

**Step 9: 关闭事务**——Commit:L275
```go
tx.close()
```
释放 rwlock，更新统计信息，执行 commitHandlers。

### 3.2 为什么必须是这个顺序？——崩溃一致性分析

关键顺序约束：

1. **数据页必须在 meta 之前写入并 fsync**（Step 7 在 Step 8 之前）：
   - meta 是"提交点"：一旦 meta 写入并 fsync 成功，事务就持久化了
   - 如果先写 meta 再写数据，崩溃后 meta 指向的页可能是旧数据或垃圾数据
   - 这就是 **WAL 原则**的变体：meta 充当"提交记录"，必须在所有数据持久化之后才能写入

2. **Spill 在写数据页之前**：spill 阶段完成所有页分配（包括 freelist 页）和脏页准备，此时 `tx.pages` 包含了所有需要写入的页。

3. **Freelist 必须在 meta 之前写入**：meta 中包含 freelist 指针。如果 meta 已写但 freelist 页还未写，崩溃后 meta 指向的 freelist 页包含垃圾数据。

4. **旧页释放（加入 pending）发生在 spill 阶段但真正复用要等**：spill 时调用 `freelist.Free(txid, oldPage)` 将旧页放入 pending，但这些页不会立即回到 free 列表（见第5节）。

5. **grow（Truncate + Sync）在 write 之前**：确保文件大小足够容纳所有新页，否则 writeAt 会失败或写入稀疏文件。

### 3.3 NoSync / NoFreelistSync 选项

| 选项 | 放松的保证 | 代价/取舍 | 代码位置 |
|------|-----------|----------|---------|
| `NoSync = true` | 跳过 Step 7 和 Step 8 的 `fdatasync()` 调用 | 写入性能大幅提升，但断电可能丢失最后已提交事务（包括可能损坏的 meta 写一半）。适用于批量导入等可重入场景 | [tx.go:write()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L566)、[tx.go:writeMeta()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L613) |
| `NoFreelistSync = true` | Step 5 不写 freelist 页，meta.freelist 设为 `PgidNoFreelist` | 写入更快（不用序列化写 freelist），但打开数据库时必须扫描全库重建 freelist（`db.freepages()` 遍历所有可达页），启动慢。适用于写多读少场景 | [tx.go:Commit()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L219-L227)、[db.go:loadFreelist()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L422-L436) |
| `NoGrowSync = true` | grow 时跳过 Truncate 后的 fsync | 在非 ext3/ext4 上安全，避免一次额外 fsync | [db.go:grow()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L1239) |

---

## 4. B+ 树 Spill（分裂落盘）与 Rebalance（合并/再平衡）

### 4.1 FillPercent（填充率）

定义在 [bucket.go:L20-L27](file:///e:/gsb/617/gsb_11/Saturn/bucket.go#L20-L27)：
- `DefaultFillPercent = 0.5`（默认50%）
- `minFillPercent = 0.1`（下限10%）
- `maxFillPercent = 1.0`（上限100%）

FillPercent 的作用：
1. **分裂阈值**（[node.go:splitTwo()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L237-L243)）：当节点序列化后大小超过 `pageSize * FillPercent` 时触发分裂。默认50%意味着分裂后两个节点各自约50%填充。
2. **合并阈值**（[node.go:rebalance()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L375)）：当节点大小小于 `pageSize * FillPercent / 2`（默认25%）且键数不足时触发合并/借用。

> 为什么默认是 50%？因为 B+ 树分裂后两个节点各约一半满，为后续插入预留空间；append-only 工作负载可设高 FillPercent（如 0.9+）以减少分裂、提升空间利用率。

### 4.2 Spill（分裂落盘）机制

入口：[bucket.go:spill()](file:///e:/gsb/617/gsb_11/Saturn/bucket.go#L746-L802) → [node.go:spill()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L295-L361)

**执行流程（后序遍历）**：

1. **递归 spill 子节点**（node.go:L304-L309）：先 spill 所有 children，确保子节点先获得新 pgid。

2. **递归 spill 子 bucket**（bucket.go:L748-L782）：对每个子 bucket，判断是否 inlineable（足够小且无子 bucket），是则内联写入父节点 value，否则递归 spill 子 bucket 后更新父节点中的 bucket header。

3. **分裂节点**（node.go:L315）：调用 `n.split(pageSize)` 将过大的节点拆分为多个节点。
   - `splitTwo()`：检查节点是否能放入一页，否则找到 splitIndex（保证第二页至少有 MinKeysPerPage=2 个键），将 inodes 拆分为两个节点
   - 若原节点无 parent（是根），创建新的父节点
   - 循环分裂直到所有节点都能放入一页

4. **COW 核心：分配新页、释放旧页**（node.go:L317-L335）：
   ```go
   for _, node := range nodes {
       // 旧页加入 pending 释放（非新节点）
       if node.pgid > 0 {
           tx.db.freelist.Free(tx.meta.Txid(), tx.page(node.pgid))
           node.pgid = 0
       }
       // 分配新的连续页
       p, err := tx.allocate((node.size() + tx.db.pageSize - 1) / tx.db.pageSize)
       // 将节点写入新页
       node.pgid = p.Id()
       node.write(p)
       node.spilled = true
       // 更新父节点中的子指针
       if node.parent != nil {
           node.parent.put(key, node.inodes[0].Key(), nil, node.pgid, 0)
       }
   }
   ```
   这就是**写时复制（Copy-On-Write）**：脏节点不修改旧页，而是分配新页写入，旧页交还给 freelist（pending）。由于父节点的子指针也被更新了，父节点也变脏，最终会递归 spill 到根，形成一条从修改点到根的"脏路径"。

5. **根分裂处理**（node.go:L355-L358）：如果根节点分裂产生了新根，继续 spill 新根。

### 4.3 Rebalance（合并/再平衡）机制

入口：[bucket.go:rebalance()](file:///e:/gsb/617/gsb_11/Saturn/bucket.go#L853-L860) → [node.go:rebalance()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L365-L448)

**触发条件**：节点在 `del()` 后被标记 `unbalanced = true`。Commit 时仅对标记了的节点做 rebalance。

**执行流程**：

1. **阈值检查**（node.go:L375-L378）：如果节点大小 > threshold（默认页大小的25%）且键数 > minKeys（叶子1个，分支2个），无需合并，直接返回。

2. **根节点特殊处理**（node.go:L381-L404）：如果是分支根且只有一个子节点，将该子节点提升为新根（树高度降1）。

3. **空节点删除**（node.go:L407-L414）：节点无键，直接从父节点删除，递归 rebalance 父节点。

4. **选择合并方向**（node.go:L419-L427）：如果是父节点的第一个孩子，与右兄弟合并；否则与左兄弟合并。

5. **合并节点**（node.go:L431-L444）：
   - 将右兄弟的子节点（children）重设父指针后移到左节点
   - 将右兄弟的所有 inodes 追加到左节点
   - 从父节点删除右兄弟的键，删除右节点
   - 右节点的旧页调用 `free()` 加入 pending
   - 递归 rebalance 父节点

> 注意：bbolt 的 rebalance **只做合并，不做"向兄弟借用"**（即经典 B+ 树的 redistribute）。这是简化设计：不借用只合并，可能导致节点填充率略低，但逻辑简单且避免了复杂的键传播。合并后如果父节点键不足，会递归向上合并。

---

## 5. Freelist 两种实现与 Pending 页机制

Freelist 接口定义在 [freelist/freelist.go](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/freelist.go#L19-L82)。共享逻辑在 [freelist/shared.go](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/shared.go)。

### 5.1 两种实现对比

| 特性 | Array 实现 | HashMap 实现 |
|------|-----------|-------------|
| 文件 | [freelist/array.go](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/array.go) | [freelist/hashmap.go](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/hashmap.go) |
| 数据结构 | `ids []Pgid` 有序数组 | `freemaps map[uint64]pidSet`（span 大小→起始页号集合）+ `forwardMap/backwardMap`（双向映射页区间） |
| 分配策略 | 线性扫描 `ids` 找连续 n 页，找到后删除 | 先精确匹配 size=n 的 span；若无则找 size>n 的 span，分割后返回前 n 页，剩余部分重新加入 |
| 连续多页分配 | 遍历数组，跟踪 `initial` 和 `previd`，当 `id-initial+1 == n` 时找到连续块 | `freemaps[n]` 直接给出 size=n 的所有起始位置；或从大 span 分割 |
| 时间复杂度 | O(N) 分配，O(N) 合并 | O(1)~O(K) 分配（K 为 span 大小种类数），O(1) 合并 |
| 空间分配特点 | 倾向分配最小可用页号（因为 ids 有序） | 不保证最小页号，但性能更好 |
| 默认 | 是（`FreelistArrayType`） | 否（需显式设置 `FreelistMapType`） |

### 5.2 Array 分配实现细节

[freelist/array.go:Allocate()](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/array.go#L21-L61)：线性扫描有序数组，维护当前连续块起始 `initial`，当连续长度达到 n 时从数组中删除这 n 个页。

### 5.3 HashMap 分配实现细节

[freelist/hashmap.go:Allocate()](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/hashmap.go#L61-L106)：
1. 精确匹配：若 `freemaps[n]` 非空，从中取一个 span 直接返回
2. 近似匹配：遍历所有 size >= n 的 span，取第一个，分割为 n 页返回，剩余 `size-n` 页作为新 span 加入

**Span 合并**：[hashmap.go:mergeWithExistingSpan()](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/hashmap.go#L222-L247) 利用 `forwardMap`（起始→大小）和 `backwardMap`（结束→大小）检查新释放的页是否与前后已有空闲 span 相邻，是则合并为更大的 span。这保证了空闲页始终以最大连续区间组织。

### 5.4 Pending 页机制——为什么释放的页不能立即复用？

这是与 MVCC 快照语义紧密关联的核心设计。

**三个关键数据结构**（[freelist/shared.go:L18-L25](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/shared.go#L18-L25)）：
- `ids`（array）/ `freemaps`（hashmap）：**真正可分配**的空闲页
- `pending map[Txid]*txPending`：按事务 id 暂存的"已释放但不可复用"的页
- `readonlyTXIDs []Txid`：当前活跃的只读事务 id 列表
- `cache map[Pgid]struct{}`：所有空闲+pending 页的快速查找集合

**为什么需要 pending？**

考虑以下场景：
1. 事务 T5（读写）正在进行
2. 有一个长只读事务 T3 还活着，它的 meta 快照是 txid=3
3. T5 spill 时释放了页 P（该页在 T3 的快照中仍被 B+ 树引用）
4. 如果 P 立即回到 free 列表并被分配给 T5 的新页，T5 会覆盖 P 的内容
5. 此时 T3 若通过 cursor 访问 P，读到的是 T5 写入的新数据——**快照隔离被破坏！**

**pending 与真正 free 的区别**：

- 当写事务 W 调用 `freelist.Free(wTxid, page)` 时（spill 阶段）：
  - 页被加入 `pending[wTxid].ids`
  - 页被加入 `cache`（标记为已释放，防止重复释放）
  - 但页**不会**出现在可分配的 `ids`/`freemaps` 中

- 当**新读写事务开启**时，[db.go:beginRWTx()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L870) 调用 `freelist.ReleasePendingPages()`：
  [freelist/shared.go:ReleasePendingPages()](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/shared.go#L141-L158)：
  1. 对 `readonlyTXIDs` 排序
  2. 找到**最老的活跃只读事务** `minid`
  3. 调用 `release(minid - 1)`：将所有 `txid <= minid-1` 的 pending 页移入真正的 free 列表（`mergeSpans`）
  4. 对只读事务之间的区间也做 releaseRange（处理"同一事务内分配并释放"的特殊情况）

**核心不变量**：一个页在事务 T 中被释放后，必须等到**所有看到 T 之前快照的只读事务都结束**，才能被重新分配。这与第1问的快照语义呼应：每个只读事务登记 `AddReadonlyTXID(txid)`，其结束时 `RemoveReadonlyTXID(txid)`；新写事务开始时以最老活跃只读事务为界，将所有更老的 pending 页释放。

> **重要后果**：长只读事务会阻止页回收，导致数据库文件持续增长（写时复制不断分配新页，旧页无法复用）。这是 bbolt 用户文档明确警告的行为。

---

## 6. Mmap 按需重映射与 Cursor 遍历

### 6.1 Mmap 初始映射与增长策略

**初始映射**：[db.go:mmap()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L456-L553)
1. 计算合适的 mmap 大小（通过 `mmapSize()`）
2. 若已有映射，先 dereference 所有 rwtx 的节点（因为即将 munmap，旧指针将失效）
3. 调用 `munmap()` 撤销旧映射
4. 调用平台相关的 `mmap()` 建立新映射
5. 设置 `meta0`、`meta1` 指针（指向 mmap 中的页0、页1）
6. 校验两个 meta 页

**容量增长策略**：[db.go:mmapSize()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L581-L613)
- 32KB 到 1GB：每次**翻倍**（2^15=32KB → 2^30=1GB）
- 超过 1GB：每次增长 1GB（`MaxMmapStep = 1<<30`）
- 结果必须对齐到页大小
- 不超过 `MaxMapSize`

**何时触发重映射**：[db.go:allocate()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L1205-L1213)
```go
if minsz >= db.datasz {
    if err := db.mmap(minsz); err != nil { ... }
}
```
当分配新页时，如果需要的地址空间（`(pgid+count)*pageSize`）超过当前 mmap 大小 `datasz`，就触发 mmap 增长。

### 6.2 Mmap 与活跃事务的协调

**为何有事务活跃时不能随意 remap？**

mmap 重映射的步骤是 `munmap()` → `mmap()`。munmap 后，所有指向旧映射区域的指针（包括：
- node 中 inode 的 Key()/Value() 字节切片（直接指向 mmap 内存）
- page 指针
- cursor 栈中的 page 引用

）都变成**悬空指针（dangling pointer）**，解引用会导致 segfault 或读到垃圾数据。

**协调机制**：`mmaplock sync.RWMutex`

- 只读事务：在 `beginTx()` 中获取 `mmaplock.RLock()`，在 `removeTx()` 中释放
- 读写事务：在 `beginRWTx()` 中**不**获取 mmap 读锁（因为它是唯一写者，且它通过 `metalock` 保护 meta 操作），但在 mmap() 中：
  - 首先获取 `mmaplock.Lock()`（写锁）——这必须等待所有只读事务释放其 RLock
  - 在 munmap 前，调用 `db.rwtx.root.dereference()`（[node.go:dereference()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L463-L491)）将所有 mmap 指针拷贝到堆内存，防止使用悬空指针

> **死锁警告**：在同一 goroutine 中先开只读事务再开写事务会死锁！因为写事务在需要 mmap 增长时要获取 mmaplock 写锁，但该 goroutine 中的只读事务持有读锁，且写事务无法等待自己释放（[db.go:Begin()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L756-L759) 注释明确说明）。

### 6.3 Cursor 栈式定位与顺序遍历

Cursor 定义在 [cursor.go](file:///e:/gsb/617/gsb_11/Saturn/cursor.go#L22-L25)，核心是 `stack []elemRef`，栈中每个元素记录当前层的 `page` 或 `node` 及 `index`。

**栈定位（Seek）**：[cursor.go:seek()](file:///e:/gsb/617/gsb_11/Saturn/cursor.go#L159-L166) → [search()](file:///e:/gsb/617/gsb_11/Saturn/cursor.go#L283-L302)

从根开始递归二分搜索：
1. 将当前 page/node 压入 stack
2. 若是叶子节点，在叶子中用二分查找定位 index（`nsearch()`）
3. 若是分支节点，用二分查找找到第一个 key >= 目标 key 的 index（若不精确匹配则回退一个 index），然后递归 search 对应的子页
4. 最终 stack 顶部是叶子节点，index 指向目标位置或其后继

分支搜索逻辑（[searchNode()](file:///e:/gsb/617/gsb_11/Saturn/cursor.go#L304-L322) / [searchPage()](file:///e:/gsb/617/gsb_11/Saturn/cursor.go#L324-L345)）：使用 `sort.Search` 找 lower_bound，若不精确匹配则 index--（因为 B+ 树分支节点的 key 是"分隔键"，目标 key 可能在 index-1 对应的子树中）。

**First/Last**：
- [First()](file:///e:/gsb/617/gsb_11/Saturn/cursor.go#L44-L61)：从根开始，每层取 index=0 的子节点，一直到叶子
- [Last()](file:///e:/gsb/617/gsb_11/Saturn/cursor.go#L66-L90)：从根开始，每层取最后一个子节点，一直到叶子

**Next 顺序遍历**：[cursor.go:next()](file:///e:/gsb/617/gsb_11/Saturn/cursor.go#L215-L247)

```
从栈顶向上回溯：
  for i = stack.len - 1 down to 0:
    if ref.index < ref.count() - 1:
      ref.index++          // 当前页还有下一个元素
      break
  if i == -1: return nil   // 遍历结束
  stack = stack[:i+1]      // 弹出更深层的栈帧
  goToFirstElementOnTheStack()  // 从新位置向下走到最左叶子
```

这模拟了经典 B+ 树遍历：在叶子节点内前进；若到达叶子末尾，回溯到父节点移动到下一个兄弟，再向下走到该兄弟子树的最左叶子。

**Prev**：类似 next，但方向相反，回溯时找 index>0 向前移动，然后 `last()` 走到该子树的最右叶子。

---

## 7. 容易忽视但对正确性至关重要的隐式不变量与风险点

### 风险点 1：Mmap 重映射使既有页/键值指针失效

**风险描述**：所有从 Get/Cursor 返回的 `[]byte` 切片、从 Page 结构体获取的指针，都直接指向 mmap 区域。当写事务触发 mmap 增长时，旧 mmap 被 munmap，这些指针变为悬空指针。如果只读事务未持有 mmaplock 读锁（理论上不会发生，因为 beginTx 获取了读锁），或者在事务关闭后继续使用返回的字节切片，会导致 use-after-free 崩溃或读到脏数据。

**代码依据**：
- [db.go:mmap()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L504-L511)：munmap 前调用 `rwtx.root.dereference()` 保护写事务自身的节点，但**不保护**只读事务的引用——读事务靠 mmaplock.RLock() 阻止重映射
- [node.go:dereference()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L463-L491)：将 key/value 从 mmap 拷贝到堆内存，专门用于 remap 前
- [bucket.go:Get()](file:///e:/gsb/617/gsb_11/Saturn/bucket.go#L433) 注释："The returned value is only valid for the life of the transaction."
- [cursor.go](file:///e:/gsb/617/gsb_11/Saturn/cursor.go#L15-L17) 注释："Keys and values returned from the cursor are only valid for the life of the transaction."

**防御**：必须在事务生命周期内使用返回的 []byte，且必须在事务结束后不再引用。长生命周期数据需要拷贝。

### 风险点 2：Freelist 过早复用导致只读事务读到被覆盖页

**风险描述**：如果 freelist 的 pending 释放逻辑有误（例如 ReleasePendingPages 错误计算了最小活跃只读事务 id），仍被老只读事务引用的页可能被提前回收并分配给写事务的新数据。此时只读事务通过旧的 pgid 指针读到的是新写入的数据，快照隔离被破坏，这是**数据损坏**级别的 bug。

**代码依据**：
- [freelist/shared.go:ReleasePendingPages()](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/shared.go#L141-L158)：正确性依赖 `readonlyTXIDs` 准确记录所有活跃只读事务
- [freelist/shared.go:Free()](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/shared.go#L56-L87)：页被释放时只加入 `pending[txid]`，不直接加入 free
- [db.go:beginTx()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L822)：只读事务开始时调用 `AddReadonlyTXID`；结束时 [removeTx()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L883) 调用 `RemoveReadonlyTXID`
- 验证断言：[freelist/shared.go:Free()](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/shared.go#L68-L72) 检查 "freed page was allocated by the same transaction"（写事务不能释放自己分配的页——这些页从不在老快照中）

**防御**：长只读事务会阻塞页回收是正确行为，不要为了"性能"去缩短 readonlyTXIDs 列表或绕过 ReleasePendingPages 的 minid 检查。

### 风险点 3：Meta 选择/校验错误导致回退到旧状态丢失已提交事务

**风险描述**：db.meta() 选择 txid 更高的 meta，但如果该 meta 的 checksum 损坏，会静默回退到 txid 更低的旧 meta。这在两个场景下有风险：
1. 如果 checksum 算法有碰撞或计算错误，有效 meta 可能被误判为无效
2. 如果两个 meta 的 txid 比较逻辑出错（例如溢出，但 Txid 是 uint64，实际不太可能溢出）
3. 在 NoSync 模式下，meta 写入后但 fsync 前崩溃，OS 可能丢弃写了一半的 meta 页，导致回退到上一个完整 meta——这是预期行为，但可能丢失最近一次"看起来已提交"的事务

**代码依据**：
- [db.go:meta()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L1141-L1162)：先选 txid 高的 metaA，Validate 失败才选 metaB
- [common/meta.go:Validate()](file:///e:/gsb/617/gsb_11/Saturn/internal/common/meta.go#L25-L34)：magic/version/checksum 任一不匹配即认为 meta 损坏
- [common/meta.go:Sum64()](file:///e:/gsb/617/gsb_11/Saturn/internal/common/meta.go#L61-L65)：使用 FNV-64a（非加密哈希，有理论碰撞概率，但实践中极低）
- [tx.go:writeMeta()](file:///e:/gsb/617/gsb_11/Saturn/tx.go#L595-L625)：meta 写入后才 fsync

**防御**：不要在关键数据路径关闭 NoSync；依赖 meta 交替写入（txid%2）保证崩溃时至少有一个完整 meta。

### 风险点 4（附加）：同一 Goroutine 嵌套事务导致死锁

**风险描述**：文档明确警告不要在同一 goroutine 中先开只读事务再开写事务。原因：写事务在 mmap 增长时需要获取 `mmaplock.Lock()`（写锁），但同 goroutine 中的只读事务已持有 `mmaplock.RLock()`。Go 的 RWMutex 不是可重入的，且读锁不被"同协程的写锁请求"递归兼容，导致死锁。

**代码依据**：
- [db.go:Begin()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L756-L763) 注释："Opening a read transaction and a write transaction in the same goroutine can cause the writer to deadlock"
- [db.go:mmap()](file:///e:/gsb/617/gsb_11/Saturn/db.go#L457)：`db.mmaplock.Lock()` 需要等待所有读锁释放

### 风险点 5（附加）：Spill 时旧页释放与根节点分裂的 pgid 断言

**风险描述**：spill 过程中有多个 `panic` 断言检查 pgid 不超过高水位线。如果页分配或 spill 顺序出错（例如子节点 spill 前父节点就引用了子节点新 pgid），会触发 panic。这些 panic 是防御性检查，但在数据损坏的数据库上可能直接崩溃。

**代码依据**：
- [node.go:spill()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L330-L332)：`if p.Id() >= tx.meta.Pgid() { panic(...) }`
- [bucket.go:spill()](file:///e:/gsb/617/gsb_11/Saturn/bucket.go#L796-L798)：`if b.rootNode.pgid >= b.tx.meta.Pgid() { panic(...) }`
- [node.go:put()](file:///e:/gsb/617/gsb_11/Saturn/node.go#L118-L119)：`if pgId >= n.bucket.tx.meta.Pgid() { panic(...) }`

---

## 附录：核心文件索引

| 文件 | 职责 |
|------|------|
| [db.go](file:///e:/gsb/617/gsb_11/Saturn/db.go) | DB 结构体、Open/Close、Begin/事务管理、mmap、页分配、grow |
| [tx.go](file:///e:/gsb/617/gsb_11/Saturn/tx.go) | Tx 结构体、init、Commit/Rollback、write/writeMeta、页访问 |
| [bucket.go](file:///e:/gsb/617/gsb_11/Saturn/bucket.go) | Bucket 实现、Get/Put/Delete、spill/rebalance、node 缓存 |
| [node.go](file:///e:/gsb/617/gsb_11/Saturn/node.go) | 内存节点表示、read/write、put/del、split/spill/rebalance/dereference |
| [cursor.go](file:///e:/gsb/617/gsb_11/Saturn/cursor.go) | Cursor 实现、First/Last/Next/Prev/Seek、栈式遍历 |
| [internal/common/page.go](file:///e:/gsb/617/gsb_11/Saturn/internal/common/page.go) | 磁盘页结构、Page/branchPageElement/leafPageElement |
| [internal/common/meta.go](file:///e:/gsb/617/gsb_11/Saturn/internal/common/meta.go) | Meta 结构、Validate、checksum |
| [internal/common/bucket.go](file:///e:/gsb/617/gsb_11/Saturn/internal/common/bucket.go) | InBucket 磁盘表示 |
| [internal/common/types.go](file:///e:/gsb/617/gsb_11/Saturn/internal/common/types.go) | 常量（Magic/Version/MaxMmapStep/DefaultAllocSize）、Txid 类型 |
| [internal/freelist/freelist.go](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/freelist.go) | Freelist 接口定义 |
| [internal/freelist/shared.go](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/shared.go) | 共享逻辑：pending、ReleasePendingPages、Free/Rollback、Read/Write |
| [internal/freelist/array.go](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/array.go) | 数组式 freelist 实现 |
| [internal/freelist/hashmap.go](file:///e:/gsb/617/gsb_11/Saturn/internal/freelist/hashmap.go) | HashMap 式 freelist（span 管理）实现 |
