# bbolt 系统性深度代码理解

> 基于仓库快照（etcd-io/bbolt 当前 master 风格代码），所有结论均可在给出的文件 + 行号上核对。下文统一使用相对路径，绝对路径前缀为 `e:\gsb\617\gsb_11\Mercury\`。

---

## 1. 事务生命周期 与 MVCC 并发模型

### 1.1 入口：`DB.Begin / Update / View`

- `DB.Begin(writable)` 在 [db.go#L767-L783](file:///e:/gsb/617/gsb_11/Mercury/db.go#L767-L783) 根据 `writable` 分发到 `beginTx`（只读）或 `beginRWTx`（读写）。
- 高层封装 `DB.Update`（[db.go#L905-L930](file:///e:/gsb/617/gsb_11/Mercury/db.go#L905-L930)）和 `DB.View`（[db.go#L936-L961](file:///e:/gsb/617/gsb_11/Mercury/db.go#L936-L961)）会自动 `defer rollback`，并通过 `tx.managed = true` 防止用户在闭包里手动 Commit/Rollback。
- 共有 4 把锁，由 `DB` 字段定义（[db.go#L145-L148](file:///e:/gsb/617/gsb_11/Mercury/db.go#L145-L148)）：
  - `rwlock sync.Mutex`：写者唯一锁，序列化所有 RW 事务；
  - `metalock sync.Mutex`：保护 meta 引用（`db.meta0/meta1`）和 `freelist` 的“事务级”状态；
  - `mmaplock sync.RWMutex`：mmap remap 与活跃事务的协调；
  - `statlock sync.RWMutex`：保护 `stats`。

### 1.2 只读事务（RO）生命周期

`DB.beginTx`（[db.go#L792-L837](file:///e:/gsb/617/gsb_11/Mercury/db.go#L792-L837)）：

1. `metalock.Lock()` → 临界区里读 meta；
2. `mmaplock.RLock()`：以读模式持有 mmap 锁，阻止任何 remap 在事务存活期发生（remap 需要 `Lock()`，见 [db.go#L457](file:///e:/gsb/617/gsb_11/Mercury/db.go#L457)）；
3. `t.init(db)`：在 [tx.go#L47-L65](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L47-L65) 中将 `db.meta()` 拷贝到 `tx.meta` 一份本地副本，以及 root bucket 引用；
4. `db.freelist.AddReadonlyTXID(tx.meta.Txid())`（[db.go#L821-L823](file:///e:/gsb/617/gsb_11/Mercury/db.go#L821-L823)）：把当前 RO 的 txid 注册进 freelist，用于阻止仍可能引用其页的释放；
5. `metalock.Unlock()`，但 `mmaplock` 的 RLock 持续到 Rollback。

读路径走 `Cursor`/`Bucket`，最终通过 `tx.page(id)` 在 `tx.pages`（写事务才有）和 `db.page(id)`（mmap）里取页（[tx.go#L629-L642](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L629-L642)）。

收尾必须用 `Rollback`（不能 `Commit`，[tx.go#L186-L188](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L186-L188) 在 commit 时拒绝非 writable 事务）：

- `Rollback` → `nonPhysicalRollback`（[tx.go#L302-L320](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L302-L320)）→ `tx.close()`；
- `tx.close()` 对只读事务调用 `db.removeTx(tx)`（[db.go#L875-L896](file:///e:/gsb/617/gsb_11/Mercury/db.go#L875-L896)）：先 `mmaplock.RUnlock()`，再 `metalock.Lock()` 调 `freelist.RemoveReadonlyTXID(tx.meta.Txid())`，最后清统计。这就解除了对 mmap remap 与 pending 释放的阻塞。

### 1.3 读写事务（RW）生命周期

`DB.beginRWTx`（[db.go#L839-L872](file:///e:/gsb/617/gsb_11/Mercury/db.go#L839-L872)）：

1. `rwlock.Lock()`：保证“at most one writer”，所有同时到达的写者串行；
2. `metalock.Lock()`（defer Unlock）：进入 meta 临界区；
3. `t.init(db)`：拷贝 meta，并在 [tx.go#L61-L64](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L61-L64) 中 `tx.meta.IncTxid()`。这是 txid 单调递增的唯一来源——每次成功 Begin RW 都会基于“当前 meta 的 txid”+1；
4. `db.rwtx = t` 暴露给 freelist allocate 使用；
5. `db.freelist.ReleasePendingPages()`（[db.go#L870](file:///e:/gsb/617/gsb_11/Mercury/db.go#L870) → [shared.go#L141-L158](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L141-L158)）：把所有 `txid <= 最早活跃只读 txid - 1` 的 pending 页正式释放进 free 列表。这是“延迟回收”最关键的一步，对应第 5、6 节的快照保证。

请注意：`beginRWTx` **没有获取 `mmaplock`**。但 `DB.allocate`（[db.go#L1165-L1220](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1165-L1220)）在需要 remap 时会调用 `db.mmap(minsz)`，后者获取 `mmaplock.Lock()` 写锁——此时它会被任何尚未结束的只读事务阻塞，这就是文档 [db.go#L756-L759](file:///e:/gsb/617/gsb_11/Mercury/db.go#L756-L759) 提示“同 goroutine 同时开 RO+RW 会死锁”的根因。

读写期间所有 dirty 页留在 `tx.pages`（map[Pgid]*Page），由 `Tx.allocate`（[tx.go#L501-L517](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L501-L517)）填充。

收尾要么 `Commit()`（详见第 3 节），要么 `Rollback()`：

- `tx.rollback()`（带 reload 版本，[tx.go#L323-L343](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L323-L343)）会 `freelist.Rollback(txid)` 撤销 pending，并按 `NoFreelistSync` 选择 `Reload` 或 `NoSyncReload`；
- `tx.close()`（[tx.go#L345-L378](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L345-L378)）对 writable 路径释放 `db.rwtx = nil` 和 `db.rwlock.Unlock()`。

### 1.4 MVCC 并发模型

bbolt 的 MVCC 是“**单写多读 + COW + 双 meta**”：

1. **多 RO ↔ 一 RW 并发**：靠 `rwlock` 序列化写者；读者只持 `mmaplock.RLock`，互不阻塞，写者也不会因 `rwlock` 与读者冲突。读者只在 mmap 需要扩张时被“延迟到下次 remap”阻塞写者。
2. **一致快照**：在 `Tx.init`（[tx.go#L47-L58](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L47-L58)）只读事务把 `db.meta()` 整个 `Copy` 到 `tx.meta`。`db.meta()`（[db.go#L1141-L1162](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1141-L1162)）总是返回 txid 较高且通过 Validate 的那一份。一旦快照确定，root pgid、freelist pgid、HWM `pgid` 全部冻结。任何后续写事务由于 COW（脏节点必然分配新页，旧页只进 pending），不会覆盖该快照覆盖的任何页 → 读路径自然一致。
3. **txid 单调推进**：唯一递增点为 [tx.go#L63](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L63) 的 `tx.meta.IncTxid()`；初始值由 `init()` 写入 0 和 1 两个 meta（[db.go#L662](file:///e:/gsb/617/gsb_11/Mercury/db.go#L662) 的 `m.SetTxid(common.Txid(i))`）。
4. **写者串行化**：`db.rwlock` 是唯一同步原语；`metalock` 是细粒度的引用保护，不参与互斥写。
5. **快照与回收的耦合**：`AddReadonlyTXID/RemoveReadonlyTXID`（[shared.go#L120-L133](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L120-L133)）使写事务在 Begin 时调用 `ReleasePendingPages()` 时，可以严格只释放“老于最早 RO 的 pending”，从而保证 RO 仍能读到旧版页。

---

## 2. 磁盘页与 Meta 布局

### 2.1 Page 结构

[internal/common/page.go#L31-L36](file:///e:/gsb/617/gsb_11/Mercury/internal/common/page.go#L31-L36)：

```go
type Page struct {
    id       Pgid
    flags    uint16
    count    uint16
    overflow uint32
}
```

页头之后是该页类型对应的负载（`branchPageElement` / `leafPageElement` 数组、Meta、Pgid 数组等）。

页类型由 `flags` 单标位决定（[page.go#L18-L23](file:///e:/gsb/617/gsb_11/Mercury/internal/common/page.go#L18-L23)）：

| 常量 | 值 | 含义 |
|---|---|---|
| `BranchPageFlag` | 0x01 | B+ 树分支节点 |
| `LeafPageFlag` | 0x02 | B+ 树叶子节点 / inline bucket |
| `MetaPageFlag` | 0x04 | meta 页（仅 page 0、1） |
| `FreelistPageFlag` | 0x10 | freelist 页 |

`Page.IsValidPage()` 强制只能命中其中一种（[page.go#L78-L83](file:///e:/gsb/617/gsb_11/Mercury/internal/common/page.go#L78-L83)），`FastCheck` 在每次 `tx.page` / `db.page` 后调用（[tx.go#L633](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L633), [tx.go#L640](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L640)）。

### 2.2 overflow 页

当一个逻辑页的负载 > `pageSize` 时，使用连续物理页表示。`Page.overflow = N` 表示在该页之后再占用 N 个物理页（共 N+1 个连续页）。证据：

- 分配：`DB.allocate(txid, count)` 在 [db.go#L1173-L1174](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1173-L1174) 设置 `p.SetOverflow(uint32(count - 1))`；高水位前进 `count` 页（[db.go#L1216-L1217](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1216-L1217)）。
- 写出：`Tx.write` 按 `(p.Overflow()+1) * pageSize` 计算写入字节数（[tx.go#L533](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L533)）。
- freelist 释放：`shared.Free` 在 [shared.go#L77](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L77) 用 `for id := p.Id(); id <= p.Id()+Pgid(p.Overflow()); id++` 一次释放所有覆盖页。
- 大节点 spill：`node.spill` 用 `(node.size() + pageSize - 1)/pageSize` 计算需要多少个连续页（[node.go#L324](file:///e:/gsb/617/gsb_11/Mercury/node.go#L324)）。

### 2.3 双 Meta 设计

#### 元数据结构

[internal/common/meta.go#L12-L22](file:///e:/gsb/617/gsb_11/Mercury/internal/common/meta.go#L12-L22)：

```go
type Meta struct {
    magic, version, pageSize, flags uint32
    root      InBucket
    freelist  Pgid
    pgid      Pgid       // HWM, 下一个可分配 pgid
    txid      Txid
    checksum  uint64
}
```

#### 双 meta 写法

- 初始化：`DB.init`（[db.go#L645-L689](file:///e:/gsb/617/gsb_11/Mercury/db.go#L645-L689)）写入两个 meta，分别 txid=0 和 txid=1；
- 提交：`Meta.Write` 通过 `p.id = Pgid(m.txid % 2)` 决定写入 page 0 还是 page 1（[meta.go#L51](file:///e:/gsb/617/gsb_11/Mercury/internal/common/meta.go#L51)），并重算 checksum（[meta.go#L54-L55](file:///e:/gsb/617/gsb_11/Mercury/internal/common/meta.go#L54-L55)）。这意味着**奇偶 txid 交替覆盖两个 meta 页**——任意时刻磁盘上保留着前一次提交的 meta 与本次提交的 meta。

#### 打开时的选择与校验

读取 page size：`DB.getPageSize`（[db.go#L332-L366](file:///e:/gsb/617/gsb_11/Mercury/db.go#L332-L366)）→ `getPageSizeFromFirstMeta` / `getPageSizeFromSecondMeta`（[db.go#L368-L417](file:///e:/gsb/617/gsb_11/Mercury/db.go#L368-L417)）：先读 page 0；如失败再以多种 1KB~16MB 的步长试探读 page 1，每次都要 `m.Validate()`。

mmap 后取引用：`db.mmap`（[db.go#L538-L550](file:///e:/gsb/617/gsb_11/Mercury/db.go#L538-L550)）拿到 `db.meta0` / `db.meta1`，`Validate` 仅当**两个都失败**才报错。

运行期选择：`DB.meta()`（[db.go#L1141-L1162](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1141-L1162)）选 txid 较高且 `Validate()` 通过的；若高的损坏则回退到低的；若两个都坏 panic。

`Meta.Validate`（[meta.go#L25-L34](file:///e:/gsb/617/gsb_11/Mercury/internal/common/meta.go#L25-L34)）三连：`magic == 0xED0CDAED`、`version == 2`、`checksum == Sum64()`（FNV-64a，作用域是 `Meta` 中 checksum 之前的字节，见 [meta.go#L61-L65](file:///e:/gsb/617/gsb_11/Mercury/internal/common/meta.go#L61-L65)）。

#### 服务于崩溃恢复

由于 `p.id = Pgid(txid % 2)`，每次提交只会重写**两个 meta 中的一个**。如果在写入新 meta 的中途断电：

- 新 meta（txid 较高那一份）checksum 失败 → 自动回退到旧 meta；
- 旧 meta 仍然完整指向尚未被覆盖的 root/freelist/HWM；
- 由于在 `tx.write()` 之前所有数据页 + freelist 都已经 fsync 落盘且高水位下的旧页**未被本次复用**（pending 机制），旧 meta 看到的状态是完整一致的。

这就是 bbolt 的“两份 meta + COW + 顺序提交”做出的不需 WAL 的崩溃一致性。

---

## 3. 读写事务 Commit 完整流水线

入口 `Tx.Commit`（[tx.go#L170-L283](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L170-L283)）。下面按行号串起来：

| # | 步骤 | 代码位置 | 作用 |
|---|---|---|---|
| 1 | `tx.root.rebalance()` | [tx.go#L195](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L195) | 合并/借用，处理 Delete 后的不平衡节点 |
| 2 | `tx.root.spill()` | [tx.go#L204](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L204) | 把脏 node 切分成合适大小的页，分配新页（可能扩 mmap），旧页进 freelist pending |
| 3 | `tx.meta.RootBucket().SetRootPage(tx.root.RootPage())` | [tx.go#L212](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L212) | 把新 root 写入 tx 内 meta 副本 |
| 4 | `freelist.Free(txid, db.page(meta.Freelist()))` | [tx.go#L215-L217](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L215-L217) | 旧 freelist 页本身也加入 pending |
| 5 | `tx.commitFreelist()` 或 `meta.SetFreelist(PgidNoFreelist)` | [tx.go#L219-L227](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L219-L227) / [tx.go#L285-L298](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L285-L298) | 为新 freelist 分配新页并 `freelist.Write(p)` 序列化 |
| 6 | `db.grow(...)` | [tx.go#L230-L240](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L230-L240) | 若 HWM 增长则 truncate 文件 + fsync |
| 7 | `tx.write()` 写脏页 + fsync | [tx.go#L244](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L244) → [tx.go#L519-L592](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L519-L592) | 按 pgid 排序 pwrite 所有脏页（含新 freelist），`fdatasync` 一次 |
| 8 | `tx.writeMeta()` 写 meta + fsync | [tx.go#L267](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L267) → [tx.go#L595-L625](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L595-L625) | 写到 `Pgid(txid%2)` 的 meta 页，再 `fdatasync` |
| 9 | `tx.close()`、commitHandlers | [tx.go#L275-L280](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L275-L280) | 释放 rwlock |

### 3.1 顺序为何必须如此

**先数据页后 meta，且每段都 fsync** 是 crash safety 的核心：

1. `rebalance → spill`（步骤 1-2）只会**消费 free 页或扩展 HWM**；旧页只进入 freelist pending，从来不会被本事务再分配（`Free` 中 `allocTxid == txid` 会 panic，[shared.go#L66-L72](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L66-L72)）。这意味着在 `writeMeta` 切换之前，**任何被旧 meta 引用的页都没有被覆盖**。
2. `tx.write()` 的 fsync（[tx.go#L568](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L568)，"beforeSyncDataPages" gofail 点）保证：**新 root 子树 + 新 freelist 全部持久到磁盘**。
3. 第二次 fsync（`writeMeta`，[tx.go#L615](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L615)，"beforeSyncMetaPage" gofail 点）才让新 meta 持久。这个时间点是“事务提交点”：在它之前任何崩溃，重启后 `db.meta()` 都会丢弃高 txid 的损坏 meta，回退到旧 meta（旧 root、旧 freelist、旧 HWM 仍然有效）；在它之后崩溃，则新 meta 完整生效。
4. 顺序倒过来——例如先写 meta 后写 data——崩溃后 meta 指向的 root 子树可能含尚未落盘的脏页 → 数据库进入不可恢复的状态。

### 3.2 NoSync / NoFreelistSync 等选项的取舍

- `NoSync`（[db.go#L45-L55](file:///e:/gsb/617/gsb_11/Mercury/db.go#L45-L55)）：跳过 `tx.write` 与 `tx.writeMeta` 的 `fdatasync`（条件 `!tx.db.NoSync || common.IgnoreNoSync`，见 [tx.go#L566-L572](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L566-L572)、[tx.go#L613-L619](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L613-L619)）。性能高，但崩溃后磁盘可能落后于内存任意提交，**完全失去 ACID 中的 Durability**。`init()`（[db.go#L683](file:///e:/gsb/617/gsb_11/Mercury/db.go#L683)）和 `grow()`（[db.go#L1248](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1248)）的 fsync 不受此影响。
- `NoFreelistSync`（[db.go#L57-L60](file:///e:/gsb/617/gsb_11/Mercury/db.go#L57-L60), [tx.go#L219-L227](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L219-L227)）：commit 时不写 freelist 页，meta 写 `PgidNoFreelist = 0xffff...ffff`（[types.go#L18](file:///e:/gsb/617/gsb_11/Mercury/internal/common/types.go#L18)）。代价：**重启时 `loadFreelist`（[db.go#L422-L436](file:///e:/gsb/617/gsb_11/Mercury/db.go#L422-L436)）必须扫描整个 DB**（`freepages` 在 [db.go#L1277-L1312](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1277-L1312) 通过 `recursivelyCheckBucket` 找出 unreachable 页），大库下打开慢。`hasSyncedFreelist` 由 `db.meta().Freelist() != PgidNoFreelist` 判定（[db.go#L438-L440](file:///e:/gsb/617/gsb_11/Mercury/db.go#L438-L440)）。注意 `Open` 在 [db.go#L313-L323](file:///e:/gsb/617/gsb_11/Mercury/db.go#L313-L323) 会“自动从 NoFreelistSync 切回有 freelist”（让旧版本仍能打开）。
- `NoGrowSync`（[db.go#L69-L75](file:///e:/gsb/617/gsb_11/Mercury/db.go#L69-L75) → [db.go#L1239-L1257](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1239-L1257)）：跳过 `truncate + Sync` 文件长度。某些 fs（ext3/4）下不安全，崩溃后文件 size metadata 可能不一致。
- `StrictMode`：commit 后跑 `tx.Check()`，性能极低，仅调试用（[tx.go#L251-L264](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L251-L264)）。

---

## 4. spill / rebalance 与 FillPercent + COW

### 4.1 FillPercent

- 默认值 `DefaultFillPercent = 0.5`（[bucket.go#L27](file:///e:/gsb/617/gsb_11/Mercury/bucket.go#L27)）；
- 上下界 `minFillPercent = 0.1`、`maxFillPercent = 1.0`（[bucket.go#L21-L22](file:///e:/gsb/617/gsb_11/Mercury/bucket.go#L21-L22)）；
- 在 `node.splitTwo` 中夹紧到 [0.1,1.0]（[node.go#L237-L242](file:///e:/gsb/617/gsb_11/Mercury/node.go#L237-L242)）：

```go
threshold := int(float64(pageSize) * fillPercent)
```

`splitIndex(threshold)`（[node.go#L271-L291](file:///e:/gsb/617/gsb_11/Mercury/node.go#L271-L291)）从前往后累加 inode 大小，超过 threshold 且当前位置 ≥ `MinKeysPerPage` 即切分。即 FillPercent 控制**“一个 spilled 节点的目标占用页比例”**——增大它有利顺序追加场景（更紧凑、更少分裂）。

`splitTwo` 还有两个早退条件（[node.go#L232-L234](file:///e:/gsb/617/gsb_11/Mercury/node.go#L232-L234)）：inode 数 ≤ `MinKeysPerPage*2 = 4` 或整体能放进单页，则不切。

### 4.2 spill：写时复制的关键

`node.spill`（[node.go#L295-L361](file:///e:/gsb/617/gsb_11/Mercury/node.go#L295-L361)）流程：

1. 先递归 `child.spill()`（孩子可能引发 sibling 的 split-merge，所以用下标循环而不是 range）；
2. `node.split(pageSize)` 切成多个新 node；
3. **核心 COW**：

   ```go
   if node.pgid > 0 {
       tx.db.freelist.Free(tx.meta.Txid(), tx.page(node.pgid))
       node.pgid = 0
   }
   p, err := tx.allocate((node.size()+pageSize-1)/pageSize)
   ...
   node.pgid = p.Id()
   node.write(p)
   ```

   即：脏 node **总是先把旧 pgid 交还 freelist（pending），再分配新 pgid**。原 mmap 上的旧页内容保持不变 → 任何还活着的快照仍读得到。
4. 把新 pgid 注入父 inode：`node.parent.put(...)`，所以父节点也变 dirty，会再次进入 spill 链路。

### 4.3 rebalance：删除后的合并/借用

`Bucket.rebalance` 调到 [node.go#L365-L448](file:///e:/gsb/617/gsb_11/Mercury/node.go#L365-L448)：

- 触发：在 `node.del`（[node.go#L145-L159](file:///e:/gsb/617/gsb_11/Mercury/node.go#L145-L159)）会 `n.unbalanced = true`；只有 unbalanced 的节点才会进入 rebalance。
- 阈值：`threshold := pageSize*FillPercent / 2`（[node.go#L375](file:///e:/gsb/617/gsb_11/Mercury/node.go#L375)）。**默认 50%×0.5 = 25% 页空间**；当 `node.size() > threshold && len(inodes) > minKeys()` 直接 return，不合并。
- 根节点特例（[node.go#L380-L404](file:///e:/gsb/617/gsb_11/Mercury/node.go#L380-L404)）：分支 root 只剩一个孩子时**坍缩**——把孩子提升为新的 root，原 root 通过 `child.free()` 进 pending。
- 空节点（[node.go#L406-L414](file:///e:/gsb/617/gsb_11/Mercury/node.go#L406-L414)）：从父节点删除自己，父递归 rebalance。
- 合并策略（[node.go#L418-L447](file:///e:/gsb/617/gsb_11/Mercury/node.go#L418-L447)）：bbolt **不做“向兄弟借用 1 个 key”**，而是直接**合并相邻兄弟到一个节点**。规则：若自己是父的第一个孩子（idx==0）则与下一个兄弟合并，否则与上一个兄弟合并；右节点的所有 inode 追加到左节点尾部，右节点 `free()`，父节点删该 key 后递归 rebalance。
- 合并完成后**没有立即 split** —— 因为后续 `Tx.Commit` 顺序是先 rebalance 再 spill（[tx.go#L195-L208](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L195-L208)），合并后过大的节点交由 spill 重新切分。

### 4.4 旧页归还 freelist 的时间

`free()`（[node.go#L494-L499](file:///e:/gsb/617/gsb_11/Mercury/node.go#L494-L499)）和 spill 中的 `freelist.Free` 都**只是把 pgid 加入 pending[txid]**（[shared.go#L56-L87](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L56-L87)），不进入可分配列表 `ids`。这就是 COW + 安全回收的耦合点（详见第 5 节）。

---

## 5. freelist 双实现与 pending 语义

### 5.1 共享逻辑

`shared`（[shared.go#L18-L33](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L18-L33)）持有：

- `cache map[Pgid]struct{}`：所有 free + pending 页的快速集合，`Freed()` 用它（[shared.go#L51-L54](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L51-L54)），保证“页已不可达”但不一定“可重分配”；
- `pending map[Txid]*txPending`：按 txid 分桶的待释放页（含 `alloctx` 记录“它是哪个 tx 分配的”、`lastReleaseBegin` 优化）；
- `allocs map[Pgid]Txid`：记录“pgid → 分配它的 txid”，用于校验“一个事务不能 free 自己刚分配的页”（[shared.go#L66-L72](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L66-L72)），并供 `releaseRange` 跨 extent 释放使用；
- `readonlyTXIDs []Txid`：所有活跃 RO 的 txid 列表。

#### Pending → Free 的唯一闸门：`ReleasePendingPages`

`ReleasePendingPages`（[shared.go#L141-L158](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L141-L158)）只在 `beginRWTx` 中调用（[db.go#L870](file:///e:/gsb/617/gsb_11/Mercury/db.go#L870)），算法：

1. 找出 `minid = min(readonlyTXIDs)`，若没有 RO 则视作 `MaxUint64`；
2. `release(minid - 1)`：把 `txid <= minid-1` 的 pending 全部移入 free（[shared.go#L160-L171](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L160-L171)，调用 `mergeSpans`）；
3. 遍历每个 RO txid，跨 extent `releaseRange(begin, end)`（[shared.go#L173-L203](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L173-L203)）——只释放“在 \[begin,end] 区间内既被 alloc 又被 free 的页”，因为这些页对任何活跃 RO 都不可见。

这就是为什么**一个页被释放后不能立即被复用**：在它“释放”那一刻，可能还有 txid 较小的 RO 事务持有的快照 root 仍然能通过 B+ 树到达它。直到所有 ≤ 它的“alloc tx”和“free tx”范围都没有活跃 RO 时，才允许进入可分配列表。这正好与第 1 节的快照语义闭环：

- RO 看到 `tx.meta.Txid() = T`；其能可达的最远修改是 txid ≤ T 的所有提交；
- 因此“在 txid > T 的 RW 中被 free 的页”可以立即并入 free（不会被 RO 看到——它根本不在 RO 的 root 子树里）；
- “在 txid ≤ T 的 RW 中被 free 的页”必须等 RO 关闭后再释放，否则下一个 RW 可能 allocate 同一个 pgid 并覆盖内容，破坏 RO 一致性。

### 5.2 Array 实现

[internal/freelist/array.go](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/array.go)：

- 状态：`ids []Pgid`，按升序保存所有可分配 pgid（[array.go#L13](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/array.go#L13)）。
- `Allocate(txid, n)`（[array.go#L21-L61](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/array.go#L21-L61)）：线性扫描，维护 `initial` 指向当前候选连续段起点，一旦累计长度等于 `n` 就切片摘除并返回。所以连续多页分配 = O(N) 扫描整个有序列表找首个长度 ≥ n 的连续段。
- `mergeSpans(ids)`（[array.go#L71-L99](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/array.go#L71-L99)）：合并新释放的 pgid 到现有 `ids`，复用 `common.Pgids.Merge`（归并排序）。
- 优点：实现极简、写入小；缺点：大量碎片下 `Allocate` 的 O(N) 扫描成为热点。

### 5.3 HashMap 实现

[internal/freelist/hashmap.go](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/hashmap.go)：

- 状态（[hashmap.go#L14-L21](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/hashmap.go#L14-L21)）：
  - `freemaps map[uint64]pidSet`：key = 连续段 size，value = 该 size 所有起始 pgid 的集合；
  - `forwardMap map[Pgid]uint64`：start → size；
  - `backwardMap map[Pgid]uint64`：end → size。
- `Allocate(txid, n)`（[hashmap.go#L61-L106](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/hashmap.go#L61-L106)）：先查 `freemaps[n]` 拿精确匹配；否则遍历各 size 找到首个 ≥ n，分裂出尾部余量再 `addSpan`。**首个 fit**——不保证最低 pgid，所以注释 [db.go#L62-L66](file:///e:/gsb/617/gsb_11/Mercury/db.go#L62-L66) 说 "doesn't guarantee that it offers the smallest page id available"。
- `mergeSpans` + `mergeWithExistingSpan`（[hashmap.go#L173-L247](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/hashmap.go#L173-L247)）：把要释放的 pgid 排序成最大 run，再用 backward/forward map 与已有相邻 span 合并，使大块更易出现，碎片少。
- 几乎所有操作（除 `freePageIds` 用于序列化时排序 + `mergeSpans` 排序）都是 O(1) 平均复杂度。

### 5.4 序列化与反序列化

- 写：`shared.Write`（[shared.go#L287-L309](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L287-L309)）把 free + pending **全部** 通过 `Copyall`（[shared.go#L207-L214](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L207-L214)）合并成一个有序 pgid 列表写到 freelist 页；当数量 ≥ 0xFFFF 时把真实 count 放在第一个 element（与 `Page.FreelistPageCount` [page.go#L132-L150](file:///e:/gsb/617/gsb_11/Mercury/internal/common/page.go#L132-L150) 对应）。
- 读：`shared.Read`（[shared.go#L257-L276](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L257-L276)）加载并 `Init`，hashMap 实现里 `Init` 会按连续段重建 freemaps（[hashmap.go#L23-L59](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/hashmap.go#L23-L59)）。

---

## 6. mmap 重映射 + Cursor 栈式遍历

### 6.1 文件增长与 mmap 重映射

#### 触发链
1. 写事务在 `Tx.allocate` 调用 `db.allocate(txid, count)`（[db.go#L1165-L1220](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1165-L1220)）；
2. freelist 拿不到位置 → 走 HWM 分配。若 `minsz = (id+count+1)*pageSize >= db.datasz`，则调用 `db.mmap(minsz)`（[db.go#L1205-L1213](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1205-L1213)）；
3. `db.grow(...)` 在 commit 阶段 truncate 文件（[db.go#L1222-L1261](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1222-L1261)），保证 file size ≥ 新 mmap 长度。

#### 容量增长策略
`mmapSize`（[db.go#L581-L613](file:///e:/gsb/617/gsb_11/Mercury/db.go#L581-L613)）：
- 起步 32KB，**翻倍至 1GB**（i 从 15 到 30）；
- 超过 1GB 后按 `MaxMmapStep = 1<<30 = 1GB` 步长增长（[types.go#L10](file:///e:/gsb/617/gsb_11/Mercury/internal/common/types.go#L10)）；
- 必为 page size 倍数；
- 上限 `MaxMapSize`（架构相关常量，见 `internal/common/bolt_*.go`）。

`AllocSize = 16MB`（[types.go#L30](file:///e:/gsb/617/gsb_11/Mercury/internal/common/types.go#L30)）控制文件 truncate 的步长（[db.go#L1263-L1271](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1263-L1271)），避免每次 truncate+fsync。

#### 与活跃事务的关系
`db.mmap`（[db.go#L456-L553](file:///e:/gsb/617/gsb_11/Mercury/db.go#L456-L553)）开头：

```go
db.mmaplock.Lock()
defer db.mmaplock.Unlock()
```

- RO 事务持有 `mmaplock.RLock()`（[db.go#L801](file:///e:/gsb/617/gsb_11/Mercury/db.go#L801)），所以 remap 必须等所有 RO 结束。这是为何文档建议设置 `InitialMmapSize` 大一些来避免长读阻塞 RW。
- RW 事务**自身不持 mmaplock**，所以 `db.mmap` 的 `Lock()` 真正阻塞的对象是 RO。
- remap 之前 [db.go#L504-L506](file:///e:/gsb/617/gsb_11/Mercury/db.go#L504-L506)：`if db.rwtx != nil { db.rwtx.root.dereference() }`：把当前 RW 已经访问过的 inode key/value 从 mmap 拷贝到堆内存（`node.dereference` [node.go#L463-L491](file:///e:/gsb/617/gsb_11/Mercury/node.go#L463-L491)），因为 `munmap` 之后这些 unsafe slice 会指向无效地址。这是写者“我自己持有的 mmap 指针”自救。
- 之后 `munmap → mmap → meta0/meta1 重新指向新映射`（[db.go#L538-L540](file:///e:/gsb/617/gsb_11/Mercury/db.go#L538-L540)）。

### 6.2 Cursor 栈式遍历

`Cursor`（[cursor.go#L22-L25](file:///e:/gsb/617/gsb_11/Mercury/cursor.go#L22-L25)）：

```go
type Cursor struct {
    bucket *Bucket
    stack  []elemRef
}
```

`elemRef = {page, node, index}`：当 RW 已把对应页材化为 node 则 `node != nil`，否则用 `page`（mmap）。

- 定位 `Seek/seek` → `c.search(seek, RootPage())`：从 root 开始递归把每一层选中的 branchPageElement 入栈，直到 leaf；
- 顺序遍历：
  - `goToFirstElementOnTheStack`（[cursor.go#L168-L187](file:///e:/gsb/617/gsb_11/Mercury/cursor.go#L168-L187)）：沿着栈顶反复取第一个孩子直到 leaf；
  - `Next`（[cursor.go#L92-L102](file:///e:/gsb/617/gsb_11/Mercury/cursor.go#L92-L102)）→ `c.next()` 在最深层 +1，溢出则回退到上一层 +1，再下钻到第一个孩子，是经典 B+ 树深度优先；
  - `First/Last/Prev` 对称处理。

`Bucket.pageNode(pgid)` 决定每一步取 page 还是 node（写事务下若节点已被材化为 node，则后续遍历直接走 node，否则走 mmap 上的原页）。

---

## 7. 容易被忽视但对正确性至关重要的隐式不变量 / 风险点

下面给出 5 条最关键的、**有代码证据**的隐式约束：

### 7.1 mmap remap 使既有指针失效——RO 必须持 mmaplock.RLock 至关闭

- 证据：`db.mmap`（[db.go#L457-L519](file:///e:/gsb/617/gsb_11/Mercury/db.go#L457-L519)）会先 `Lock`、`munmap`、再 `mmap`；任何指针（包含 `db.meta0/meta1`、`tx.meta.RootBucket()` 间接引用的页）在 `munmap` 后都失效。
- 保护：RO 在 `beginTx` 抓 `mmaplock.RLock`（[db.go#L801](file:///e:/gsb/617/gsb_11/Mercury/db.go#L801)），直到 `removeTx` 才 RUnlock（[db.go#L877](file:///e:/gsb/617/gsb_11/Mercury/db.go#L877)）。RW 不抓，但通过 `root.dereference`（[db.go#L504-L506](file:///e:/gsb/617/gsb_11/Mercury/db.go#L504-L506)）+ `node.dereference`（[node.go#L463-L491](file:///e:/gsb/617/gsb_11/Mercury/node.go#L463-L491)）自救。
- 风险：业务在 tx.Rollback 之后还使用 `[]byte` 返回值，将读到 stale/SEGV——文档明确规定“Keys and values returned … only valid for the life of the transaction”（[cursor.go#L17](file:///e:/gsb/617/gsb_11/Mercury/cursor.go#L17)）。

### 7.2 freelist 过早复用 → 旧 RO 读到被覆盖页

- 证据：`shared.Free`（[shared.go#L56-L87](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L56-L87)）只入 `pending[txid]` 不入 `ids`；正式释放走 `ReleasePendingPages → release/releaseRange`（[shared.go#L141-L203](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L141-L203)），仅在 `beginRWTx` 时执行（[db.go#L870](file:///e:/gsb/617/gsb_11/Mercury/db.go#L870)）。
- 不变量：`min(readonlyTXIDs)` 之前的 pending 才能进入 free。
- 风险：长期不关闭 RO 会导致 pending 永远无法回收，db 文件**膨胀且 free 不可用**，文档警告 [db.go#L765-L767](file:///e:/gsb/617/gsb_11/Mercury/db.go#L765-L767)：“IMPORTANT: You must close read-only transactions … else the database will not reclaim old pages.”

### 7.3 双 meta 选择：错误回退到旧状态

- 证据：`DB.meta()`（[db.go#L1141-L1162](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1141-L1162)）选 txid 高的；当高 txid 那张 `Validate` 失败（checksum/magic/version 不匹配）则回退到低 txid。
- 不变量保护点：commit 流水线必须**先 fsync data，再 fsync meta**（[tx.go#L568](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L568)、[tx.go#L615](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L615)）。任何顺序错乱（例如启用 NoSync）都会让选回旧 meta 时仍可能丢数据。
- 风险：用户开启 `NoSync` 或自行 fsync 缺失 → 高 txid meta 落盘但相关 data 页未落 → 选高 meta 后，B+ 树指针指向未初始化页 → `FastCheck` panic（[page.go#L90-L98](file:///e:/gsb/617/gsb_11/Mercury/internal/common/page.go#L90-L98)）。

### 7.4 同 goroutine RO + RW 的死锁

- 证据：RW 通过 `db.allocate → db.mmap` 抓 `mmaplock.Lock`（[db.go#L457](file:///e:/gsb/617/gsb_11/Mercury/db.go#L457)），需要等 RO 的 RLock 释放；如果同一 goroutine 既持有 RO 又试图开 RW，自身就持有 RLock，将永远等不到 Lock。
- 文档：[db.go#L756-L759](file:///e:/gsb/617/gsb_11/Mercury/db.go#L756-L759) 显式提示。
- 风险：在 Hold 一个 `View` 闭包内调用 `Update` 必死锁。

### 7.5 高水位与 root pgid 的强约束

- `Meta.Write`（[meta.go#L42-L48](file:///e:/gsb/617/gsb_11/Mercury/internal/common/meta.go#L42-L48)）panic 条件：`root.root >= pgid` 或 `freelist >= pgid && freelist != PgidNoFreelist`；
- `node.put`（[node.go#L117-L124](file:///e:/gsb/617/gsb_11/Mercury/node.go#L117-L124)）panic 条件：`pgId >= tx.meta.Pgid()`；
- `node.spill`（[node.go#L330-L332](file:///e:/gsb/617/gsb_11/Mercury/node.go#L330-L332)）：分配的新 pgid 必须 < HWM。
- 不变量：所有出现在 B+ 树或 meta 中的 pgid 必须在 `[2, meta.pgid)` 半开区间内（页 0/1 是 meta）。任何破坏（freelist bug、surgery 工具误用）都会被这些断言抓住。
- 风险：手工修改文件或 `surgeon` 工具操作不当 → 重新打开时直接 panic，但属于“安全失败”——比静默错误好。

### 7.6 （附加）一个事务不能 free 自己刚 alloc 的页

- 证据：`shared.Free` 中 `Verify` 块（[shared.go#L66-L72](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L66-L72)）；`Rollback` 中也 panic（[shared.go#L104-L107](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go#L104-L107)）。
- 不变量：保证 spill 在 “先 free 旧页 + 后 alloc 新页” 时旧 pgid != 新 pgid，否则同一事务 commit 会写到刚释放的页，破坏 COW。

---

## 8. 总结图：一次成功 RW Commit 的关键事件链

```
Begin RW
 ├─ rwlock.Lock                              // serialize writers
 ├─ metalock.Lock
 ├─ tx.meta = Copy(db.meta()); txid++
 ├─ ReleasePendingPages()                    // release pendings older than min(readonlyTxids)
 └─ metalock.Unlock; user code...

Commit
 ├─ rebalance()                              // merge underflow nodes
 ├─ spill()                                  // COW: free old pgid (pending), alloc new
 ├─ meta.root = newRoot
 ├─ commitFreelist() / SetFreelist(noFL)     // serialize freelist
 ├─ db.grow(file)                            // truncate + fsync (file size)
 ├─ tx.write()  ──fsync(data+freelist)──┐    // first sync
 ├─ tx.writeMeta() ──fsync(meta)────────┘    // second sync = atomic commit point
 └─ close: rwtx=nil; rwlock.Unlock
```

崩溃发生的位置→恢复结果：

- write 之前：旧 meta 完整有效；
- write 之后、writeMeta 之前：旧 meta 仍有效，新数据“孤儿”（被旧 freelist 视为分配但未引用）；
- writeMeta data 写入但 fsync 之前：可能旧 meta 或新 meta；选高的 → Validate；checksum 不过则回退到旧；
- writeMeta fsync 之后：新 meta 生效，事务持久。

bbolt 的精巧之处在于：**没有 WAL，全靠双 meta + 顺序 fsync + COW + pending freelist 实现单写多读的快照隔离与崩溃一致性**。所有不变量都集中在少量函数里（`Tx.Commit`、`Meta.Write`、`shared.Free/release`、`db.mmap/meta`），便于审阅。

---

## 附：核心文件 / 函数索引

| 主题 | 入口 |
|---|---|
| RO Begin | [db.beginTx](file:///e:/gsb/617/gsb_11/Mercury/db.go#L792-L837) |
| RW Begin | [db.beginRWTx](file:///e:/gsb/617/gsb_11/Mercury/db.go#L839-L872) |
| Commit 总线 | [Tx.Commit](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L170-L283) |
| 写脏页+fsync | [Tx.write](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L519-L592) |
| 写 meta+fsync | [Tx.writeMeta](file:///e:/gsb/617/gsb_11/Mercury/tx.go#L595-L625) |
| 选 meta | [DB.meta](file:///e:/gsb/617/gsb_11/Mercury/db.go#L1141-L1162) |
| meta 校验 | [Meta.Validate](file:///e:/gsb/617/gsb_11/Mercury/internal/common/meta.go#L25-L34) |
| meta 写出 | [Meta.Write](file:///e:/gsb/617/gsb_11/Mercury/internal/common/meta.go#L42-L58) |
| mmap 增长 | [DB.mmap / mmapSize](file:///e:/gsb/617/gsb_11/Mercury/db.go#L456-L613) |
| Page 结构 | [common.Page](file:///e:/gsb/617/gsb_11/Mercury/internal/common/page.go#L31-L36) |
| spill | [node.spill](file:///e:/gsb/617/gsb_11/Mercury/node.go#L295-L361) |
| split | [node.splitTwo](file:///e:/gsb/617/gsb_11/Mercury/node.go#L229-L266) |
| rebalance | [node.rebalance](file:///e:/gsb/617/gsb_11/Mercury/node.go#L365-L448) |
| freelist 接口 | [freelist.Interface](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/freelist.go) |
| pending 共享 | [shared.Free / ReleasePendingPages](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/shared.go) |
| array | [array.Allocate](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/array.go#L21-L61) |
| hashmap | [hashMap.Allocate](file:///e:/gsb/617/gsb_11/Mercury/internal/freelist/hashmap.go#L61-L106) |
| Cursor | [cursor.go](file:///e:/gsb/617/gsb_11/Mercury/cursor.go) |
