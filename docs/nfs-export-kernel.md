# 内核客户端 NFS 导出(export_operations)

## 背景

让装不了 cubefs.ko 的机器通过标准 NFS 把整个 cubefs 卷挂载成共享存储。方案:网关节点用 **cubefs 内核客户端**挂载卷,再用 nfs-ganesha(VFS FSAL)或 kernel nfsd 把挂载点 **re-export** 出去。

re-export 的前提:文件系统必须实现 `export_operations`,给 NFS 编解码持久 file handle。否则 `name_to_handle_at` 返回 `EOPNOTSUPP`,ganesha VFS FSAL 与 nfsd 都无法工作。本改动为 cubefs 内核客户端补上 `export_operations`。

## 设计

**file handle 布局**:`[ino_lo:u32][ino_hi:u32][i_generation:u32]` = 3 个 u32(12 字节)。自定义 `fh_type = CFS_FILEID_INO64_GEN(0x81)`,避开 `enum fid_type` 现有取值;ino 是 64 位,故不能复用 `FILEID_INO32_GEN`。

**fh 安全性**:cubefs inode 的 ino 由 metanode cursor **单调递增、不重用**,叠加 inode 的 `generation` 字段双保险。删除的 inode 其 ino 不会被复用,旧 fh 解析时 `cfs_meta_get` 返回 `-ENOENT` → `ESTALE`,不会错指到新文件。

**non-connectable**:整卷 export + `no_subtree_check` 场景下,`get_parent`/`fh_to_parent` 置 NULL 即可(cubefs meta 协议无反向 parent lookup RPC)。NFS client 总携带 parent fh 做 lookup,不依赖服务端 reconnect_path。

### 实现(client_kernel/cfs_fs.c)

- `cfs_encode_fh(inode, fh, max_len, parent)`:编 `i_ino` 高低 32 位 + `i_generation`;buf 不足写回所需长度并返回 `FILEID_INVALID`。忽略 parent。
- `cfs_fh_to_dentry(sb, fid, fh_len, fh_type)`:解 ino+gen → `cfs_meta_get(meta, ino, &iinfo)`(纯 ino 拉 InodeInfo)→ `cfs_inode_new` 重建 VFS inode → `cfs_packet_inode_release(iinfo)` → **generation 校验**(不匹配 `iput`+`ESTALE`)→ `d_obtain_alias`。
  - 引用计数:`cfs_inode_new`(iget_locked)持 1 引用,`d_obtain_alias` 消费它;仅 generation 校验失败的早退路径需手动 `iput`。
  - `cfs_meta_get` 返回 `-ENOENT`(inode 已删)→ `ESTALE`,其他错误透传。
- `cfs_export_ops`:仅 `encode_fh` + `fh_to_dentry`;在 `cfs_fs_fill_super` 注册 `sb->s_export_op`。

gen 统一用 `inode->i_generation`(u32),encode 与校验同源,规避 metanode u64 gen → i_generation u32 截断的不一致。

## 改动文件

- `client_kernel/cfs_fs.c`:`cfs_encode_fh`/`cfs_fh_to_dentry`/`cfs_export_ops` + `fill_super` 注册 + `#include <linux/exportfs.h>`
- `client_kernel/cfs_fs.h`:`extern cfs_export_ops` + `CFS_FILEID_INO64_GEN`/`CFS_FH_LEN_U32` 宏

## NFS server 端配置要点

cubefs `cfs_statfs` 的 `f_fsid={0,0}`(未设),所以 server 端**必须显式指定 fsid**:
- ganesha VFS FSAL:`EXPORT { Path=/mnt/cubefs-kernel; FSAL{Name=VFS;} Filesystem_Id=100.1; Protocols=4; ... }`
- kernel nfsd:`/etc/exports` 加 `fsid=100,no_subtree_check`
- 建议 NFSv4-only(单 2049 端口)

## 测试(在挂载了 cubefs 的节点)

1. **fh 编解码**:`name_to_handle_at` 应从 `EOPNOTSUPP` 转为成功(handle_bytes=12, type=0x81);`open_by_handle_at` 回读内容一致。
2. **ESTALE**:取文件 handle 后 `rm`,再 `open_by_handle_at` → `ESTALE`。
3. **ganesha 挂载**:起 ganesha → 远端 `mount -t nfs4 <gw>:/cubefs /mnt/nfs` → 读写、多机并发(close-to-open 一致性)。
4. **server 重启不 ESTALE**:client 持打开文件 → 重启 ganesha → 继续 read/stat 不应 ESTALE(fh 纯 ino+gen 无服务端易失状态)。

## macOS 客户端兼容:必须禁用 delegation

**现象**:macOS(`mount -t nfs -o vers=4.0`)挂载后,`ls`/读、`touch`(建空文件)均正常,但**写数据**(`echo x > f`、fio 写)卡住并报 `nfs server ...: resource temporarily unavailable (jukebox)`。Linux 客户端(`mount -t nfs4`,协商到 4.1+)完全正常。

**根因**:`jukebox` = `NFS4ERR_DELAY`。macOS NFS client 写文件时会**主动请求 write delegation**(Linux 不激进请求),ganesha 默认授予;但 cubefs.ko 的 `cfs_file_fops` **无 lease/delegation 回调**,delegation 的授予/回收一旦卡住,ganesha 即对写操作返 `NFS4ERR_DELAY`。

**修复**:ganesha `EXPORT` 块加 `Delegations = None;`(per-export 关闭授予)。单网关多客户端场景 delegation 收益小、一致性风险大,本就该关。

**mac 的 DELAY 退避陷阱**:macOS NFS client 一旦收到一次 `NFS4ERR_DELAY`,会进入**本地退避**——之后对该挂载的写**直接短路返回 jukebox、不再发网络**(tcpdump 抓到 0 写包,只剩每 ~15s 一次的 getattr 保活)。所以**改完 server 配置后,mac 必须 `sudo umount -f` 重挂**才能清掉退避状态,否则旧退避仍卡,看着像没修好。

## 诊断要点(踩过的坑)

- **grace period 不是 jukebox 来源**:ganesha 重启日志虽打 `IN GRACE duration 30`,但无 prior client 需 reclaim 时**同一秒立即 `NOT IN GRACE`**,grace 实际不持续。排查 jukebox 时不要误判成 grace。
- **抓包要对齐时序**:mac 写卡时若已进退避,抓包是 0 写包;且 mac 每 15s 的 getattr 保活会干扰判断。务必"先 `umount -f` 重挂、再抓重挂后的首次写"。
- **ganesha dbus 动态调日志在本构建不可用**:`org.ganesha.nfsd.log.component` 接口 `Set`/`GetAll` 报错。要开 debug 只能改 configmap 的 `LOG{Components{...}}` + 重启 pod。

## 性能基线(my-vol,2 副本,bs=1M)

| 客户端/链路 | 操作 | 带宽 | 备注 |
|------|------|------|------|
| 内核客户端直测(`/mnt/cubefs-kernel`) | dd 单流写 direct | 215 MB/s | cubefs 单流写上限 |
| 集群万兆内网 84 → NFS 网关 | dd 单流写 direct | 190 MB/s | ganesha 仅损耗 ~12% |
| 同机房 dev_bd(RTT 0.2ms,vers=4.2) → NFS | dd 单流写 direct | 181 MB/s | |
| 同机房 dev_bd → NFS | fio iodepth16 写 | **427 MB/s** | 并发把写带宽拉满 |
| 同机房 dev_bd → NFS | dd buffered 读 | 1.5 GB/s | |
| 同机房 dev_bd → NFS | fio iodepth16 buffered 读 | **1.2 GB/s** | |
| 办公网 mac → NFS | fio buffered 写 | 25 MB/s | 瓶颈在 mac↔机房网络(≈200Mbps),非 cubefs/ganesha |

**结论**:ganesha 网关几乎无损耗(集群内 190 vs 内核 215);同机房客户端读写都很好(写 180–430 MB/s、读 1.2–1.5 GB/s),实际应用完全可用。远端 mac 慢是**网络物理限制**(数据必须从办公网传到机房),cubefs/ganesha 侧无优化空间,提速只能改网络(有线/专线/就近部署网关)。

## O_DIRECT 读冷读带宽崩溃 → 已修复(direct 读改 async 并发)

**现象**:`fio --direct=1 --rw=read --bs=1M --size=10G --iodepth=16` 读大文件时 O_DIRECT 读极慢、表现为"卡"(经 NFS 时 ganesha worker 全占、client `nfs: server not responding`、fio D 态 `io_getevents`)。本地直测冷读(drop cache)同样复现。

**根因(非 NFS/非死锁,是 sync 串行)**:`cfs_extent_read_pages` 原对 `direct_io` 走 `extent_read_pages_sync`——逐 `EXTENT_BLOCK` **串行发包、同步等 datanode 返回**;冷读(datanode 从盘读)时每个 RPC 的磁盘延迟串行累加,带宽崩溃。buffered 读走 `extent_read_pages_async`——经 work queue **并发提交多个读包**、reply_cb 异步解锁,流水线掩盖单 RPC 延迟。实测同一冷读 3G:**sync direct <80MB/s vs async buffered 1.5GB/s**(同量级网络/盘,纯路径差异)。该 `if(direct_io) sync` 是 v3.6 内核客户端移植时的原始设计(commit e5c5847db),**非为修 bug 引入,也非分支缺主线修复**。

**修复**:`cfs_extent_read_pages` 统一走 async(删除 `direct_io` 分支与死代码 `extent_read_pages_sync`)。direct 的临时页同样经 reply_cb 解锁、由外层 `cfs_extent_dio_read_write` 的 `wait_on_page_locked` 等待完成 + `TestClearPageError` 判错,语义与 buffered 一致、安全。数据正确性另靠 packet 层短读判错(见 netem 读修复)。commit 06bfedd2b,ko srcversion **92EF7B91**。

**验证通过(2026-06-16,ko 92EF7B91,109 节点)**:dev_bd 挂 109 NFS 跑当初卡死场景 `fio --direct=1 --rw=read --bs=1M --iodepth=16`:**卡死 → 1829 MiB/s(1.9GB/s)**,写 886 MB/s;`fio --verify=md5`(O_DIRECT 写带校验和+O_DIRECT 读回逐块校验)`err=0`,数据正确。注:**async 优化的是「多 in-flight 并发」**,`dd bs=1M` 单流(depth=1,无 readahead)仍受冷读单 RPC 延迟限制、提升有限,这非本次修复目标——目标是 fio/NFS 高并发场景,已达成。

**边界(修复前)**:O_DIRECT 小文件/热读正常(datanode 缓存命中),仅「大文件/冷读」串行延迟累加才暴露;buffered 读写、O_DIRECT 写一直正常。

**踩到卡死后清理**:卡住进程 D 态(`kill -9` 无效),`umount -f -l` 卸载挂载点但进程仍 Ds,需等该笔 IO 返回或重启 ganesha pod 断开 NFS。
