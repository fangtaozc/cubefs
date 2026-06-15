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
