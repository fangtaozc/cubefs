/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_FS_H__
#define __CFS_FS_H__

#include "cfs_common.h"

#include "cfs_extent.h"
#include "cfs_log.h"
#include "cfs_master.h"
#include "cfs_meta.h"
#include "cfs_option.h"
#include "cfs_stats.h"

extern const struct address_space_operations cfs_address_ops;
extern const struct file_operations cfs_file_fops;
extern const struct inode_operations cfs_file_iops;
extern const struct file_operations cfs_dir_fops;
extern const struct inode_operations cfs_dir_iops;
extern const struct inode_operations cfs_symlink_iops;
extern const struct inode_operations cfs_special_iops;
extern const struct dentry_operations cfs_dentry_ops;
extern const struct super_operations cfs_super_ops;
extern const struct export_operations cfs_export_ops;
extern struct file_system_type cfs_fs_type;

/* NFS file handle 布局:[ino_lo:u32][ino_hi:u32][i_generation:u32] = 3 个 u32。
 * fh_type 取私有区(0x80+),避开 enum fid_type(linux/exportfs.h)现有取值。 */
#define CFS_FILEID_INO64_GEN 0x81
#define CFS_FH_LEN_U32 3

struct cfs_mount_info {
	struct cfs_options *options;
	/* Leaf name actually registered under /proc/fs/cubefs (usually the
	 * volume name, "<vol>-<seq>" when a live mount already holds it) plus
	 * this mount's link in the module-wide registry of taken names. The
	 * registry is what lets init_proc pick a free name WITHOUT asking
	 * procfs — proc_mkdir()'s duplicate path WARNs with a full stack trace
	 * before it returns NULL, so "try and fall back" cannot be quiet. */
	char *proc_leaf;
	struct list_head proc_leaf_link;
	struct proc_dir_entry *proc_dir;
	struct proc_dir_entry *proc_log;
	struct proc_dir_entry *proc_stats;
	struct cfs_log *log;
	struct cfs_stats *stats;
	struct cfs_master_client *master;
	struct cfs_meta_client *meta;
	struct cfs_extent_client *ec;
	atomic_long_t links_limit;
	struct delayed_work update_limit_work;
};

struct cfs_mount_info *cfs_mount_info_new(struct cfs_options *options);
void cfs_mount_info_release(struct cfs_mount_info *cmi);
int cfs_fs_module_init(void);
void cfs_fs_module_exit(void);
#endif
