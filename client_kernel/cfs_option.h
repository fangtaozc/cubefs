/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_OPTION_H__
#define __CFS_OPTION_H__

#include "cfs_common.h"

struct cfs_options {
	struct sockaddr_storage_array addrs;
	char *volume;
	char *path;
	char *owner;
	u32 dentry_cache_valid_ms;
	u32 attr_cache_valid_ms;
	u32 quota_cache_valid_ms;
	bool enable_quota;
	/* First address (host:port) from mount option
	 * "ossaccelmoveraddr=host1:port1,host2:port2,..." — mirrors the FUSE
	 * client's ossAccelMoverAddr mount option, including only ever using
	 * the first address (client/fs/oss_accel.go does the same). NULL when
	 * the option is absent, meaning the cold-read gate stays fully
	 * disabled (zero behavior change) — see cfs_fs.c cfs_oss_accel_gate. */
	char *oss_accel_mover_addr;
};

struct cfs_options *cfs_options_new(const char *dev_str, const char *opt_str);
void cfs_options_release(struct cfs_options *options);

#endif
