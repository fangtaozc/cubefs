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
	/* 系统层面收尾: shared admin token from mount option
	 * "ossacceladmintoken=..." — sent as `Authorization: Bearer <token>`
	 * on every /ossAccelRecall call (lcnode/oss_accel_auth.go gates that
	 * endpoint behind this same token). NULL/absent sends no header,
	 * matching lcnode's own passthrough-when-empty behavior. */
	char *oss_accel_admin_token;
};

struct cfs_options *cfs_options_new(const char *dev_str, const char *opt_str);
void cfs_options_release(struct cfs_options *options);

/* Returns a newly allocated copy of @opt_str with the value of every
 * credential-bearing option replaced by a fixed placeholder, for logging.
 * cfs_mount() logs the raw mount options at entry, which put the lcnode admin
 * token in cleartext into the kernel ring buffer — anyone with dmesg access
 * then had the token (the `mount` output never showed it, so this was easy to
 * miss). Caller owns the result and must kfree() it; NULL on allocation
 * failure, in which case callers must log nothing rather than the raw string.
 */
char *cfs_options_redact(const char *opt_str);

#endif
