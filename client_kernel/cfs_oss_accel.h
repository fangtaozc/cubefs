/*
 * Copyright 2026 The CubeFS Authors.
 */
#ifndef CFS_OSS_ACCEL_H
#define CFS_OSS_ACCEL_H

#include <linux/types.h>

/*
 * cfs_oss_accel_recall_via_helper shells out to a small standalone Go binary
 * (cmd/cfs-oss-accel-helper) that performs one oss-accel cold-read recall
 * (an HTTP call to a mover's /ossAccelRecall — see lcnode/oss_accel.go).
 *
 * Kernel space has no existing mechanism to make an HTTP call directly (no
 * HTTP client, no S3 stack, and deliberately none should be added here —
 * this fork's client/fs/oss_accel.go FUSE-side gate keeps that complexity in
 * a separate userspace mover process too, just reached via a plain HTTP call
 * instead of a helper exec). This function is a thin, self-contained wrapper
 * around call_usermodehelper with no dependency on any cfs_inode/superblock
 * state — callers (the read-gate orchestration in cfs_fs.c, which knows
 * about inode locking and cache invalidation) own everything else.
 *
 * s3key is the oss-accel.s3key xattr value, forwarded as-is to the helper's
 * -path flag (the mover treats it as the S3 key basis, not a filesystem
 * path — matching client/fs/oss_accel.go's own gate exactly). checksum may
 * be an empty string (skips server-side verification, not a security
 * concern since the bytes still come from the inode's own designated S3 key).
 *
 * admin_token (系统层面收尾) is forwarded as-is to the helper's -token flag;
 * NULL/empty sends no Authorization header, matching lcnode's own
 * passthrough-when-empty behavior when its own admin token isn't configured.
 *
 * Returns:
 *   0        recall succeeded (or the mover reports the inode already at
 *            the target class); caller should refresh cached extents/attrs
 *            and proceed with a normal read.
 *   -EAGAIN  not yet safe to recall (helper exit code 11, mover 425);
 *            caller must return -EAGAIN to the reader, not retry internally.
 *   -EIO     any other failure (bad helper path, network error, non-200/425
 *            mover response). Caller must not fall through to a read that
 *            could return zero-filled data.
 */
int cfs_oss_accel_recall_via_helper(const char *mover_addr,
				    const char *admin_token, const char *vol,
				    u64 ino, u64 size, const char *s3key,
				    const char *checksum);

#endif
