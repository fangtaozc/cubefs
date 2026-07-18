/*
 * Copyright 2026 The CubeFS Authors.
 */
#include "cfs_oss_accel.h"
#include "cfs_packet.h"

#include <linux/errno.h>
#include <linux/kmod.h>
#include <linux/moduleparam.h>
#include <linux/sched.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/wait.h>

/* Exit codes from cmd/cfs-oss-accel-helper/main.go — kept in sync manually,
 * there is no shared header between a kernel module and a Go binary. */
#define HELPER_EXIT_SUCCESS 0
#define HELPER_EXIT_TRY_AGAIN 11

static char *oss_accel_helper_path = "/usr/local/bin/cfs-oss-accel-helper";
module_param(oss_accel_helper_path, charp, 0644);
MODULE_PARM_DESC(oss_accel_helper_path,
		 "path to the cfs-oss-accel-helper binary invoked to recall oss-accel cold files (see cmd/cfs-oss-accel-helper)");

int cfs_oss_accel_recall_via_helper(const char *mover_addr, const char *vol,
				    u64 ino, u64 size, const char *s3key,
				    const char *checksum)
{
	char *argv[11];
	char *envp[] = { "HOME=/", "PATH=/sbin:/usr/sbin:/bin:/usr/bin",
			 NULL };
	char *mover_arg, *vol_arg, *ino_arg, *size_arg, *path_arg,
		*checksum_arg, *sc_arg, *vsc_arg, *asc_arg;
	int ret;
	int status;

	mover_arg = kasprintf(GFP_NOFS, "-mover=%s", mover_addr);
	vol_arg = kasprintf(GFP_NOFS, "-vol=%s", vol);
	ino_arg = kasprintf(GFP_NOFS, "-ino=%llu", ino);
	size_arg = kasprintf(GFP_NOFS, "-size=%llu", size);
	path_arg = kasprintf(GFP_NOFS, "-path=%s", s3key);
	checksum_arg = kasprintf(GFP_NOFS, "-checksum=%s", checksum);
	sc_arg = kasprintf(GFP_NOFS, "-sc=%u", CFS_STORAGE_CLASS_REPLICA_HDD);
	vsc_arg = kasprintf(GFP_NOFS, "-vsc=%u",
			    CFS_STORAGE_CLASS_REPLICA_HDD);
	asc_arg = kasprintf(GFP_NOFS, "-asc=%u",
			    CFS_STORAGE_CLASS_REPLICA_HDD);
	if (!mover_arg || !vol_arg || !ino_arg || !size_arg || !path_arg ||
	    !checksum_arg || !sc_arg || !vsc_arg || !asc_arg) {
		ret = -ENOMEM;
		goto out_free;
	}

	argv[0] = oss_accel_helper_path;
	argv[1] = mover_arg;
	argv[2] = vol_arg;
	argv[3] = ino_arg;
	argv[4] = size_arg;
	argv[5] = sc_arg;
	argv[6] = vsc_arg;
	argv[7] = asc_arg;
	argv[8] = path_arg;
	argv[9] = checksum_arg;
	argv[10] = NULL;

	/* UMH_WAIT_PROC blocks this (process-context, sleepable) thread until
	 * the helper exits. status encodes the same wait()-status word a
	 * userspace wait(2) would see, but WIFEXITED/WEXITSTATUS are libc
	 * macros, not available in kernel space — extract the exit code
	 * manually (status>>8 & 0xff is exactly what WEXITSTATUS expands to).
	 * A negative return means the fork/exec itself failed (helper
	 * missing, bad permissions) before any exit code exists; bit 0x7f of
	 * a non-negative status set means the child died from a signal
	 * rather than exiting normally — treat both as a hard failure. */
	status = call_usermodehelper(oss_accel_helper_path, argv, envp,
				     UMH_WAIT_PROC);
	if (status < 0 || (status & 0x7f) != 0) {
		ret = -EIO;
		goto out_free;
	}
	switch ((status >> 8) & 0xff) {
	case HELPER_EXIT_SUCCESS:
		ret = 0;
		break;
	case HELPER_EXIT_TRY_AGAIN:
		ret = -EAGAIN;
		break;
	default:
		ret = -EIO;
		break;
	}

out_free:
	kfree(mover_arg);
	kfree(vol_arg);
	kfree(ino_arg);
	kfree(size_arg);
	kfree(path_arg);
	kfree(checksum_arg);
	kfree(sc_arg);
	kfree(vsc_arg);
	kfree(asc_arg);
	return ret;
}
