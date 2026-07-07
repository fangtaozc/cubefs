/*
 * mount.cubefs —— CubeFS 内核客户端 mount 助手。
 *
 * 背景：util-linux 2.39+（Ubuntu 24.04 等）默认用新 mount API
 * (fsopen/fsconfig/fsmount) 挂载。CubeFS 内核模块只实现了 legacy 的
 * file_system_type.mount，新 API 下 `mount -t cubefs` 会直接返回 EINVAL、
 * 根本进不到 cfs_mount。util-linux 约定：若存在 /sbin/mount.<fstype>，
 * `mount -t <fstype>` 会改为执行该助手。本助手直接调用 mount(2) 老系统调用，
 * 绕过新 API，从而让 `mount -t cubefs` 与 systemd cubefs-mount@ 正常工作。
 *
 * 调用约定(util-linux)：mount.cubefs <source> <target> [-sfnv] [-o opts]
 * 编译：cc -O2 -o /sbin/mount.cubefs mount.cubefs.c
 */
#include <sys/mount.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

int main(int argc, char **argv)
{
	const char *src = NULL, *tgt = NULL, *opts = "";
	int i;

	for (i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "-o") && i + 1 < argc) {
			opts = argv[++i];
		} else if (argv[i][0] == '-') {
			/* 忽略 util-linux 传入的 -s/-f/-n/-v 等标志 */
			continue;
		} else if (!src) {
			src = argv[i];
		} else if (!tgt) {
			tgt = argv[i];
		}
	}
	if (!src || !tgt) {
		fprintf(stderr,
			"usage: mount.cubefs <//masters/vol> <mountpoint> -o owner=<owner>[,opts]\n");
		return 1;
	}
	/* CubeFS 的挂载选项全部走 data 串(第 5 参)由内核模块自行解析；
	 * VFS flags(第 4 参)传 0，与直接 mount(2) 验证过的行为一致。 */
	if (mount(src, tgt, "cubefs", 0, opts) != 0) {
		fprintf(stderr, "mount.cubefs: mount(%s, %s) failed: %s\n", src,
			tgt, strerror(errno));
		return 32; /* util-linux 约定的 mount 失败码 */
	}
	return 0;
}
