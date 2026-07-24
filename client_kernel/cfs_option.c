
/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_option.h"

#define DENTRY_CACHE_VALID_MS 5 * 1000u
#define ATTR_CACHE_VALID_MS 30 * 1000u
#define QUOTA_CACHE_VALID_MS 120 * 1000u

static int addrs_parse(const char *str, size_t len,
		       struct sockaddr_storage_array *addrs)
{
	size_t i;
	size_t num;
	const char *s, *e;
	int ret;

	num = 1;
	for (i = 0; i < len; i++)
		num += str[i] == ',';

	ret = sockaddr_storage_array_init(addrs, num);
	if (ret < 0)
		return ret;

	s = str;
	for (; addrs->num < addrs->cap; addrs->num++) {
		e = strnchr(s, str + len - s, ',');
		ret = cfs_parse_addr(s, e ? e - s : str + len - s,
				     &addrs->base[addrs->num]);
		if (ret < 0)
			break;
		s = e + 1;
	}
	if (ret < 0) {
		sockaddr_storage_array_clear(addrs);
		return ret;
	}
	return 0;
}

static void cfs_options_clear(struct cfs_options *options)
{
	if (!options)
		return;
	/* 幂等:释放后置 NULL,避免 parse 失败路径与 cfs_options_release
	 * 重复清理时 kfree 已释放指针造成 double-free(BUG slub.c:384)。
	 * kfree(NULL) 安全,故不再判空。sockaddr_storage_array_clear 已幂等。 */
	sockaddr_storage_array_clear(&options->addrs);
	kfree(options->volume);
	options->volume = NULL;
	kfree(options->path);
	options->path = NULL;
	kfree(options->owner);
	options->owner = NULL;
	kfree(options->oss_accel_mover_addr);
	options->oss_accel_mover_addr = NULL;
	kfree(options->oss_accel_admin_token);
	options->oss_accel_admin_token = NULL;
}

/**
 * @param dev_str in, format: //172.16.1.101:17010,172.16.1.102:17010,172.16.1.103:17010/ltptest
 * @param opt_str in, format: owner=ltptest,key1=val1
 * @param option out
 */
static int cfs_options_parse(const char *dev_str, const char *opt_str,
			     struct cfs_options *options)
{
	const char *start, *end;
	int ret;

	if (!dev_str)
		return -EINVAL;
	if (strncmp(dev_str, "//", 2) != 0)
		return -EINVAL;
	start = dev_str + 2;

	/**
     * parse master addrs
     */
	if (!(end = strchr(start, '/'))) {
		return -EINVAL;
	}
	ret = addrs_parse(start, end - start, &options->addrs);
	if (ret < 0) {
		cfs_options_clear(options);
		return ret;
	}
	start = end + 1;

	/**
     * parse volume
     */
	end = strchr(start, '/');
	if (end)
		options->volume = kstrndup(start, end - start, GFP_NOFS);
	else
		options->volume = kstrdup(start, GFP_NOFS);
	if (!options->volume) {
		cfs_options_clear(options);
		return -ENOMEM;
	}

	/**
     * parse path
     */
	if (end) {
		start = end + 1;
		options->path = kstrdup(start, GFP_NOFS);
		if (!options->path) {
			cfs_options_clear(options);
			return -ENOMEM;
		}
	}

	/* opt_str 可能为 NULL(mount 未带 -o 选项),不能无条件解引用 */
	start = opt_str;
	while (start && *start) {
		if (strncmp(start, "owner=", 6) == 0) {
			start += 6;
			end = strchr(start, ',');
			if (end)
				options->owner =
					kstrndup(start, end - start, GFP_NOFS);
			else
				options->owner = kstrdup(start, GFP_NOFS);
			if (!options->owner) {
				cfs_options_clear(options);
				return -ENOMEM;
			}
			if (end)
				start = end + 1;
			else
				break;
		} else if (strncmp(start, "dentry_cache_valid_ms=", 22) == 0) {
			start += 22;
			end = strchr(start, ',');
			if (end)
				ret = cfs_kstrntou32(
					start, end - start, 10,
					&options->dentry_cache_valid_ms);
			else
				ret = kstrtou32(
					start, 10,
					&options->dentry_cache_valid_ms);
			if (ret < 0) {
				cfs_options_clear(options);
				return -EINVAL;
			}
			if (end)
				start = end + 1;
			else
				break;
		} else if (strncmp(start, "attr_cache_valid_ms=", 20) == 0) {
			start += 20;
			end = strchr(start, ',');
			if (end)
				ret = cfs_kstrntou32(
					start, end - start, 10,
					&options->attr_cache_valid_ms);
			else
				ret = kstrtou32(start, 10,
						&options->attr_cache_valid_ms);
			if (ret < 0) {
				cfs_options_clear(options);
				return -EINVAL;
			}
			if (end)
				start = end + 1;
			else
				break;
		} else if (strncmp(start, "quota_cache_valid_ms=", 21) == 0) {
			start += 21;
			end = strchr(start, ',');
			if (end)
				ret = cfs_kstrntou32(
					start, end - start, 10,
					&options->quota_cache_valid_ms);
			else
				ret = kstrtou32(start, 10,
						&options->quota_cache_valid_ms);
			if (ret < 0) {
				cfs_options_clear(options);
				return -EINVAL;
			}
			if (end)
				start = end + 1;
			else
				break;
		} else if (strncmp(start, "ossaccelmoveraddr=", 18) == 0) {
			/* Single "host:port" only (no comma-separated list) —
			 * the value would otherwise collide with this parser's
			 * own comma-as-option-separator convention. This also
			 * matches actual behavior: the FUSE client's equivalent
			 * option (client/fs/oss_accel.go) only ever uses the
			 * first address of its own comma-separated list too. */
			start += 18;
			end = strchr(start, ',');
			if (end)
				options->oss_accel_mover_addr = kstrndup(
					start, end - start, GFP_NOFS);
			else
				options->oss_accel_mover_addr =
					kstrdup(start, GFP_NOFS);
			if (!options->oss_accel_mover_addr) {
				cfs_options_clear(options);
				return -ENOMEM;
			}
			if (end)
				start = end + 1;
			else
				break;
		} else if (strncmp(start, "ossacceladmintoken=", 19) == 0) {
			/* 系统层面收尾: shared admin token sent as
			 * `Authorization: Bearer <token>` on every
			 * /ossAccelRecall call — see cfs_option.h. Single
			 * value, same comma-terminates-value convention as
			 * ossaccelmoveraddr= above. */
			start += 19;
			end = strchr(start, ',');
			if (end)
				options->oss_accel_admin_token = kstrndup(
					start, end - start, GFP_NOFS);
			else
				options->oss_accel_admin_token =
					kstrdup(start, GFP_NOFS);
			if (!options->oss_accel_admin_token) {
				cfs_options_clear(options);
				return -ENOMEM;
			}
			if (end)
				start = end + 1;
			else
				break;
		} else if (strncmp(start, "enable_quota=", 13) == 0) {
			start += 13;
			end = strchr(start, ',');
			/* 统一用 cfs_kstrntobool(支持 "true"/"false");内核 kstrtobool
			 * 在 5.15 不识别 "false"/"true",末尾(无逗号)时会返回 -EINVAL
			 * 触发上面的清理路径。 */
			if (end)
				ret = cfs_kstrntobool(start, end - start,
						      &options->enable_quota);
			else
				ret = cfs_kstrntobool(start, strlen(start),
						      &options->enable_quota);
			if (ret < 0) {
				cfs_options_clear(options);
				return -EINVAL;
			}
			if (end)
				start = end + 1;
			else
				break;
		} else {
			start++;
		}
	}

	/* owner 必填:缺失时(mount 未带 -o owner=,或 opt_str 为空/NULL)若放行,
	 * 下游以 NULL owner 做字符串操作会内核 oops(panic_on_oops → 节点重启)。
	 * 显式要求,mount 干净失败 -EINVAL 而非崩溃。 */
	if (!options->owner) {
		cfs_pr_err("mount: option 'owner' is required\n");
		cfs_options_clear(options);
		return -EINVAL;
	}

	return 0;
}

/**
 * @return options if success, error code if failed.
 */
struct cfs_options *cfs_options_new(const char *dev_str, const char *opt_str)
{
	struct cfs_options *options;
	int ret;

	options = kzalloc(sizeof(*options), GFP_NOFS);
	if (!options)
		return ERR_PTR(-ENOMEM);
	options->dentry_cache_valid_ms = DENTRY_CACHE_VALID_MS;
	options->attr_cache_valid_ms = ATTR_CACHE_VALID_MS;
	options->quota_cache_valid_ms = QUOTA_CACHE_VALID_MS;
	ret = cfs_options_parse(dev_str, opt_str, options);
	if (ret < 0) {
		cfs_options_release(options);
		return ERR_PTR(ret);
	}
	return options;
}

void cfs_options_release(struct cfs_options *options)
{
	if (!options)
		return;
	cfs_options_clear(options);
	kfree(options);
}
