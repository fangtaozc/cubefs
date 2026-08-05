/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_COMMON_H__
#define __CFS_COMMON_H__

#include <crypto/hash.h>
#include <crypto/md5.h>
#include <linux/backing-dev.h>
#include <linux/crc32.h>
#include <linux/fs.h>
#include <linux/hashtable.h>
#include <linux/inet.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/list_lru.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/namei.h>
#include <linux/net.h>
#include <linux/pagemap.h>
#include <linux/pagevec.h>
#include <linux/poll.h>
#include <linux/printk.h>
#include <linux/signal.h>
#include <linux/slab.h>
#include <linux/socket.h>
#include <linux/spinlock.h>
#include <linux/statfs.h>
#include <linux/string.h>
#include <linux/tcp.h>
#include <linux/vmalloc.h>
#include <linux/writeback.h>
#include <linux/xattr.h>
#ifdef KERNEL_HAS_SOCK_CREATE_KERN_WITH_NET
#include <net/net_namespace.h>
#endif

#include "config.h"

#undef pr_fmt
#define pr_fmt(fmt) "cfs: %s() " fmt

#define cfs_pr_err(fmt, ...) pr_err(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_pr_warning(fmt, ...) pr_warn(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_pr_notice(fmt, ...) pr_notice(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_pr_info(fmt, ...) pr_info(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_pr_debug(fmt, ...) \
	printk(KERN_DEBUG pr_fmt(fmt), __FUNCTION__, ##__VA_ARGS__)

#define cfs_move(p, v) \
	p;             \
	p = v

/* define array */

#define DEFINE_ARRAY(type, name)                                               \
	struct name##_array {                                                  \
		type name *base;                                               \
		size_t num;                                                    \
		size_t cap;                                                    \
	};                                                                     \
	static inline int name##_array_init(struct name##_array *array,        \
					    size_t cap)                        \
	{                                                                      \
		array->base = NULL;                                            \
		array->num = 0;                                                \
		array->cap = cap;                                              \
		if (array->cap == 0)                                           \
			return 0;                                              \
		if (!(array->base = kcalloc(                                   \
			      array->cap, sizeof(array->base[0]), GFP_NOFS)))  \
			return -ENOMEM;                                        \
		return 0;                                                      \
	}                                                                      \
	static inline void name##_array_clear(struct name##_array *array)      \
	{                                                                      \
		if (!array || !array->base)                                    \
			return;                                                \
		while (array->num > 0) {                                       \
			array->num--;                                          \
			name##_clear(&array->base[array->num]);                \
		}                                                              \
		kfree(array->base);                                            \
		array->base = NULL;                                            \
		array->cap = 0;                                                \
	}                                                                      \
	static inline void name##_array_move(struct name##_array *dst,         \
					     struct name##_array *src)         \
	{                                                                      \
		BUG_ON(!dst || !src);                                          \
		dst->base = cfs_move(src->base, NULL);                         \
		dst->num = cfs_move(src->num, 0);                              \
		dst->cap = cfs_move(src->cap, 0);                              \
	}                                                                      \
	static inline int name##_array_clone(struct name##_array *dst,         \
					     const struct name##_array *src)   \
	{                                                                      \
		int ret;                                                       \
		BUG_ON(!dst || !src);                                          \
		ret = name##_array_init(dst, src->num);                        \
		if (ret < 0)                                                   \
			return ret;                                            \
		dst->num = src->num;                                           \
		memcpy(dst->base, src->base, sizeof(src->base[0]) * src->num); \
		return 0;                                                      \
	}

/**
 * define u64_array.
 */
static inline void u64_clear(u64 *u)
{
	*u = 0;
}

DEFINE_ARRAY(, u64)

/**
 * define string_array.
 */
typedef char *string;
static inline void string_clear(string *s)
{
	kfree(*s);
	*s = NULL;
}

DEFINE_ARRAY(, string)

/**
 * define sockaddr_storage_array.
 */
static inline void sockaddr_storage_clear(struct sockaddr_storage *ss)
{
	(void)ss;
}

DEFINE_ARRAY(struct, sockaddr_storage)

/**
 * define default block size of cubefs.
 */
#define CFS_DEFAULT_BLK_SIZE (1u << 12)

/**
 * define string parse function.
 */
#define CFS_MAX_U64_STRING_LEN 128
static inline int cfs_kstrntou64(const char *start, size_t len,
				 unsigned int base, u64 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtou64(buf, base, res);
}

static inline int cfs_kstrntos64(const char *start, size_t len,
				 unsigned int base, s64 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtos64(buf, base, res);
}

static inline int cfs_kstrntou32(const char *start, size_t len,
				 unsigned int base, u32 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtou32(buf, base, res);
}

static inline int cfs_kstrntou16(const char *start, size_t len,
				 unsigned int base, u16 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtou16(buf, base, res);
}

static inline int cfs_kstrntou8(const char *start, size_t len,
				unsigned int base, u8 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtou8(buf, base, res);
}

static inline int cfs_kstrntos8(const char *start, size_t len,
				unsigned int base, s8 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtos8(buf, base, res);
}

static inline int cfs_kstrntobool(const char *start, size_t len, bool *res)
{
	/* 必须精确长度匹配:原用调用方传入的 len 做 strncasecmp,len==0 会匹配
	 * "false"(比较 0 字符返 0)、len<5 时 "t"/"tr"/"f"/"fal" 等前缀都被接受
	 * → 截断/空布尔值被静默曲解而非报错。 */
	if (len == 5 && strncasecmp(start, "false", 5) == 0) {
		*res = false;
		return 0;
	} else if (len == 4 && strncasecmp(start, "true", 4) == 0) {
		*res = true;
		return 0;
	}
	return -EINVAL;
}

const char *cfs_pr_addr(const struct sockaddr_storage *ss);
int cfs_parse_addr(const char *str, size_t len, struct sockaddr_storage *ss);
int cfs_addr_cmp(const struct sockaddr_storage *ss1,
		 const struct sockaddr_storage *ss2);
const char *cfs_pr_time(struct timespec64 *time);
int cfs_parse_time(const char *str, size_t len, struct timespec64 *time);

int cfs_base64_encode(const char *str, size_t len, char **base64);
/* out_len 可传 NULL(不关心真实解码长度,沿用旧调用方式——如 symlink target,
 * 本身按 NUL 结尾字符串处理,不受影响)。非 NULL 时按输入末尾 '=' 补位个数
 * 算出真实解码字节数写回——base64_decode 的输出缓冲固定按 (base64_len/4)*3
 * 分配,但当原始内容不是 3 的整数倍时,末尾 1-2 个字节是补位字符解出来的
 * 0x00,不是真实内容;调用方如果把结果当成任意二进制(而不是天然不含内嵌
 * NUL 的路径字符串)使用,必须知道真实长度在哪里截止,不能不做修改直接假设
 * 整块缓冲都是有效数据。修复T3(内核客户端二进制xattr)引入。 */
int cfs_base64_decode(const char *base64, size_t base64_len, char **str, size_t *out_len);

#endif
