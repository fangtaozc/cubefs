/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_log.h"

struct cfs_log *cfs_log_new(void)
{
	struct cfs_log *log;

	log = kvzalloc(sizeof(*log), GFP_KERNEL);
	if (!log)
		return ERR_PTR(-ENOMEM);
	spin_lock_init(&log->lock);
	log->level = CFS_LOG_DEBUG;
	init_waitqueue_head(&log->wait);
	atomic_set(&log->refcnt, 1); /* cmi 持有的初始引用 */
	return log;
}

/* put 语义:cmi 释放与每个 /proc log fd 关闭各放一个引用,归 0 才 kvfree。
 * 保证 poller 睡在 log->wait 期间 log 不被释放(H9 UAF)。 */
void cfs_log_release(struct cfs_log *log)
{
	if (!log)
		return;
	if (!atomic_dec_and_test(&log->refcnt))
		return;
	kvfree(log);
}

void cfs_log_write(struct cfs_log *log, const char *fmt, ...)
{
	char *text;
	u32 nr_text, offset = 0;
	u32 copy;
	va_list args;

	va_start(args, fmt);
	nr_text = vsnprintf(NULL, 0, fmt, args);
	va_end(args);

	nr_text++;
	/* GFP_NOFS:日志经 readahead/writepage 出错路径可达(reclaim/回写上下文),
	 * GFP_KERNEL 会 direct reclaim 回写本 fs 脏页 → 递归死锁。 */
	text = kvmalloc(nr_text, GFP_NOFS);
	if (!text) {
		cfs_pr_err("oom\n");
		return;
	}

	va_start(args, fmt);
	nr_text = vsnprintf(text, nr_text, fmt, args);
	va_end(args);

	spin_lock(&log->lock);
	while (nr_text > 0) {
		copy = min_t(u32, CFS_LOG_BUF_LEN - log->head, nr_text);
		memcpy(log->buf + log->head, text + offset, copy);
		log->head = (log->head + copy) % CFS_LOG_BUF_LEN;
		log->size = min_t(u32, log->size + copy, CFS_LOG_BUF_LEN);
		if (log->size == CFS_LOG_BUF_LEN)
			log->tail =
				(log->head - CFS_LOG_BUF_LEN) % CFS_LOG_BUF_LEN;
		offset += copy;
		nr_text -= copy;
	}
	spin_unlock(&log->lock);
	wake_up(&log->wait);
	kvfree(text);
}

int cfs_log_read(struct cfs_log *log, char __user *buf, size_t len)
{
	u32 offset = 0;
	u32 copy;
	char *tmp;

	/* 绝不能在 spin_lock 内 copy_to_user:用户页缺页会睡眠 → scheduling while
	 * atomic / 死锁(读 /proc/fs/cubefs/<vol>/log,cfs_logtail.py 触发)。
	 * 锁内拷到临时内核缓冲并推进 ring,锁外再 copy_to_user。 */
	spin_lock(&log->lock);
	len = min_t(u32, len, log->size);
	spin_unlock(&log->lock);
	if (len == 0)
		return 0;
	tmp = kvmalloc(len, GFP_KERNEL);
	if (!tmp)
		return -ENOMEM;

	spin_lock(&log->lock);
	len = min_t(u32, len, log->size);
	while (offset < len) {
		copy = min_t(u32, CFS_LOG_BUF_LEN - log->tail, len - offset);
		memcpy(tmp + offset, log->buf + log->tail, copy);
		log->tail = (log->tail + copy) % CFS_LOG_BUF_LEN;
		log->size -= copy;
		offset += copy;
	}
	spin_unlock(&log->lock);

	if (offset && copy_to_user(buf, tmp, offset)) {
		kvfree(tmp);
		return -EFAULT;
	}
	kvfree(tmp);
	return offset;
}
