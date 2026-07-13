/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_extent.h"
#include <linux/kthread.h>

extern struct workqueue_struct *extent_work_queue;

static void extent_reader_tx_work_cb(struct work_struct *work);
/* rx 改为 per-reader 专用 recv kthread，原理见 cfs_extent_writer.c 同改注释。 */
static int extent_reader_rx_thread_fn(void *data);

struct cfs_extent_reader *cfs_extent_reader_new(struct cfs_extent_stream *es,
						struct cfs_data_partition *dp,
						u32 host_idx, u64 ext_id)
{
	struct cfs_extent_reader *reader;
	int ret;

	BUG_ON(dp == NULL);
	/* 失败一律返回 NULL(不返回 ERR_PTR):所有调用点都用 if(!x) 判断,
	 * 返回 ERR_PTR 会被漏判 → 毒指针入链表/解引用 → panic。 */
	reader = kzalloc(sizeof(*reader), GFP_NOFS);
	if (!reader)
		return NULL;
	host_idx = host_idx % dp->members.num;
	ret = cfs_socket_create(CFS_SOCK_TYPE_TCP, &dp->members.base[host_idx],
				es->ec->log, &reader->sock);
	if (ret < 0) {
		kfree(reader);
		return NULL;
	}
	reader->es = es;
	reader->dp = dp;
	reader->ext_id = ext_id;
	spin_lock_init(&reader->lock_tx);
	spin_lock_init(&reader->lock_rx);
	INIT_LIST_HEAD(&reader->tx_packets);
	INIT_LIST_HEAD(&reader->rx_packets);
	INIT_WORK(&reader->tx_work, extent_reader_tx_work_cb);
	init_waitqueue_head(&reader->tx_wq);
	init_waitqueue_head(&reader->rx_wq);
	init_waitqueue_head(&reader->rx_pending_wq);
	atomic_set(&reader->tx_inflight, 0);
	atomic_set(&reader->rx_inflight, 0);
	reader->host_idx = host_idx;
	atomic_set(&reader->refcnt, 1); /* 创建者持有的初始引用(P2-2) */
	reader->rx_thread = kthread_run(extent_reader_rx_thread_fn, reader,
					"cfs-rrx-%llu", ext_id);
	if (IS_ERR(reader->rx_thread)) {
		cfs_socket_release(reader->sock, true);
		kfree(reader);
		return NULL;
	}
	return reader;
}

/* 排空一条包链表：逐个以错误完成 handle_reply（解锁其绑定页）+ 释放。
 * 用于 reader 拆除时兜底任何未处理的包，杜绝漏解锁导致 wait_on_page_locked 死锁。 */
static void reader_drain_packet_list(struct list_head *list, spinlock_t *lock,
				     atomic_t *inflight)
{
	struct cfs_packet *packet;
	int drained = 0;

	while (true) {
		spin_lock(lock);
		packet = list_first_entry_or_null(list, struct cfs_packet, list);
		if (packet)
			list_del(&packet->list);
		spin_unlock(lock);
		if (!packet)
			break;
		packet->error = -EIO;
		if (packet->handle_reply)
			packet->handle_reply(packet);
		cfs_packet_release(packet);
		drained++;
	}
	if (drained && inflight)
		atomic_sub(drained, inflight);
}

/* 真正拆除:仅在 refcnt 归 0 时由 cfs_extent_reader_release 调用。 */
static void __cfs_extent_reader_free(struct cfs_extent_reader *reader)
{
	/* 先停 tx（不再产生新 rx_packets），再停 recv 线程。 */
	cancel_work_sync(&reader->tx_work);
	if (reader->rx_thread)
		kthread_stop(reader->rx_thread);
	/* 兜底：cancel_work_sync 可能截停 tx_work 使部分包滞留 tx_packets（从未
	 * 发送、也就永远等不到回复）；rx 线程停止时也可能有滞留的 rx_packets。
	 * 逐个以 -EIO 完成 handle_reply，保证其临时页被解锁，否则 DIO 的
	 * wait_on_page_locked 永久 D 死锁（随机小 IO reader 高频拆除时高发）。 */
	reader_drain_packet_list(&reader->tx_packets, &reader->lock_tx,
				 &reader->tx_inflight);
	reader_drain_packet_list(&reader->rx_packets, &reader->lock_rx,
				 &reader->rx_inflight);
	/* recover 指针持有其一个引用(owning),此处 put:rx 线程已 kthread_stop,
	 * 不再有人经 reader->recover 访问,安全释放。 */
	if (reader->recover)
		cfs_extent_reader_release(reader->recover);
	cfs_data_partition_release(reader->dp);
	cfs_socket_release(reader->sock, true);
	kfree(reader);
}

/* 引用计数 put(P2-2):归 0 才真正拆除。原 release 语义(拆除)迁入 __free,
 * 所有旧 release 调用点自动变为"放一个引用"。 */
void cfs_extent_reader_release(struct cfs_extent_reader *reader)
{
	if (!reader)
		return;
	if (!atomic_dec_and_test(&reader->refcnt))
		return;
	__cfs_extent_reader_free(reader);
}

void cfs_extent_reader_flush(struct cfs_extent_reader *reader)
{
	wait_event(reader->tx_wq, atomic_read(&reader->tx_inflight) == 0);
	wait_event(reader->rx_wq, atomic_read(&reader->rx_inflight) == 0);
}

void cfs_extent_reader_request(struct cfs_extent_reader *reader,
			       struct cfs_packet *packet)
{
	spin_lock(&reader->lock_tx);
	list_add_tail(&packet->list, &reader->tx_packets);
	spin_unlock(&reader->lock_tx);
	atomic_inc(&reader->tx_inflight);
	queue_work(extent_work_queue, &reader->tx_work);
}

static void extent_reader_tx_work_cb(struct work_struct *work)
{
	struct cfs_extent_reader *reader =
		container_of(work, struct cfs_extent_reader, tx_work);
	struct cfs_packet *packet;
	int cnt = 0;

	while (true) {
		spin_lock(&reader->lock_tx);
		packet = list_first_entry_or_null(&reader->tx_packets,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		spin_unlock(&reader->lock_tx);
		if (!packet)
			break;

		if (!(reader->flags &
		      (EXTENT_READER_F_ERROR | EXTENT_READER_F_RECOVER))) {
			int ret = cfs_socket_send_packet(reader->sock, packet);
			/* 必须用 READER flag 位:原来错用 WRITER_F_ERROR(0x4)rx 按
			 * READER_F_ERROR(0x2)认不出,而 WRITER_F_RECOVER(0x2)恰好撞成
			 * READER_F_ERROR → 分类错乱、无 failover 反假 EIO。 */
			if (ret == -ENOMEM)
				reader->flags |= EXTENT_READER_F_ERROR;
			else if (ret < 0)
				reader->flags |= EXTENT_READER_F_RECOVER;
		}
		spin_lock(&reader->lock_rx);
		list_add_tail(&packet->list, &reader->rx_packets);
		spin_unlock(&reader->lock_rx);
		atomic_inc(&reader->rx_inflight);
		wake_up(&reader->rx_pending_wq);
	}
	atomic_sub(cnt, &reader->tx_inflight);
	wake_up(&reader->tx_wq);
}

static int extent_reader_rx_thread_fn(void *data)
{
	struct cfs_extent_reader *reader = data;
	struct cfs_extent_stream *es = reader->es;
	struct cfs_extent_reader *recover;
	struct cfs_packet *packet;
	int cnt;
	int ret;

	while (!kthread_should_stop()) {
	wait_event(reader->rx_pending_wq,
		   !list_empty_careful(&reader->rx_packets) ||
			   kthread_should_stop());
	recover = reader->recover;
	cnt = 0;
	while (true) {
		spin_lock(&reader->lock_rx);
		packet = list_first_entry_or_null(&reader->rx_packets,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		spin_unlock(&reader->lock_rx);
		if (!packet)
			break;

		if (reader->flags & EXTENT_READER_F_ERROR) {
			packet->error = -EIO;
			goto handle_packet;
		}

		if (reader->flags & EXTENT_READER_F_RECOVER)
			goto recover_packet;

		ret = cfs_socket_recv_packet(reader->sock, packet);
		if (ret < 0 || packet->reply.hdr.result_code != CFS_STATUS_OK) {
			reader->flags |= EXTENT_READER_F_RECOVER;
			goto recover_packet;
		}
		goto handle_packet;

recover_packet:
		if (!recover) {
			mutex_lock(&es->lock_readers);
			if (es->nr_readers >= es->max_readers) {
				mutex_unlock(&es->lock_readers);
				reader->flags |= EXTENT_READER_F_ERROR;
				packet->error = -EPERM;
				goto handle_packet;
			}
			mutex_unlock(&es->lock_readers);

			cfs_data_partition_get(reader->dp);
			recover = cfs_extent_reader_new(es, reader->dp,
							reader->host_idx + 1,
							reader->ext_id);
			if (!recover) {
				cfs_data_partition_put(reader->dp);
				reader->flags |= EXTENT_READER_F_ERROR;
				packet->error = -ENOMEM;
				goto handle_packet;
			}

			mutex_lock(&es->lock_readers);
			/* recover 有两个持有者:es->readers 链表 + 本 reader->recover
			 * 指针。_new 的初始引用归 reader->recover(owning),链表另取一个
			 * (P2-2)。 */
			cfs_extent_reader_get(recover);
			list_add_tail(&recover->list, &es->readers);
			es->nr_readers++;
			mutex_unlock(&es->lock_readers);
			reader->recover = recover;
		}

		cfs_packet_set_callback(packet, packet->handle_reply, recover);
		msleep(100);
		cfs_extent_reader_request(recover, packet);
		continue;

handle_packet:
		if (packet->handle_reply)
			packet->handle_reply(packet);
		cfs_packet_release(packet);
	}
	atomic_sub(cnt, &reader->rx_inflight);
	wake_up(&reader->rx_wq);
	}
	return 0;
}
