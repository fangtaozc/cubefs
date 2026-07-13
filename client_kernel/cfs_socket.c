/*
 * Copyright 2023 The CubeFS Authors.
 */
#include <linux/tcp.h>
#include <net/sock.h>
#include <linux/version.h>
#include <linux/bvec.h>
#include "cfs_socket.h"

/* 6.4 起 iov_iter 的 iov 成员改名 __iov，提供 iter_iov() 访问器。 */
#if LINUX_VERSION_CODE < KERNEL_VERSION(6, 4, 0)
#define iter_iov(iter) ((iter)->iov)
#endif

/* kernel_sendpage 于 6.5 移除，改用 MSG_SPLICE_PAGES + sock_sendmsg。 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 5, 0)
static inline int cfs_kernel_sendpage(struct socket *sock, struct page *page,
				      int offset, size_t size, int flags)
{
	struct bio_vec bvec;
	struct msghdr msg = { .msg_flags = flags | MSG_SPLICE_PAGES };

	bvec_set_page(&bvec, page, size, offset);
	iov_iter_bvec(&msg.msg_iter, ITER_SOURCE, &bvec, 1, size);
	return sock_sendmsg(sock, &msg);
}
#else
#define cfs_kernel_sendpage(sock, page, offset, size, flags) \
	kernel_sendpage((sock), (page), (offset), (size), (flags))
#endif

#define SOCK_POOL_BUCKET_COUNT 128
#define SOCK_POOL_LRU_INTERVAL_MS 60 * 1000u
// #define DEBUG

struct cfs_socket_pool {
	struct hlist_head head[SOCK_POOL_BUCKET_COUNT];
	struct list_head lru;
	struct mutex lock;
	struct delayed_work work;
};

static struct cfs_socket_pool *sock_pool;

static inline u32 hash_sockaddr_storage(const struct sockaddr_storage *addr)
{
	const struct sockaddr_in *in;

	switch (addr->ss_family) {
	case AF_INET:
		in = (const struct sockaddr_in *)addr;
		return in->sin_addr.s_addr | in->sin_port;
	default:
		return 0;
	}
}

int cfs_socket_create(enum cfs_socket_type type,
		      const struct sockaddr_storage *ss, struct cfs_log *log,
		      struct cfs_socket **cskp)
{
	struct cfs_socket *csk;
	u32 key;
	int ret;

	BUG_ON(sock_pool == NULL);

	key = hash_sockaddr_storage(ss);
	mutex_lock(&sock_pool->lock);
	hash_for_each_possible(sock_pool->head, csk, hash, key) {
		if (cfs_addr_cmp(&csk->ss_dst, ss) == 0)
			break;
	}

	if (!csk) {
		mutex_unlock(&sock_pool->lock);

		csk = kzalloc(sizeof(*csk), GFP_NOFS);
		if (!csk)
			return -ENOMEM;

		memcpy(&csk->ss_dst, ss, sizeof(*ss));
#ifdef KERNEL_HAS_SOCK_CREATE_KERN_WITH_NET
		ret = sock_create_kern(&init_net, AF_INET, SOCK_STREAM,
				       IPPROTO_TCP, &csk->sock);
#else
		ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP,
				       &csk->sock);
#endif
		if (ret < 0) {
			kfree(csk);
			return ret;
		}
		csk->sock->sk->sk_allocation = GFP_NOFS;

		ret = kernel_connect(csk->sock, (struct sockaddr *)&csk->ss_dst,
				     sizeof(csk->ss_dst), 0 /*O_NONBLOCK*/);
		if (ret < 0 && ret != -EINPROGRESS) {
			sock_release(csk->sock);
			kfree(csk);
			return ret;
		}

		csk->tx_buffer = cfs_buffer_new(0);
		csk->rx_buffer = cfs_buffer_new(0);
		if (!csk->tx_buffer || !csk->rx_buffer) {
			cfs_buffer_release(csk->tx_buffer);
			cfs_buffer_release(csk->rx_buffer);
			sock_release(csk->sock);
			kfree(csk);
			return -ENOMEM;
		}

		tcp_sock_set_nodelay(csk->sock->sk);

		sock_set_reuseaddr(csk->sock->sk);
		csk->pool = sock_pool;
	} else {
		hash_del(&csk->hash);
		list_del(&csk->list);
		mutex_unlock(&sock_pool->lock);
	}
	csk->log = log;
	*cskp = csk;

	return 0;
}

void cfs_socket_release(struct cfs_socket *csk, bool forever)
{
	if (!csk)
		return;
	if (forever) {
		if (csk->sock)
			sock_release(csk->sock);
		cfs_buffer_release(csk->tx_buffer);
		cfs_buffer_release(csk->rx_buffer);
		kfree(csk);
	} else {
		u32 key = hash_sockaddr_storage(&csk->ss_dst);
		mutex_lock(&sock_pool->lock);
		hash_add(sock_pool->head, &csk->hash, key);
		list_add_tail(&csk->list, &sock_pool->lru);
		csk->jiffies = jiffies;
		mutex_unlock(&sock_pool->lock);
	}
}

// void cfs_socket_set_callback(struct cfs_socket *csk,
// 			     const struct cfs_socket_ops *ops, void *private)
// {
// 	csk->sock->sk->sk_user_data = private;
// 	csk->sock->sk->sk_data_ready = ops->sk_data_ready;
// 	csk->sock->sk->sk_write_space = ops->sk_write_space;
// 	csk->sock->sk->sk_state_change = ops->sk_state_change;
// }

int cfs_socket_set_recv_timeout(struct cfs_socket *csk, u32 timeout_ms)
{
	csk->sock->sk->sk_rcvtimeo = msecs_to_jiffies(timeout_ms);
	/* 一并设发送超时:原来只设 sk_rcvtimeo,sk_sndtimeo 默认无限;对接受连接
	 * 但停止读取(接收窗满)的黑洞/卡死 peer,kernel_sendmsg 会在 sk_stream_
	 * wait_memory 永久阻塞、进程长期 D 态占住挂载点。与既有 5s 收超时对称,
	 * 把发送 hang 也界定为超时→上层走 recover 换副本。 */
	csk->sock->sk->sk_sndtimeo = msecs_to_jiffies(timeout_ms);
	return 0;
}

int cfs_socket_send(struct cfs_socket *csk, void *data, size_t len)
{
	struct iovec iov = {
		.iov_base = data,
		.iov_len = len,
	};

	return cfs_socket_send_iovec(csk, &iov, 1);
}

int cfs_socket_recv(struct cfs_socket *csk, void *data, size_t len)
{
	struct iovec iov = {
		.iov_base = data,
		.iov_len = len,
	};

	return cfs_socket_recv_iovec(csk, &iov, 1);
}

int cfs_socket_send_iovec(struct cfs_socket *csk, struct iovec *iov,
			  size_t nr_segs)
{
	struct iov_iter ii;
	size_t len = iov_length(iov, nr_segs);
	int ret = 0;
	sigset_t blocked, oldset;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	siginitsetinv(&blocked, sigmask(SIGKILL));
	sigprocmask(SIG_SETMASK, &blocked, &oldset);
#ifdef KERNEL_HAS_IOV_ITER_WITH_TAG
	iov_iter_init(&ii, WRITE, iov, nr_segs, len);
#else
	iov_iter_init(&ii, iov, nr_segs, len, 0);
#endif
	while (iov_iter_count(&ii) > 0) {
		struct msghdr msghdr = {
			.msg_flags = MSG_NOSIGNAL,
		};

		ret = kernel_sendmsg(csk->sock, &msghdr,
				     (struct kvec *)iter_iov(&ii), ii.nr_segs,
				     iov_iter_count(&ii));
		if (ret < 0)
			break;
		if (ret == 0) {
			/* peer 半关闭:advance(0) 会导致 count 不减而无限自旋,按错误处理 */
			ret = -EPIPE;
			break;
		}
		iov_iter_advance(&ii, ret);
	}
	sigprocmask(SIG_SETMASK, &oldset, NULL);
	return ret < 0 ? ret : (int)len;
}

int cfs_socket_recv_iovec(struct cfs_socket *csk, struct iovec *iov,
			  size_t nr_segs)
{
	struct msghdr msghdr = {
		.msg_flags = MSG_WAITALL | MSG_NOSIGNAL,
	};
	size_t len = iov_length(iov, nr_segs);
	int ret;
	sigset_t blocked, oldset;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	siginitsetinv(&blocked, sigmask(SIGKILL));
	sigprocmask(SIG_SETMASK, &blocked, &oldset);
	ret = kernel_recvmsg(csk->sock, &msghdr, (struct kvec *)iov, nr_segs,
			     len, msghdr.msg_flags);
	sigprocmask(SIG_SETMASK, &oldset, NULL);
	/*
	 * 注意:此处不能判短读(ret!=len)。本函数是通用 recv,master HTTP 通信(变长响应)
	 * 依赖部分读返回。短读判错只能在 packet 协议路径(cfs_socket_recv_packet /
	 * cfs_socket_recv_pages,固定长度)单独做,否则会误杀 HTTP 导致 mount 失败。
	 */
	return ret;
}

static int cfs_socket_send_pages(struct cfs_socket *csk,
				 struct cfs_page_frag *frags, size_t nr)
{
	size_t i;
	sigset_t blocked, oldset;
	int ret = 0;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	siginitsetinv(&blocked, sigmask(SIGKILL));
	sigprocmask(SIG_SETMASK, &blocked, &oldset);
	for (i = 0; i < nr; i++) {
		ret = cfs_kernel_sendpage(csk->sock, frags[i].page->page,
					  frags[i].offset, frags[i].size,
					  MSG_NOSIGNAL);
		if (ret < 0)
			break;
	}
	sigprocmask(SIG_SETMASK, &oldset, NULL);
	return ret;
}

static int cfs_socket_recv_pages(struct cfs_socket *csk,
				 struct cfs_page_frag *frags, size_t nr)
{
	size_t i;
	sigset_t blocked, oldset;
	int ret = 0;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	siginitsetinv(&blocked, sigmask(SIGKILL));
	sigprocmask(SIG_SETMASK, &blocked, &oldset);
	for (i = 0; i < nr; i++) {
		struct kvec vec;
		struct msghdr msghdr = {
			.msg_flags = MSG_WAITALL | MSG_NOSIGNAL,
		};

		vec.iov_base = kmap(frags[i].page->page) + frags[i].offset;
		vec.iov_len = frags[i].size;
		ret = kernel_recvmsg(csk->sock, &msghdr, &vec, 1, vec.iov_len,
				     msghdr.msg_flags);
		kunmap(frags[i].page->page);
		/* 短读(RCVTIMEO 超时部分读):数据页必须收满,否则返回错位/不完整数据 */
		if (ret >= 0 && ret != (int)vec.iov_len)
			ret = -EIO;
		if (ret < 0)
			break;
	}
	sigprocmask(SIG_SETMASK, &oldset, NULL);
	return ret;
}

int cfs_socket_send_packet(struct cfs_socket *csk, struct cfs_packet *packet)
{
	int ret = 0;

	cfs_buffer_reset(csk->tx_buffer);
	switch (packet->request.hdr.opcode) {
	case CFS_OP_EXTENT_CREATE:
	case CFS_OP_STREAM_WRITE:
	case CFS_OP_STREAM_RANDOM_WRITE:
	case CFS_OP_STREAM_READ:
	case CFS_OP_STREAM_FOLLOWER_READ:
		break;
	default:
		ret = cfs_packet_request_data_to_json(packet, csk->tx_buffer);
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, invalid request data %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, ret);
			return ret;
		}
		packet->request.hdr.size =
			cpu_to_be32(cfs_buffer_size(csk->tx_buffer));
	}

#ifdef DEBUG
	cfs_pr_debug(
		"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, ext_offset=%llu, "
		"kernel_offset=%llu, arglen=%u, datalen=%u, data=%.*s\n",
		csk->sock, be64_to_cpu(packet->request.hdr.req_id),
		packet->request.hdr.opcode,
		be64_to_cpu(packet->request.hdr.pid),
		be64_to_cpu(packet->request.hdr.ext_id),
		be64_to_cpu(packet->request.hdr.ext_offset),
		be64_to_cpu(packet->request.hdr.kernel_offset),
		be32_to_cpu(packet->request.hdr.arglen),
		be32_to_cpu(packet->request.hdr.size),
		(int)cfs_buffer_size(csk->tx_buffer),
		cfs_buffer_data(csk->tx_buffer));
#endif

	/* v3.5: 写类 op 带 ProtoVersion 标志,避免 forbidWriteOpOfProtoVer0 拒绝 */
	switch (packet->request.hdr.opcode) {
	case CFS_OP_EXTENT_CREATE:
	case CFS_OP_STREAM_WRITE:
	case CFS_OP_STREAM_RANDOM_WRITE:
	case CFS_OP_TRUNCATE:
		packet->request.hdr.ext_type |= 0x10;
		break;
	}
	/* send hdr */
	ret = cfs_socket_send(csk, &packet->request.hdr,
			      sizeof(packet->request.hdr));
	if (ret < 0) {
		cfs_log_error(csk->log,
			      "so(%p) id=%llu, op=0x%x, send header error %d\n",
			      csk->sock,
			      be64_to_cpu(packet->request.hdr.req_id),
			      packet->request.hdr.opcode, ret);
		return ret;
	}
	if (packet->request.hdr.ext_type & 0x10) {
		u8 _pv[12] = { 0 };
		_pv[11] = 1; /* VerSeq=0(8B) + ProtoVersion=1(4B BE) */
		ret = cfs_socket_send(csk, _pv, sizeof(_pv));
		if (ret < 0)
			return ret;
	}

	/* send arg */
	if (packet->request.arg) {
		ret = cfs_socket_send(csk, cfs_buffer_data(packet->request.arg),
				      cfs_buffer_size(packet->request.arg));
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, send arg error %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, ret);
			return ret;
		}
	}

	/* send data */
	switch (packet->request.hdr.opcode) {
	case CFS_OP_EXTENT_CREATE:
		ret = cfs_socket_send(csk, &packet->request.data.ino,
				      sizeof(packet->request.data.ino));
		break;
	case CFS_OP_STREAM_WRITE:
	case CFS_OP_STREAM_RANDOM_WRITE:
		ret = cfs_socket_send_pages(csk,
					    packet->request.data.write.frags,
					    packet->request.data.write.nr);
		break;
	case CFS_OP_STREAM_READ:
	case CFS_OP_STREAM_FOLLOWER_READ:
		break;
	default:
		if (cfs_buffer_size(csk->tx_buffer) > 0)
			ret = cfs_socket_send(csk,
					      cfs_buffer_data(csk->tx_buffer),
					      cfs_buffer_size(csk->tx_buffer));
		break;
	}
	if (ret < 0)
		cfs_log_error(csk->log,
			      "so(%p) id=%llu, op=0x%x, send data error %d\n",
			      csk->sock,
			      be64_to_cpu(packet->request.hdr.req_id),
			      packet->request.hdr.opcode, ret);
	return ret < 0 ? ret : 0;
}

int cfs_socket_recv_packet(struct cfs_socket *csk, struct cfs_packet *packet)
{
	int ret;
	u32 arglen, datalen;

	/**
	 * packet header
	 */
	ret = cfs_socket_recv(csk, &packet->reply.hdr,
			      sizeof(packet->reply.hdr));
	/* 短读:packet 头必须完整,否则 arglen/datalen 解析为脏值 */
	if (ret >= 0 && ret != (int)sizeof(packet->reply.hdr))
		ret = -EIO;
	if (ret < 0) {
		cfs_log_error(csk->log,
			      "so(%p) id=%llu, op=0x%x, recv header error %d\n",
			      csk->sock,
			      be64_to_cpu(packet->request.hdr.req_id),
			      packet->request.hdr.opcode, ret);
		return ret;
	}

	/* 校验 reply 头,防连接错位/串包/类型混淆:magic 错,或 req_id/opcode 与本请求
	 * 不匹配,说明服务端回了别的包(或流已错位)。若不校验就按不可信 reply.opcode 分支,
	 * 会用错联合体成员(如非 READ 包走 read.frags)、按不可信 arglen/datalen 分配/收包
	 * → 越界/巨额分配/把一个 inode 的数据串给另一个请求。此时返回 -EBADMSG,调用方错误
	 * 路径会丢弃该连接(不回池),避免残留字节污染后续请求。 */
	if (packet->reply.hdr.magic != CFS_PACKET_MAGIC ||
	    packet->reply.hdr.req_id != packet->request.hdr.req_id ||
	    packet->reply.hdr.opcode != packet->request.hdr.opcode) {
		cfs_log_error(
			csk->log,
			"so(%p) reply mismatch: magic=0x%x req_id(reply=%llu req=%llu) op(reply=0x%x req=0x%x)\n",
			csk->sock, packet->reply.hdr.magic,
			be64_to_cpu(packet->reply.hdr.req_id),
			be64_to_cpu(packet->request.hdr.req_id),
			packet->reply.hdr.opcode, packet->request.hdr.opcode);
		return -EBADMSG;
	}

	/* v3.5: 请求带 ProtoVersion 标志时 reply 也带,读掉 VerSeq(8)+ProtoVersion(4) 对齐 */
	if (packet->request.hdr.ext_type & 0x10) {
		u8 _pv[12];
		ret = cfs_socket_recv(csk, _pv, sizeof(_pv));
		if (ret >= 0 && ret != (int)sizeof(_pv))
			ret = -EIO;
		if (ret < 0)
			return ret;
	}

	arglen = be32_to_cpu(packet->reply.hdr.arglen);
	datalen = be32_to_cpu(packet->reply.hdr.size);

	/**
	 * packet arg
	 */
	if (arglen > 0) {
		if (packet->reply.arg) {
			ret = cfs_buffer_resize(packet->reply.arg, arglen);
		} else if (!(packet->reply.arg = cfs_buffer_new(arglen))) {
			ret = -ENOMEM;
		}

		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, alloc reply arg oom\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode);
			return ret;
		}
		ret = cfs_socket_recv(csk, cfs_buffer_data(packet->reply.arg),
				      arglen);
		/* 短读:arg 必须收满 arglen,否则数据不完整 */
		if (ret >= 0 && ret != (int)arglen)
			ret = -EIO;
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, recv arg(%u) error %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, arglen, ret);
			return ret;
		}
		cfs_buffer_seek(packet->reply.arg, arglen);
	}

	/**
	 * packet data
	 */
	if (datalen > 0 && packet->reply.hdr.result_code == CFS_STATUS_OK &&
	    (packet->reply.hdr.opcode == CFS_OP_STREAM_READ ||
	     packet->reply.hdr.opcode == CFS_OP_STREAM_FOLLOWER_READ)) {
#ifdef DEBUG
		cfs_pr_debug(
			"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, rc=0x%x, arglen=%u, datalen=%u\n",
			csk->sock, be64_to_cpu(packet->reply.hdr.req_id),
			packet->reply.hdr.opcode,
			be64_to_cpu(packet->reply.hdr.pid),
			be64_to_cpu(packet->reply.hdr.ext_id),
			packet->reply.hdr.result_code, arglen, datalen);
#endif
		/**
		 *  reply read extent message
		 */
		ret = cfs_socket_recv_pages(csk, packet->reply.data.read.frags,
					    packet->reply.data.read.nr);
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, recv data(%u) error %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, datalen, ret);
			return ret;
		}
		/* 读数据 CRC 校验(H6):datanode 每个读回复包头带该数据块的
		 * crc32(store.Read 返回、reply.SetCRC),客户端按收到的 frag 重算比对。
		 * datanode 盘静默损坏/内存翻转/串包会返 OK+错数据,不校验则静默返脏数据。
		 * 失配返 -EBADMSG,rx 线程据此走 recover 换副本。写路径用同一
		 * cfs_page_frags_crc32 且被 datanode 接受,证算法一致;普通 extent 冷读
		 * (含各尺寸)已验零误报。 */
		{
			u32 expect = be32_to_cpu(packet->reply.hdr.crc);
			u32 actual = cfs_page_frags_crc32(
				packet->reply.data.read.frags,
				packet->reply.data.read.nr);
			if (actual != expect) {
				cfs_log_error(
					csk->log,
					"so(%p) id=%llu READ crc mismatch expect=0x%x actual=0x%x datalen=%u\n",
					csk->sock,
					be64_to_cpu(packet->request.hdr.req_id),
					expect, actual, datalen);
				return -EBADMSG;
			}
		}
	} else if (datalen > 0) {
		/**
		 *  reply other message
		 */
		cfs_buffer_reset(csk->rx_buffer);
		if (datalen > cfs_buffer_capacity(csk->rx_buffer)) {
			size_t grow_len =
				datalen - cfs_buffer_capacity(csk->rx_buffer);
			ret = cfs_buffer_grow(csk->rx_buffer, grow_len);
			if (ret < 0) {
				cfs_log_error(
					csk->log,
					"so(%p) id=%llu, op=0x%x, recv data oom\n",
					csk->sock,
					be64_to_cpu(packet->request.hdr.req_id),
					packet->request.hdr.opcode);
				return ret;
			}
		}

		ret = cfs_socket_recv(csk, cfs_buffer_data(csk->rx_buffer),
				      datalen);
		/* 短读:data 必须收满 datalen,否则数据不完整 */
		if (ret >= 0 && ret != (int)datalen)
			ret = -EIO;
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, tcp recv data error %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, ret);
			return ret;
		}
		cfs_buffer_seek(csk->rx_buffer, datalen);

		if (packet->reply.hdr.result_code == CFS_STATUS_OK) {
			struct cfs_json *json;
#ifdef DEBUG
			cfs_pr_debug(
				"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, rc=0x%x, arglen=%u, datalen=%u, data=%.*s\n",
				csk->sock,
				be64_to_cpu(packet->reply.hdr.req_id),
				packet->reply.hdr.opcode,
				be64_to_cpu(packet->reply.hdr.pid),
				be64_to_cpu(packet->reply.hdr.ext_id),
				packet->reply.hdr.result_code, arglen, datalen,
				(int)cfs_buffer_size(csk->rx_buffer),
				cfs_buffer_data(csk->rx_buffer));
#endif
			/**
			 *  reply ok message
			 */
			json = cfs_json_parse(cfs_buffer_data(csk->rx_buffer),
					      cfs_buffer_size(csk->rx_buffer));
			if (!json) {
				cfs_log_error(
					csk->log,
					"so(%p) id=%llu, op=0x%x, invliad json\n",
					csk->sock,
					be64_to_cpu(packet->request.hdr.req_id),
					packet->request.hdr.opcode);
				return -EBADMSG;
			}

			ret = cfs_packet_reply_data_from_json(json, packet);
			if (ret < 0) {
				cfs_log_error(
					csk->log,
					"so(%p) id=%llu, op=0x%x, parse json error %d\n",
					csk->sock,
					be64_to_cpu(packet->request.hdr.req_id),
					packet->request.hdr.opcode, ret);
				ret = -EBADMSG;
			}
			cfs_json_release(json);
			if (ret < 0)
				return ret;
		} else {
			/**
			 *  reply error message
			 */
			cfs_log_warn(
				csk->log,
				"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, rc=0x%x, from=%s, data=%.*s\n",
				csk->sock,
				be64_to_cpu(packet->reply.hdr.req_id),
				packet->reply.hdr.opcode,
				be64_to_cpu(packet->reply.hdr.pid),
				be64_to_cpu(packet->reply.hdr.ext_id),
				packet->reply.hdr.result_code,
				cfs_pr_addr(&csk->ss_dst),
				(int)cfs_buffer_size(csk->rx_buffer),
				cfs_buffer_data(csk->rx_buffer));
		}
	} else {
#ifdef DEBUG
		cfs_pr_debug(
			"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, rc=0x%x, arglen=%u, datalen=%u\n",
			csk->sock, be64_to_cpu(packet->reply.hdr.req_id),
			packet->reply.hdr.opcode,
			be64_to_cpu(packet->reply.hdr.pid),
			be64_to_cpu(packet->reply.hdr.ext_id),
			packet->reply.hdr.result_code, arglen, datalen);
#endif
	}

	return ret < 0 ? ret : 0;
}

static inline bool is_sock_valid(struct cfs_socket *sock)
{
	return sock->jiffies + msecs_to_jiffies(SOCK_POOL_LRU_INTERVAL_MS) >
	       jiffies;
}

static void socket_pool_lru_work_cb(struct work_struct *work)
{
	struct delayed_work *delayed_work = to_delayed_work(work);
	struct cfs_socket *sock;
	struct cfs_socket *tmp;

	schedule_delayed_work(delayed_work,
			      msecs_to_jiffies(SOCK_POOL_LRU_INTERVAL_MS));
	mutex_lock(&sock_pool->lock);
	list_for_each_entry_safe(sock, tmp, &sock_pool->lru, list) {
		if (is_sock_valid(sock))
			break;
		hash_del(&sock->hash);
		list_del(&sock->list);
		cfs_socket_release(sock, true);
	}
	mutex_unlock(&sock_pool->lock);
}

int cfs_socket_module_init(void)
{
	if (sock_pool)
		return 0;
	sock_pool = kzalloc(sizeof(*sock_pool), GFP_KERNEL);
	if (!sock_pool)
		return -ENOMEM;
	hash_init(sock_pool->head);
	INIT_LIST_HEAD(&sock_pool->lru);
	mutex_init(&sock_pool->lock);
	INIT_DELAYED_WORK(&sock_pool->work, socket_pool_lru_work_cb);
	schedule_delayed_work(&sock_pool->work,
			      msecs_to_jiffies(SOCK_POOL_LRU_INTERVAL_MS));
	return 0;
}

void cfs_socket_module_exit(void)
{
	struct cfs_socket *sock;
	struct hlist_node *tmp;
	int i;

	if (!sock_pool)
		return;
	cancel_delayed_work_sync(&sock_pool->work);
	hash_for_each_safe(sock_pool->head, i, tmp, sock, hash) {
		hash_del(&sock->hash);
		cfs_socket_release(sock, true);
	}
	mutex_destroy(&sock_pool->lock);
	kfree(sock_pool);
	sock_pool = NULL;
}
