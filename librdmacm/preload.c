/*
 * Copyright (c) 2011-2012 Intel Corporation.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
#define _GNU_SOURCE
#include <config.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <stdarg.h>
#include <dlfcn.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <semaphore.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>

#include <sys/uio.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <rdma/rsocket.h>
#include "cma.h"
#include "indexer.h"

#define ETRYRDMA 999

struct socket_calls {
	int (*socket)(int domain, int type, int protocol);
	int (*bind)(int socket, const struct sockaddr *addr, socklen_t addrlen);
	int (*listen)(int socket, int backlog);
	int (*accept)(int socket, struct sockaddr *addr, socklen_t *addrlen);
	int (*connect)(int socket, const struct sockaddr *addr, socklen_t addrlen);
	ssize_t (*recv)(int socket, void *buf, size_t len, int flags);
	ssize_t (*recvfrom)(int socket, void *buf, size_t len, int flags,
			    struct sockaddr *src_addr, socklen_t *addrlen);
	ssize_t (*recvmsg)(int socket, struct msghdr *msg, int flags);
	ssize_t (*read)(int socket, void *buf, size_t count);
	ssize_t (*readv)(int socket, const struct iovec *iov, int iovcnt);
	ssize_t (*send)(int socket, const void *buf, size_t len, int flags);
	ssize_t (*sendto)(int socket, const void *buf, size_t len, int flags,
			  const struct sockaddr *dest_addr, socklen_t addrlen);
	ssize_t (*sendmsg)(int socket, const struct msghdr *msg, int flags);
	ssize_t (*write)(int socket, const void *buf, size_t count);
	ssize_t (*writev)(int socket, const struct iovec *iov, int iovcnt);
	int (*poll)(struct pollfd *fds, nfds_t nfds, int timeout);
	int (*shutdown)(int socket, int how);
	int (*close)(int socket);
	int (*getpeername)(int socket, struct sockaddr *addr, socklen_t *addrlen);
	int (*getsockname)(int socket, struct sockaddr *addr, socklen_t *addrlen);
	int (*setsockopt)(int socket, int level, int optname,
			  const void *optval, socklen_t optlen);
	int (*getsockopt)(int socket, int level, int optname,
			  void *optval, socklen_t *optlen);
	int (*fcntl)(int socket, int cmd, ... /* arg */);
	int (*dup2)(int oldfd, int newfd);
	ssize_t (*sendfile)(int out_fd, int in_fd, off_t *offset, size_t count);
	int (*fxstat)(int ver, int fd, struct stat *buf);
};

static struct socket_calls real;
static struct socket_calls rs;

static struct index_map idm;
static pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;

static int sq_size;
static int rq_size;
static int sq_inline;
static int fork_support;

enum fd_type {
	fd_normal,
	fd_rsocket
};

enum fd_fork_state {
	fd_ready,
	fd_tiaccoon,
	fd_fork,
	fd_fork_listen,
	fd_fork_active,
	fd_fork_passive
};

struct fd_info {
	enum fd_type type;
	enum fd_fork_state state;
	int fd;
	int dupfd;
	int realfd;
	struct sockaddr *vlocaladdr;
	socklen_t *vlocaladdrlen;
	_Atomic(int) refcnt;
};

struct config_entry {
	char *name;
	int domain;
	int type;
	int protocol;
};

// static char *sockaddr2char(const struct sockaddr *addr) {
// 	static char result[NI_MAXHOST + NI_MAXSERV + 32];
// 	char host[NI_MAXHOST], service[NI_MAXSERV];
// 	int ret;

// 	if (!addr) {
// 		snprintf(result, sizeof(result), "Address is NULL");
// 		return result;
// 	}

// 	switch (addr->sa_family) {
// 		case AF_UNIX:
// 			snprintf(result, sizeof(result), "Unix Domain Socket: %d: %s", addr->sa_family, addr->sa_data);
// 			return result;
// 			break;
// 	}

// 	ret = getnameinfo(addr, sizeof(*addr), host, sizeof(host), service, sizeof(service), NI_NUMERICHOST | NI_NUMERICSERV);
// 	if (ret != 0) {
// 		snprintf(result, sizeof(result), "getnameinfo: %s: %d: %s", gai_strerror(ret), addr->sa_family, addr->sa_data);
// 		return result;
// 	}

// 	switch (addr->sa_family) {
// 		case AF_INET:
// 			snprintf(result, sizeof(result), "IPv4 Address: %d: %s, Port: %s", addr->sa_family, host, service);
// 			break;
// 		case AF_INET6:
// 			snprintf(result, sizeof(result), "IPv6 Address: %d: %s, Port: %s", addr->sa_family, host, service);
// 			break;
// 		case AF_IB:
// 			snprintf(result, sizeof(result), "InfiniBand Address: %d: %s, Port: %s", addr->sa_family, host, service);
// 			break;
// 		default:
// 			snprintf(result, sizeof(result), "Unknown Address Family: %d: %s", addr->sa_family, host);
// 			break;
// 	}

// 	return result;
// }

// static char* byte2char(const char *buf, size_t len) {
//     static char result[1024];
//     char *ptr = result;
//     size_t i;

//     for (i = 0; i < len; i++) {
//         ptr += snprintf(ptr, sizeof(result) - (ptr - result), "%.2x ", (unsigned char)buf[i]);
//         if (ptr - result >= sizeof(result)) {
//             break;
//         }
//     }

//     // Remove the trailing space
//     if (ptr != result && *(ptr - 1) == ' ') {
//         *(ptr - 1) = '\0';
//     } else {
//         *ptr = '\0';
//     }

//     return result;
// }


static struct config_entry *config;
static int config_cnt;

static void free_config(void)
{
	while (config_cnt)
		free(config[--config_cnt].name);

	free(config);
}

/*
 * Config file format:
 * # Starting '#' indicates comment
 * # wild card values are supported using '*'
 * # domain - *, INET, INET6, IB
 * # type - *, STREAM, DGRAM
 * # protocol - *, TCP, UDP
 * program_name domain type protocol
 */
static void scan_config(void)
{
	struct config_entry *new_config;
	FILE *fp;
	char line[120], prog[64], dom[16], type[16], proto[16];

	fp = fopen(RS_CONF_DIR "/preload_config", "r");
	if (!fp)
		return;

	while (fgets(line, sizeof(line), fp)) {
		if (line[0] == '#')
			continue;

		if (sscanf(line, "%63s%15s%15s%15s", prog, dom, type, proto) != 4)
			continue;

		new_config = realloc(config, (config_cnt + 1) *
					     sizeof(struct config_entry));
		if (!new_config)
			break;

		config = new_config;
		memset(&config[config_cnt], 0, sizeof(struct config_entry));

		if (!strcasecmp(dom, "INET") ||
		    !strcasecmp(dom, "AF_INET") ||
		    !strcasecmp(dom, "PF_INET")) {
			config[config_cnt].domain = AF_INET;
		} else if (!strcasecmp(dom, "INET6") ||
			   !strcasecmp(dom, "AF_INET6") ||
			   !strcasecmp(dom, "PF_INET6")) {
			config[config_cnt].domain = AF_INET6;
		} else if (!strcasecmp(dom, "IB") ||
			   !strcasecmp(dom, "AF_IB") ||
			   !strcasecmp(dom, "PF_IB")) {
			config[config_cnt].domain = AF_IB;
		} else if (strcmp(dom, "*")) {
			continue;
		}

		if (!strcasecmp(type, "STREAM") ||
		    !strcasecmp(type, "SOCK_STREAM")) {
			config[config_cnt].type = SOCK_STREAM;
		} else if (!strcasecmp(type, "DGRAM") ||
			   !strcasecmp(type, "SOCK_DGRAM")) {
			config[config_cnt].type = SOCK_DGRAM;
		} else if (strcmp(type, "*")) {
			continue;
		}

		if (!strcasecmp(proto, "TCP") ||
		    !strcasecmp(proto, "IPPROTO_TCP")) {
			config[config_cnt].protocol = IPPROTO_TCP;
		} else if (!strcasecmp(proto, "UDP") ||
			   !strcasecmp(proto, "IPPROTO_UDP")) {
			config[config_cnt].protocol = IPPROTO_UDP;
		} else if (strcmp(proto, "*")) {
			continue;
		}

		if (strcmp(prog, "*")) {
		    if (!(config[config_cnt].name = strdup(prog)))
			    continue;
		}

		config_cnt++;
	}

	fclose(fp);
	if (config_cnt)
		atexit(free_config);
}

static int intercept_socket(int domain, int type, int protocol)
{
	int i;

	if (!config_cnt)
		return 1;

	if (!protocol) {
		if (type == SOCK_STREAM)
			protocol = IPPROTO_TCP;
		else if (type == SOCK_DGRAM)
			protocol = IPPROTO_UDP;
	}

	for (i = 0; i < config_cnt; i++) {
		if ((!config[i].name ||
		     !strncasecmp(config[i].name, program_invocation_short_name,
				  strlen(config[i].name))) &&
		    (!config[i].domain || config[i].domain == domain) &&
		    (!config[i].type || config[i].type == type) &&
		    (!config[i].protocol || config[i].protocol == protocol))
			return 1;
	}

	return 0;
}

static int fd_open(void)
{
	struct fd_info *fdi;
	int ret, index;

	fdi = calloc(1, sizeof(*fdi));
	if (!fdi)
		return ERR(ENOMEM);

	index = open("/dev/null", O_RDONLY);
	if (index < 0) {
		ret = index;
		goto err1;
	}

	fdi->dupfd = -1;
	fdi->realfd = -1;
	// fprintf(stdout, "fd_open: %d %d\n", index, fdi->realfd);
	atomic_store(&fdi->refcnt, 1);
	pthread_mutex_lock(&mut);
	ret = idm_set(&idm, index, fdi);
	pthread_mutex_unlock(&mut);
	if (ret < 0)
		goto err2;

	return index;

err2:
	real.close(index);
err1:
	free(fdi);
	return ret;
}

static void fd_store(int index, int fd, enum fd_type type, enum fd_fork_state state)
{
	struct fd_info *fdi;

	fdi = idm_at(&idm, index);
	fdi->fd = fd;
	fdi->type = type;
	fdi->state = state;
}

static void fd_store_vlocaladdr(int index, const struct sockaddr *addr, socklen_t *addrlen)
{
	struct fd_info *fdi;
	// char *addr_str, *addr_raw;

	// fprintf(stdout, "fd_store_vlocaladdr: fd_store_vlocaladdr :%d\n", index);

	fdi = idm_at(&idm, index);

	if (!fdi->vlocaladdr) {
		fdi->vlocaladdr = malloc(sizeof(struct sockaddr));
	}
	if (!fdi->vlocaladdrlen) {
		fdi->vlocaladdrlen = malloc(sizeof(socklen_t));
	}

	memcpy(fdi->vlocaladdr, addr, *addrlen);
	memcpy(fdi->vlocaladdrlen, addrlen, sizeof(socklen_t));

	// addr_str = sockaddr2char(fdi->vlocaladdr);
	// addr_raw = byte2char(fdi->vlocaladdr->sa_data, *fdi->vlocaladdrlen);
	// fprintf(stdout, "fd_store_vlocaladdr: fd_store_vlocaladdr: %d addr %s raw_addr %s addrlen %d\n", index, addr_str, addr_raw, *fdi->vlocaladdrlen);
}

static inline enum fd_type fd_get(int index, int *fd)
{
	struct fd_info *fdi;

	fdi = idm_lookup(&idm, index);
	if (fdi) {
		*fd = fdi->fd;
		return fdi->type;

	} else {
		*fd = index;
		return fd_normal;
	}
}

static inline int fd_getd(int index)
{
	struct fd_info *fdi;

	fdi = idm_lookup(&idm, index);
	return fdi ? fdi->fd : index;
}

static inline enum fd_fork_state fd_gets(int index)
{
	struct fd_info *fdi;

	fdi = idm_lookup(&idm, index);
	return fdi ? fdi->state : fd_ready;
}

static inline enum fd_type fd_gett(int index)
{
	struct fd_info *fdi;

	fdi = idm_lookup(&idm, index);
	return fdi ? fdi->type : fd_normal;
}

static inline int fd_getvlocaladdr(int index, struct sockaddr *addr, socklen_t *addrlen)
{
	struct fd_info *fdi;
	// char *addr_str, *addr_raw;
	// fprintf(stdout, "fd_getvlocaladdr: fd_getvlocaladdr: %d\n", index);

	fdi = idm_lookup(&idm, index);

	if (!fdi) {
		// fprintf(stdout, "fd_getvlocaladdr: not found fdi: %d\n", index);
		return 1;
	}
	// fprintf(stdout, "fd_getvlocaladdr: found fdi: %d\n", index);

	if (fdi->vlocaladdr && fdi->vlocaladdrlen) {
		memcpy(addr, fdi->vlocaladdr, *fdi->vlocaladdrlen);
		memcpy(addrlen, fdi->vlocaladdrlen, sizeof(socklen_t));
		// addr_str = sockaddr2char(addr);
		// addr_raw = byte2char(addr->sa_data, *addrlen);
		// fprintf(stdout, "fd_getvlocaladdr: fd_getvlocaladdr: %d addr %s raw_addr %s addrlen %d\n", index, addr_str, addr_raw, *addrlen);
		return 0;
	}
	// fprintf(stdout, "fd_getvlocaladdr: not found vlocaladdr: %d\n", index);
	return 1;
}

static enum fd_type fd_close(int index, int *fd)
{
	struct fd_info *fdi;
	enum fd_type type;

	fdi = idm_lookup(&idm, index);
	if (fdi) {
		idm_clear(&idm, index);
		*fd = fdi->fd;
		type = fdi->type;
		real.close(index);
		free(fdi);
	} else {
		*fd = index;
		type = fd_normal;
	}
	return type;
}

static void getenv_options(void)
{
	char *var;

	var = getenv("RS_SQ_SIZE");
	if (var)
		sq_size = atoi(var);

	var = getenv("RS_RQ_SIZE");
	if (var)
		rq_size = atoi(var);

	var = getenv("RS_INLINE");
	if (var)
		sq_inline = atoi(var);

	var = getenv("RDMAV_FORK_SAFE");
	if (var)
		fork_support = atoi(var);
}

static void init_preload(void)
{
	static int init;

	/* Quick check without lock */
	if (init)
		return;

	pthread_mutex_lock(&mut);
	if (init)
		goto out;

	real.socket = dlsym(RTLD_NEXT, "socket");
	real.bind = dlsym(RTLD_NEXT, "bind");
	real.listen = dlsym(RTLD_NEXT, "listen");
	real.accept = dlsym(RTLD_NEXT, "accept");
	real.connect = dlsym(RTLD_NEXT, "connect");
	real.recv = dlsym(RTLD_NEXT, "recv");
	real.recvfrom = dlsym(RTLD_NEXT, "recvfrom");
	real.recvmsg = dlsym(RTLD_NEXT, "recvmsg");
	real.read = dlsym(RTLD_NEXT, "read");
	real.readv = dlsym(RTLD_NEXT, "readv");
	real.send = dlsym(RTLD_NEXT, "send");
	real.sendto = dlsym(RTLD_NEXT, "sendto");
	real.sendmsg = dlsym(RTLD_NEXT, "sendmsg");
	real.write = dlsym(RTLD_NEXT, "write");
	real.writev = dlsym(RTLD_NEXT, "writev");
	real.poll = dlsym(RTLD_NEXT, "poll");
	real.shutdown = dlsym(RTLD_NEXT, "shutdown");
	real.close = dlsym(RTLD_NEXT, "close");
	real.getpeername = dlsym(RTLD_NEXT, "getpeername");
	real.getsockname = dlsym(RTLD_NEXT, "getsockname");
	real.setsockopt = dlsym(RTLD_NEXT, "setsockopt");
	real.getsockopt = dlsym(RTLD_NEXT, "getsockopt");
	real.fcntl = dlsym(RTLD_NEXT, "fcntl");
	real.dup2 = dlsym(RTLD_NEXT, "dup2");
	real.sendfile = dlsym(RTLD_NEXT, "sendfile");
	real.fxstat = dlsym(RTLD_NEXT, "__fxstat");

	rs.socket = dlsym(RTLD_DEFAULT, "rsocket");
	rs.bind = dlsym(RTLD_DEFAULT, "rbind");
	rs.listen = dlsym(RTLD_DEFAULT, "rlisten");
	rs.accept = dlsym(RTLD_DEFAULT, "raccept");
	rs.connect = dlsym(RTLD_DEFAULT, "rconnect");
	rs.recv = dlsym(RTLD_DEFAULT, "rrecv");
	rs.recvfrom = dlsym(RTLD_DEFAULT, "rrecvfrom");
	rs.recvmsg = dlsym(RTLD_DEFAULT, "rrecvmsg");
	rs.read = dlsym(RTLD_DEFAULT, "rread");
	rs.readv = dlsym(RTLD_DEFAULT, "rreadv");
	rs.send = dlsym(RTLD_DEFAULT, "rsend");
	rs.sendto = dlsym(RTLD_DEFAULT, "rsendto");
	rs.sendmsg = dlsym(RTLD_DEFAULT, "rsendmsg");
	rs.write = dlsym(RTLD_DEFAULT, "rwrite");
	rs.writev = dlsym(RTLD_DEFAULT, "rwritev");
	rs.poll = dlsym(RTLD_DEFAULT, "rpoll");
	rs.shutdown = dlsym(RTLD_DEFAULT, "rshutdown");
	rs.close = dlsym(RTLD_DEFAULT, "rclose");
	rs.getpeername = dlsym(RTLD_DEFAULT, "rgetpeername");
	rs.getsockname = dlsym(RTLD_DEFAULT, "rgetsockname");
	rs.setsockopt = dlsym(RTLD_DEFAULT, "rsetsockopt");
	rs.getsockopt = dlsym(RTLD_DEFAULT, "rgetsockopt");
	rs.fcntl = dlsym(RTLD_DEFAULT, "rfcntl");

	getenv_options();
	scan_config();
	init = 1;
out:
	pthread_mutex_unlock(&mut);
}

/*
 * We currently only handle copying a few common values.
 */
static int copysockopts(int dfd, int sfd, struct socket_calls *dapi,
			struct socket_calls *sapi)
{
	socklen_t len;
	int param, ret;

	ret = sapi->fcntl(sfd, F_GETFL);
	if (ret > 0)
		ret = dapi->fcntl(dfd, F_SETFL, ret);
	if (ret)
		return ret;

	len = sizeof param;
	ret = sapi->getsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &param, &len);
	if (param && !ret)
		ret = dapi->setsockopt(dfd, SOL_SOCKET, SO_REUSEADDR, &param, len);
	if (ret)
		return ret;

	len = sizeof param;
	ret = sapi->getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, &param, &len);
	if (param && !ret)
		ret = dapi->setsockopt(dfd, SOL_SOCKET, SO_SNDBUF, &param, len);
	if (ret)
		return ret;

	len = sizeof param;
	ret = sapi->getsockopt(sfd, SOL_SOCKET, SO_RCVBUF, &param, &len);
	if (param && !ret)
		ret = dapi->setsockopt(dfd, SOL_SOCKET, SO_RCVBUF, &param, len);
	if (ret)
		return ret;

	len = sizeof param;
	ret = sapi->getsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, &param, &len);
	if (param && !ret)
		ret = dapi->setsockopt(dfd, IPPROTO_TCP, TCP_NODELAY, &param, len);
	if (ret)
		return ret;

	return 0;
}

/*
 * Convert between an rsocket and a normal socket.
 */
static int transpose_socket(int socket, enum fd_type new_type)
{
	socklen_t len = 0;
	int sfd, dfd, param, ret;
	struct socket_calls *sapi, *dapi;

	// fprintf(stdout, "transpose_socket: transpose_socket: %d %d\n", socket, new_type);

	sfd = fd_getd(socket);
	if (new_type == fd_rsocket) {
		dapi = &rs;
		sapi = &real;
	} else {
		dapi = &real;
		sapi = &rs;
	}

	ret = sapi->getsockname(sfd, NULL, &len);
	if (ret)
		return ret;

	param = (len == sizeof(struct sockaddr_in6)) ? PF_INET6 : PF_INET;
	dfd = dapi->socket(param, SOCK_STREAM, 0);
	if (dfd < 0)
		return dfd;

	ret = copysockopts(dfd, sfd, dapi, sapi);
	if (ret)
		goto err;

	fd_store(socket, dfd, new_type, fd_ready);
	return dfd;

err:
	dapi->close(dfd);
	return ret;
}

/*
 * Use defaults on failure.
 */
static void set_rsocket_options(int rsocket)
{
	if (sq_size)
		rsetsockopt(rsocket, SOL_RDMA, RDMA_SQSIZE, &sq_size, sizeof sq_size);

	if (rq_size)
		rsetsockopt(rsocket, SOL_RDMA, RDMA_RQSIZE, &rq_size, sizeof rq_size);

	if (sq_inline)
		rsetsockopt(rsocket, SOL_RDMA, RDMA_INLINE, &sq_inline, sizeof sq_inline);
}

int socket(int domain, int type, int protocol)
{
	static __thread int recursive;
	int index, ret;

	// fprintf(stdout, "socket: socket: %d %d %d\n", domain, type, protocol);
	init_preload();

	if (recursive || !intercept_socket(domain, type, protocol))
		goto real;

	index = fd_open();
	if (index < 0)
		return index;

	if ((domain == PF_INET || domain == PF_INET6) &&
	    (type == SOCK_STREAM) && (!protocol || protocol == IPPROTO_TCP)) {
		ret = real.socket(domain, type, protocol);
		if (ret < 0)
			return ret;
		if (fork_support) {
			// fprintf(stdout, "socket: fork_support: %d\n", index);
			fd_store(index, ret, fd_normal, fd_fork);
		} else {
			// fprintf(stdout, "socket: tiaccoon: %d\n", index);
			fd_store(index, ret, fd_normal, fd_tiaccoon);
		}
		return index;
	}

	recursive = 1;
	ret = rsocket(domain, type, protocol);
	// fprintf(stdout, "socket: rsocket after: %d\n", ret);
	recursive = 0;
	if (ret >= 0) {
		fd_store(index, ret, fd_rsocket, fd_ready);
		set_rsocket_options(ret);
		// fprintf(stdout, "socket: set_rsocket_options: %d\n", index);
		return index;
	}
	fd_close(index, &ret);
real:
	ret = real.socket(domain, type, protocol);
	// fprintf(stdout, "socket: real.socket: %d\n", ret);
	return ret;
}

int bind(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	int fd, ret, ret2, domain, rfd;
	// int rsock;
	struct sockaddr *paddr;
	socklen_t paddrlen;
	// char *addr_str, *addr_raw;
	struct fd_info *fdi;
	// addr_str = sockaddr2char(addr);
	// addr_raw = byte2char(addr->sa_data, addrlen);
	// fprintf(stdout, "bind: bind: %d addr %s raw_addr %s addrlen %d\n", socket, addr_str, addr_raw, addrlen);
	if (fd_get(socket, &fd) == fd_rsocket) {
		// fprintf(stdout, "bind: fd_rsocket: %d %d\n", socket, fd);
		ret = rbind(fd, addr, addrlen);
		// fprintf(stdout, "bind: rbind: %d %d ret %d errno %d\n", socket, fd, ret, errno);
		return ret;
	} else {// fd_normal
		if (fd_gets(socket) == fd_tiaccoon) {
			// fprintf(stdout, "bind: tiaccoon: %d %d\n", socket, fd);
			fd_store_vlocaladdr(socket, addr, &addrlen);
			// fprintf(stdout, "bind: fd_store_vlocaladdr 1: %d %d\n", socket, fd);
			paddr = malloc(sizeof(struct sockaddr));
			paddrlen = sizeof(struct sockaddr);
			ret2 = fd_getvlocaladdr(socket, paddr, &paddrlen); // Use physical addr
			if (ret2) {
				// fprintf(stdout, "bind: fd_getvlocaladdr failed: %d %d\n", socket, fd);
				return ret2;
			}
			// fprintf(stdout, "bind: fd_getvlocaladdr physical addr: %d %d %d %d\n", socket, fd, ret2, paddrlen);
			ret = real.bind(fd, paddr, paddrlen);
			// fprintf(stdout, "bind: real.bind(tiaccoon): %d %d ret %d\n", socket, fd, ret);
			if (ret > ETRYRDMA) {
				paddrlen = ret - ETRYRDMA;
			// if (1) { // TODO: remove
				// addr_str = sockaddr2char(paddr);
				// addr_raw = byte2char(paddr->sa_data, paddrlen);
				// fprintf(stdout, "bind: try rdma: %d %d paddr %s raw_paddr %s paddrlen %d ret %d errno %d\n",
				// 	socket,
				// 	fd,
				// 	addr_str,
				// 	addr_raw,
				// 	paddrlen,
				// 	ret,
				// 	errno);
				// rsock = fd_open();
				// // fprintf(stdout, "bind: fd_open: %d\n", rsock);
				// if (rsock < 0)
				// 	return 0; // binded only real.socket
				// rsock = 0; // TODO: remove
				domain = (paddrlen == sizeof(struct sockaddr_in6)) ? PF_INET6 : PF_INET;
				// fprintf(stdout, "bind: rsocket before: %d %d %d %d\n", rsock, domain ,SOCK_STREAM, 0);
				rfd = rsocket(domain, SOCK_STREAM, 0);
				// fprintf(stdout, "bind: rsocket after: %d %d %d %d rfd %d\n", rsock, domain ,SOCK_STREAM, 0, rfd);
				if (rfd < 0)
					return 0; // binded only real.socket
				ret = copysockopts(rfd, fd, &rs, &real);
				// fprintf(stdout, "bind: copysockopts: %d %d %d %d ret %d\n", socket, rsock, fd, rfd, ret);
				if (ret) {
					rclose(rfd);
					return 0; // binded only real.socket
				}
				fd_store(socket, rfd, fd_rsocket, fd_ready);
				fdi = idm_at(&idm, socket);
				if (!fdi) {
					// fprintf(stdout, "bind: idm_at failed: %d %d\n", socket, fd);
					return ret;
				}
				fdi->realfd = fd;
				ret = rbind(rfd, paddr, paddrlen);
				// fprintf(stdout, "bind: rbind: %d %d ret %d errno %d\n", socket, fd, ret, errno);
				// addr_str = sockaddr2char(addr);
				// addr_raw = byte2char(addr->sa_data, addrlen);
				// fprintf(stdout, "bind: ret addr is vlocaladdr: %d addr %s raw_addr %s addrlen %d\n",
				// 	socket,
				// 	addr_str,
				// 	addr_raw,
				// 	addrlen);
				return ret;
			} else {
				return ret;
			}
		} else {
			// fprintf(stdout, "bind: not tiaccoon: %d %d\n", socket, fd);
			ret = real.bind(fd, addr, addrlen);
			// fprintf(stdout, "bind: real.bind(not tiaccoon): %d %d ret %d\n", socket, fd, ret);
			return ret;
		}
	}
}

int listen(int socket, int backlog)
{
	int fd, ret;
	// int retreal;
	struct fd_info *fdi;
	// fprintf(stdout, "listen: listen: %d %d\n", socket, backlog);
	if (fd_get(socket, &fd) == fd_rsocket) {
		// fprintf(stdout, "listen: fd_rsocket: %d %d\n", socket, fd);
		ret = rlisten(fd, backlog);
		fdi = idm_lookup(&idm, socket);
		if (!fdi) {
			// fprintf(stdout, "listen: idm_lookup failed: %d %d\n", socket, fd);
			return ret;
		}
		// fprintf(stdout, "listen: idm_lookup: %d %d %d\n", socket, fd, fdi->realfd);
		if (fdi->realfd != -1) { // tiaccoon
			// fprintf(stdout, "listen: tiaccoon: %d %d %d\n", socket, fd, fdi->realfd);
			real.listen(fdi->realfd, backlog);
			// fprintf(stdout, "listen: real.listen(tiaccoon): %d %d %d ret %d\n", socket, fd, fdi->realfd, retreal);
			// TODO: accept queue
			// return ret;
		}
	} else { //fd_normal
		// fprintf(stdout, "listen: not fd_rsocket: %d %d\n", socket, fd);
		ret = real.listen(fd, backlog);
		// fprintf(stdout, "listen: real.listen: %d %d ret %d\n", socket, fd, ret);
		if (!ret && fd_gets(socket) == fd_fork) {
			// fprintf(stdout, "listen: fork: %d %d\n", socket, fd);
			fd_store(socket, fd, fd_normal, fd_fork_listen);
		}
	}
	return ret;
}

int accept(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	int fd, index, ret;
	struct fd_info *fdi;
	struct sockaddr *srcvlocaladdr;
	socklen_t *srcvlocaladdrlen;
	// fprintf(stdout, "accept: accept: %d\n", socket);

	if (fd_get(socket, &fd) == fd_rsocket) {
		// fprintf(stdout, "accept: fd_rsocket: %d %d\n", socket, fd);
		index = fd_open();
		if (index < 0)
			return index;

		// TODO: accept queue
		ret = raccept(fd, addr, addrlen);
		// fprintf(stdout, "accept: raccept: %d %d index %d ret %d\n", socket, fd, index, ret);
		if (ret < 0) {
			fd_close(index, &fd);
			fdi = idm_lookup(&idm, socket);
			if (!fdi) {
				// fprintf(stdout, "accept: idm_lookup failed: %d %d\n", socket, fd);
				return ret;
			}
			// fprintf(stdout, "accept: idm_lookup: %d %d %d\n", socket, fd, fdi->realfd);
			if (fdi->realfd != -1) {
				// fprintf(stdout, "accept: tiaccoon: %d %d %d\n", socket, fd, fdi->realfd);
				ret = real.accept(fdi->realfd, addr, addrlen);
				// fprintf(stdout, "accept: real.accept: %d %d %d ret %d\n", socket, fd, fdi->realfd, ret);
			}
			return ret;
		}
		srcvlocaladdr = malloc(sizeof(struct sockaddr));
		srcvlocaladdrlen = malloc(sizeof(socklen_t));
		fd_getvlocaladdr(socket, srcvlocaladdr, srcvlocaladdrlen);
		// fprintf(stdout, "accept: fd_getvlocaladdr: %d %d %d\n", socket, fd, index);

		fd_store(index, ret, fd_rsocket, fd_ready);
		fd_store_vlocaladdr(index, srcvlocaladdr, srcvlocaladdrlen);
		return index;
	} else if (fd_gets(socket) == fd_fork_listen) {
		// fprintf(stdout, "accept: fd_fork_listen: %d %d\n", socket, fd);
		index = fd_open();
		if (index < 0)
			return index;

		ret = real.accept(fd, addr, addrlen);
		if (ret < 0) {
			fd_close(index, &fd);
			return ret;
		}

		fd_store(index, ret, fd_normal, fd_fork_passive);
		return index;
	} else {
		return real.accept(fd, addr, addrlen);
	}
}

/*
 * We can't fork RDMA connections and pass them from the parent to the child
 * process.  Instead, we need to establish the RDMA connection after calling
 * fork.  To do this, we delay establishing the RDMA connection until we try
 * to send/receive on the server side.
 */
static void fork_active(int socket)
{
	struct sockaddr_storage addr;
	int sfd, dfd, ret;
	socklen_t len;
	uint32_t msg;
	long flags;

	sfd = fd_getd(socket);

	flags = real.fcntl(sfd, F_GETFL);
	real.fcntl(sfd, F_SETFL, 0);
	ret = real.recv(sfd, &msg, sizeof msg, MSG_PEEK);
	real.fcntl(sfd, F_SETFL, flags);
	if ((ret != sizeof msg) || msg)
		goto err1;

	len = sizeof addr;
	ret = real.getpeername(sfd, (struct sockaddr *) &addr, &len);
	if (ret)
		goto err1;

	dfd = rsocket(addr.ss_family, SOCK_STREAM, 0);
	if (dfd < 0)
		goto err1;

	ret = rconnect(dfd, (struct sockaddr *) &addr, len);
	if (ret)
		goto err2;

	set_rsocket_options(dfd);
	copysockopts(dfd, sfd, &rs, &real);
	real.shutdown(sfd, SHUT_RDWR);
	real.close(sfd);
	fd_store(socket, dfd, fd_rsocket, fd_ready);
	return;

err2:
	rclose(dfd);
err1:
	fd_store(socket, sfd, fd_normal, fd_ready);
}

/*
 * The server will start listening for the new connection, then send a
 * message to the active side when the listen is ready.  This does leave
 * fork unsupported in the following case: the server is nonblocking and
 * calls select/poll waiting to receive data from the client.
 */
static void fork_passive(int socket)
{
	struct sockaddr_in6 sin6;
	sem_t *sem;
	int lfd, sfd, dfd, ret, param;
	socklen_t len;
	uint32_t msg;

	sfd = fd_getd(socket);

	len = sizeof sin6;
	ret = real.getsockname(sfd, (struct sockaddr *) &sin6, &len);
	if (ret)
		goto out;
	sin6.sin6_flowinfo = 0;
	sin6.sin6_scope_id = 0;
	memset(&sin6.sin6_addr, 0, sizeof sin6.sin6_addr);

	sem = sem_open("/rsocket_fork", O_CREAT | O_RDWR,
		       S_IRWXU | S_IRWXG, 1);
	if (sem == SEM_FAILED) {
		ret = -1;
		goto out;
	}

	lfd = rsocket(sin6.sin6_family, SOCK_STREAM, 0);
	if (lfd < 0) {
		ret = lfd;
		goto sclose;
	}

	param = 1;
	rsetsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &param, sizeof param);

	sem_wait(sem);
	ret = rbind(lfd, (struct sockaddr *) &sin6, sizeof sin6);
	if (ret)
		goto lclose;

	ret = rlisten(lfd, 1);
	if (ret)
		goto lclose;

	msg = 0;
	len = real.write(sfd, &msg, sizeof msg);
	if (len != sizeof msg)
		goto lclose;

	dfd = raccept(lfd, NULL, NULL);
	if (dfd < 0) {
		ret  = dfd;
		goto lclose;
	}

	set_rsocket_options(dfd);
	copysockopts(dfd, sfd, &rs, &real);
	real.shutdown(sfd, SHUT_RDWR);
	real.close(sfd);
	fd_store(socket, dfd, fd_rsocket, fd_ready);

lclose:
	rclose(lfd);
	sem_post(sem);
sclose:
	sem_close(sem);
out:
	if (ret)
		fd_store(socket, sfd, fd_normal, fd_ready);
}

static inline enum fd_type fd_fork_get(int index, int *fd)
{
	struct fd_info *fdi;

	fdi = idm_lookup(&idm, index);
	if (fdi) {
		if (fdi->state == fd_fork_passive)
			fork_passive(index);
		else if (fdi->state == fd_fork_active)
			fork_active(index);
		*fd = fdi->fd;
		return fdi->type;

	} else {
		*fd = index;
		return fd_normal;
	}
}

int connect(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	int fd, ret, ret2;
	struct sockaddr *paddr;
	socklen_t paddrlen;
	// char *addr_str, *addr_raw;

	// addr_str = sockaddr2char(addr);
	// addr_raw = byte2char(addr->sa_data, addrlen);
	// fprintf(stdout, "connect: connect: %d addr %s raw_addr %s addrlen %d\n",
	// 	socket,
	// 	addr_str,
	// 	addr_raw,
	// 	addrlen);

	if (fd_get(socket, &fd) == fd_normal) {
		if (fd_gets(socket) == fd_tiaccoon) {
			fd_store_vlocaladdr(socket, addr, &addrlen);
			// fprintf(stdout, "connect: fd_store_vlocaladdr: %d %d\n", socket, fd);
			paddr = malloc(sizeof(struct sockaddr));
			paddrlen = sizeof(struct sockaddr);
			ret2 = fd_getvlocaladdr(socket, paddr, &paddrlen); // Use physical addr
			if (ret2) {
				// fprintf(stdout, "connect: fd_getvlocaladdr failed: %d %d\n", socket, fd);
				return ret2;
			}
			// fprintf(stdout, "connect: fd_getvlocaladdr physical addr: %d %d %d %d\n", socket, fd, ret2, paddrlen);
			ret = real.connect(fd, paddr, paddrlen); // tiaccoon
			// fprintf(stdout, "connect: tiaccoon: fd %d ret %d errno %d\n", fd, ret, errno);
			if (ret > ETRYRDMA) {
				paddrlen = ret - ETRYRDMA;
			// if (1) { // debug
				// addr_str = sockaddr2char(paddr);
				// addr_raw = byte2char(paddr->sa_data, paddrlen);
				// fprintf(stdout, "connect: try rdma: %d paddr %s raw_paddr %s paddrlen %d ret %d errno %d\n",
				// 	socket,
				// 	addr_str,
				// 	addr_raw,
				// 	paddrlen,
				// 	ret,
				// 	errno);
				ret = transpose_socket(socket, fd_rsocket);
				// fprintf(stdout, "connect: transpose_socket to rsocket: %d\n", ret);
				if (ret < 0)
					return ret;

				rclose(fd);
				fd = ret;
				ret = rconnect(fd, paddr, paddrlen);
				// fprintf(stdout, "connect: rconnect(tiaccoon): %d\n", ret);
				if (!ret || errno == EINPROGRESS) {
					// addr_str = sockaddr2char(addr);
					// addr_raw = byte2char(addr->sa_data, addrlen);
					// fprintf(stdout, "connect: ret addr is vlocaladdr: %d addr %s raw_addr %s addrlen %d\n",
					// 	socket,
					// 	addr_str,
					// 	addr_raw,
					// 	addrlen);
					return ret;
				}
			}
			fd_store(socket, fd, fd_normal, fd_ready);
			// fprintf(stdout, "connect: not rdma\n");
			return ret;
		} else if (fd_gets(socket) == fd_fork) {
			fd_store(socket, fd, fd_normal, fd_fork_active);
		}
	} else { // fd_rsocket
		// fprintf(stdout, "connect: fd_rsocket\n");
		ret = rconnect(fd, addr, addrlen);
		// fprintf(stdout, "connect: rconnect: %d\n", ret);
		if (!ret || errno == EINPROGRESS)
			return ret;

		ret = transpose_socket(socket, fd_normal);
		// fprintf(stdout, "connect: transpose_socket to normal: %d\n", ret);
		if (ret < 0)
			return ret;

		rclose(fd);
		fd = ret;
	}

	ret = real.connect(fd, addr, addrlen);
	// fprintf(stdout, "connect: real.connect: %d %d errno %d\n", fd, ret, errno);
	return ret;
}

ssize_t recv(int socket, void *buf, size_t len, int flags)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rrecv(fd, buf, len, flags) : real.recv(fd, buf, len, flags);
}

ssize_t recvfrom(int socket, void *buf, size_t len, int flags,
		 struct sockaddr *src_addr, socklen_t *addrlen)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rrecvfrom(fd, buf, len, flags, src_addr, addrlen) :
		real.recvfrom(fd, buf, len, flags, src_addr, addrlen);
}

ssize_t recvmsg(int socket, struct msghdr *msg, int flags)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rrecvmsg(fd, msg, flags) : real.recvmsg(fd, msg, flags);
}

ssize_t read(int socket, void *buf, size_t count)
{
	int fd;
	init_preload();
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rread(fd, buf, count) : real.read(fd, buf, count);
}

ssize_t readv(int socket, const struct iovec *iov, int iovcnt)
{
	int fd;
	init_preload();
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rreadv(fd, iov, iovcnt) : real.readv(fd, iov, iovcnt);
}

ssize_t send(int socket, const void *buf, size_t len, int flags)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rsend(fd, buf, len, flags) : real.send(fd, buf, len, flags);
}

ssize_t sendto(int socket, const void *buf, size_t len, int flags,
		const struct sockaddr *dest_addr, socklen_t addrlen)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rsendto(fd, buf, len, flags, dest_addr, addrlen) :
		real.sendto(fd, buf, len, flags, dest_addr, addrlen);
}

ssize_t sendmsg(int socket, const struct msghdr *msg, int flags)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rsendmsg(fd, msg, flags) : real.sendmsg(fd, msg, flags);
}

ssize_t write(int socket, const void *buf, size_t count)
{
	int fd;
	init_preload();
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rwrite(fd, buf, count) : real.write(fd, buf, count);
}

ssize_t writev(int socket, const struct iovec *iov, int iovcnt)
{
	int fd;
	init_preload();
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rwritev(fd, iov, iovcnt) : real.writev(fd, iov, iovcnt);
}

static struct pollfd *fds_alloc(nfds_t nfds)
{
	static __thread struct pollfd *rfds;
	static __thread nfds_t rnfds;

	if (nfds > rnfds) {
		if (rfds)
			free(rfds);

		rfds = malloc(sizeof(*rfds) * nfds);
		rnfds = rfds ? nfds : 0;
	}

	return rfds;
}

int poll(struct pollfd *fds, nfds_t nfds, int timeout)
{
	struct pollfd *rfds;
	int i, ret;

	init_preload();
	for (i = 0; i < nfds; i++) {
		if (fd_gett(fds[i].fd) == fd_rsocket)
			goto use_rpoll;
	}

	return real.poll(fds, nfds, timeout);

use_rpoll:
	rfds = fds_alloc(nfds);
	if (!rfds)
		return ERR(ENOMEM);

	for (i = 0; i < nfds; i++) {
		rfds[i].fd = fd_getd(fds[i].fd);
		rfds[i].events = fds[i].events;
		rfds[i].revents = 0;
	}

	ret = rpoll(rfds, nfds, timeout);

	for (i = 0; i < nfds; i++)
		fds[i].revents = rfds[i].revents;

	return ret;
}

static void select_to_rpoll(struct pollfd *fds, int *nfds,
			    fd_set *readfds, fd_set *writefds, fd_set *exceptfds)
{
	int fd, events, i = 0;

	for (fd = 0; fd < *nfds; fd++) {
		events = (readfds && FD_ISSET(fd, readfds)) ? POLLIN : 0;
		if (writefds && FD_ISSET(fd, writefds))
			events |= POLLOUT;

		if (events || (exceptfds && FD_ISSET(fd, exceptfds))) {
			fds[i].fd = fd_getd(fd);
			fds[i++].events = events;
		}
	}

	*nfds = i;
}

static int rpoll_to_select(struct pollfd *fds, int nfds,
			   fd_set *readfds, fd_set *writefds, fd_set *exceptfds)
{
	int fd, rfd, i, cnt = 0;

	for (i = 0, fd = 0; i < nfds; fd++) {
		rfd = fd_getd(fd);
		if (rfd != fds[i].fd)
			continue;

		if (readfds && (fds[i].revents & POLLIN)) {
			FD_SET(fd, readfds);
			cnt++;
		}

		if (writefds && (fds[i].revents & POLLOUT)) {
			FD_SET(fd, writefds);
			cnt++;
		}

		if (exceptfds && (fds[i].revents & ~(POLLIN | POLLOUT))) {
			FD_SET(fd, exceptfds);
			cnt++;
		}
		i++;
	}

	return cnt;
}

static int rs_convert_timeout(struct timeval *timeout)
{
	return !timeout ? -1 : timeout->tv_sec * 1000 + timeout->tv_usec / 1000;
}

int select(int nfds, fd_set *readfds, fd_set *writefds,
	   fd_set *exceptfds, struct timeval *timeout)
{
	struct pollfd *fds;
	int ret;

	fds = fds_alloc(nfds);
	if (!fds)
		return ERR(ENOMEM);

	select_to_rpoll(fds, &nfds, readfds, writefds, exceptfds);
	ret = rpoll(fds, nfds, rs_convert_timeout(timeout));

	if (readfds)
		FD_ZERO(readfds);
	if (writefds)
		FD_ZERO(writefds);
	if (exceptfds)
		FD_ZERO(exceptfds);

	if (ret > 0)
		ret = rpoll_to_select(fds, nfds, readfds, writefds, exceptfds);

	return ret;
}

int shutdown(int socket, int how)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rshutdown(fd, how) : real.shutdown(fd, how);
}

int close(int socket)
{
	struct fd_info *fdi;
	int ret;
	// fprintf(stdout, "close: close: %d\n", socket);

	init_preload();
	fdi = idm_lookup(&idm, socket);
	if (!fdi)
		return real.close(socket);

	if (fdi->dupfd != -1) {
		ret = close(fdi->dupfd);
		if (ret)
			return ret;
	}

	if (fdi->realfd != -1) {
		// fprintf(stdout, "close: real.close: %d\n", fdi->realfd);
		ret = real.close(fdi->realfd);
		if (ret)
			return ret;
	}
	// fprintf(stdout, "close: close: socket %d fdi->fd %d\n", socket, fdi->fd);

	if (atomic_fetch_sub(&fdi->refcnt, 1) != 1)
		return 0;

	idm_clear(&idm, socket);
	real.close(socket);
	ret = (fdi->type == fd_rsocket) ? rclose(fdi->fd) : real.close(fdi->fd);
	if (fdi->vlocaladdr) {
		free(fdi->vlocaladdr);
	}
	if (fdi->vlocaladdrlen) {
		free(fdi->vlocaladdrlen);
	}
	free(fdi);
	return ret;
}

int getpeername(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rgetpeername(fd, addr, addrlen) :
		real.getpeername(fd, addr, addrlen);
}

int getsockname(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	int fd;
	// char *addr_str, *addr_raw;
	init_preload();
	// addr_str = sockaddr2char(addr);
	// addr_raw = byte2char(addr->sa_data, *addrlen);
	// fprintf(stdout, "getsockname: getsockname: %d addr %s raw_addr %s addrlen %d\n", socket, addr_str, addr_raw, *addrlen);

	if (!fd_getvlocaladdr(socket, addr, addrlen)) {
		return 0;
	}

	// fprintf(stdout, "getsockname: normal getsockname: %d\n", socket);

	return (fd_get(socket, &fd) == fd_rsocket) ?
		rgetsockname(fd, addr, addrlen) :
		real.getsockname(fd, addr, addrlen);
}

int setsockopt(int socket, int level, int optname,
		const void *optval, socklen_t optlen)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rsetsockopt(fd, level, optname, optval, optlen) :
		real.setsockopt(fd, level, optname, optval, optlen);
}

int getsockopt(int socket, int level, int optname,
		void *optval, socklen_t *optlen)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rgetsockopt(fd, level, optname, optval, optlen) :
		real.getsockopt(fd, level, optname, optval, optlen);
}

int fcntl(int socket, int cmd, ... /* arg */)
{
	va_list args;
	long lparam;
	void *pparam;
	int fd, ret;

	init_preload();
	va_start(args, cmd);
	switch (cmd) {
	case F_GETFD:
	case F_GETFL:
	case F_GETOWN:
	case F_GETSIG:
	case F_GETLEASE:
		ret = (fd_get(socket, &fd) == fd_rsocket) ?
			rfcntl(fd, cmd) : real.fcntl(fd, cmd);
		break;
	case F_DUPFD:
	/*case F_DUPFD_CLOEXEC:*/
	case F_SETFD:
	case F_SETFL:
	case F_SETOWN:
	case F_SETSIG:
	case F_SETLEASE:
	case F_NOTIFY:
		lparam = va_arg(args, long);
		ret = (fd_get(socket, &fd) == fd_rsocket) ?
			rfcntl(fd, cmd, lparam) : real.fcntl(fd, cmd, lparam);
		break;
	default:
		pparam = va_arg(args, void *);
		ret = (fd_get(socket, &fd) == fd_rsocket) ?
			rfcntl(fd, cmd, pparam) : real.fcntl(fd, cmd, pparam);
		break;
	}
	va_end(args);
	return ret;
}

/*
 * dup2 is not thread safe
 */
int dup2(int oldfd, int newfd)
{
	struct fd_info *oldfdi, *newfdi;
	int ret;

	init_preload();
	oldfdi = idm_lookup(&idm, oldfd);
	if (oldfdi) {
		if (oldfdi->state == fd_fork_passive)
			fork_passive(oldfd);
		else if (oldfdi->state == fd_fork_active)
			fork_active(oldfd);
	}

	newfdi = idm_lookup(&idm, newfd);
	if (newfdi) {
		 /* newfd cannot have been dup'ed directly */
		if (atomic_load(&newfdi->refcnt) > 1)
			return ERR(EBUSY);
		close(newfd);
	}

	ret = real.dup2(oldfd, newfd);
	if (!oldfdi || ret != newfd)
		return ret;

	newfdi = calloc(1, sizeof(*newfdi));
	if (!newfdi) {
		close(newfd);
		return ERR(ENOMEM);
	}

	pthread_mutex_lock(&mut);
	idm_set(&idm, newfd, newfdi);
	pthread_mutex_unlock(&mut);

	newfdi->fd = oldfdi->fd;
	newfdi->type = oldfdi->type;
	if (oldfdi->dupfd != -1) {
		newfdi->dupfd = oldfdi->dupfd;
		oldfdi = idm_lookup(&idm, oldfdi->dupfd);
	} else {
		newfdi->dupfd = oldfd;
	}
	atomic_store(&newfdi->refcnt, 1);
	atomic_fetch_add(&oldfdi->refcnt, 1);
	return newfd;
}

ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count)
{
	void *file_addr;
	int fd;
	size_t ret;

	if (fd_get(out_fd, &fd) != fd_rsocket)
		return real.sendfile(fd, in_fd, offset, count);

	file_addr = mmap(NULL, count, PROT_READ, 0, in_fd, offset ? *offset : 0);
	if (file_addr == (void *) -1)
		return -1;

	ret = rwrite(fd, file_addr, count);
	if ((ret > 0) && offset)
		lseek(in_fd, ret, SEEK_CUR);
	munmap(file_addr, count);
	return ret;
}

int __fxstat(int ver, int socket, struct stat *buf)
{
	int fd, ret;

	init_preload();
	if (fd_get(socket, &fd) == fd_rsocket) {
		ret = real.fxstat(ver, socket, buf);
		if (!ret)
			buf->st_mode = (buf->st_mode & ~S_IFMT) | S_IFSOCK;
	} else {
		ret = real.fxstat(ver, fd, buf);
	}
	return ret;
}
