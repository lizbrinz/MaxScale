/*
 * This file is distributed as part of the MariaDB Corporation MaxScale.  It is free
 * software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation,
 * version 2.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Copyright MariaDB Corporation Ab 2013-2016
 */

/*
 * Revision History
 *
 * Date     Who         Description
 * 11-01-2016   Massimiliano Pinto  First Implementation
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dcb.h>
#include <buffer.h>
#include <service.h>
#include <session.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <router.h>
#include <poll.h>
#include <atomic.h>
#include <gw.h>

#define CDC_SMALL_BUFFER 1024
#define CDC_METHOD_MAXLEN 128
#define CDC_USER_MAXLEN 128
#define CDC_HOSTNAME_MAXLEN 512
#define CDC_USERAGENT_MAXLEN 1024
#define CDC_FIELD_MAXLEN 8192
#define CDC_REQUESTLINE_MAXLEN 8192

#define CDC_UNDEFINED                    0
#define CDC_ALLOC                        1
#define CDC_STATE_WAIT_FOR_AUTH          2
#define CDC_STATE_AUTH_OK                3
#define CDC_STATE_AUTH_FAILED            4
#define CDC_STATE_AUTH_ERR               5
#define CDC_STATE_AUTH_NO_SESSION        6
#define CDC_STATE_REGISTRATION           7
#define CDC_STATE_HANDLE_REQUEST         8
#define CDC_STATE_CLOSE                  9

#define CDC_UUID_LEN 32
#define CDC_TYPE_LEN 16
/**
 * CDC session specific data
 */
typedef struct cdc_session
{
    char user[CDC_USER_MAXLEN + 1];            /*< username for authentication */
    char uuid[CDC_UUID_LEN + 1];               /*< client uuid in registration */
    unsigned int flags[2];                     /*< Received flags              */
    uint8_t  auth_data[SHA_DIGEST_LENGTH];     /*< Password Hash               */
    int state;                                 /*< CDC protocol state          */
} CDC_session;

/**
 * CDC protocol
 */
typedef struct  cdc_protocol
{
#ifdef SS_DEBUG
    skygw_chk_t protocol_chk_top;
#endif
    int state;                      /*< CDC protocol state          */
    char user[CDC_USER_MAXLEN + 1]; /*< username for authentication */
    SPINLOCK lock;                  /*< Protocol structure lock     */
    char type[CDC_TYPE_LEN + 1];    /*< Request Type            */
#ifdef SS_DEBUG
    skygw_chk_t protocol_chk_tail;
#endif
} CDC_protocol;

/* routines */
extern int gw_hex2bin(uint8_t *out, const char *in, unsigned int len);
