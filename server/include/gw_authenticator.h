#ifndef GW_AUTHENTICATOR_H
#define GW_AUTHENTICATOR_H
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
 * Copyright MariaDB Corporation Ab 2013-2014
 */

/**
 * @file protocol.h
 *
 * The authenticator module interface definitions for MaxScale
 *
 * @verbatim
 * Revision History
 *
 * Date         Who                     Description
 * 17/02/16     Martin Brampton         Initial implementation
 *
 * @endverbatim
 */

#include <buffer.h>
#include <openssl/crypto.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/dh.h>

struct dcb;
struct server;
struct session;

/**
 * @verbatim
 * The operations that can be performed on the descriptor
 *
 *      extract         Extract the data from a buffer and place in a structure
 *      connectssl      Determine whether the connection can support SSL
 *      authenticate    Carry out the authentication
 * @endverbatim
 *
 * This forms the "module object" for authenticator modules within the gateway.
 *
 * @see load_module
 */
typedef struct gw_authenticator
{
    int (*extract)(struct dcb *, GWBUF *);
    bool (*connectssl)(struct dcb *);
    int (*authenticate)(struct dcb *);
    void (*free)(struct dcb *);
} GWAUTHENTICATOR;

/**
 * The GWAUTHENTICATOR version data. The following should be updated whenever
 * the GWAUTHENTICATOR structure is changed. See the rules defined in modinfo.h
 * that define how these numbers should change.
 */
#define GWAUTHENTICATOR_VERSION      {1, 0, 0}


#endif /* GW_AUTHENTICATOR_H */

