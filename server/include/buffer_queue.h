#ifndef _BUFFER_QUEUE_H
#define _BUFFER_QUEUE_H
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
#include <spinlock.h>

/**
 * @file dcb_queue.h An implementation of an arbitrarily long queue
 *
 * @verbatim
 * Revision History
 *
 * Date         Who             Description
 * 01/12/15     Martin Brampton Initial implementation
 *
 * @endverbatim
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * The queue structure used to store an arbitrary large queue, implemented
 * with an inbox and an outbox to improve the likelihood of suppliers and
 * consumers being able to operate independently.
 */
typedef struct
{
    SPINLOCK lock_inbox;         /**< Lock to protect the queue inbox */
    SPINLOCK lock_outbox;        /**< Lock to protect the queue outbox */
    void *inbox_head;            /**< Pointer to the head of the inbox list */
    void *inbox_tail;            /**< Pointer to the tail of the inbox list */
    void *outbox_head;           /**< Pointer to the head of the outbox list */
    void *outbox_tail;           /**< Pointer to the tail of the outbox list */
} BUFFER_QUEUE;

#ifdef __cplusplus
}
#endif

#endif /* _BUFFER_QUEUE_H */

