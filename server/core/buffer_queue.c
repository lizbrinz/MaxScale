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
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <buffer_queue.h>

BUFFER_QUEUE *
buffer_queue_alloc()
{

}

void *
buffer_queue_free(BUFFER_QUEUE *queue)
{

}

void *
buffer_enqueue(BUFFER_QUEUE *queue, DCB *dcb)
{
    spinlock_acquire(&queue->lock_inbox);
    queue->inbox_head = dcb;
    dcb->
}

DCB *
buffer_dequeue(BUFFER_QUEUE *queue)
{

}

int
buffer_queue_size(BUFFER_QUEUE *queue)
{

}