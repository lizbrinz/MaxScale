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
#include <spinlock.h>
#include <buffer.h>

BUFFER_QUEUE *
buffer_queue_alloc()
{
    BUFFER_QUEUE *queue;
    if ((queue = malloc(sizeof(BUFFER_QUEUE))))
    {
        spinlock_init(&queue->lock_inbox);
        spinlock_init(&queue->lock_outbox);
        queue->inbox_head = queue->inbox_tail = queue->outbox_head = queue->outbox_tail = NULL;
        return queue;
    }
    return NULL;
}

void
buffer_queue_free(BUFFER_QUEUE *queue)
{
    if (queue)
    {
        gwbuf_free(inbox_head);
        gwbuf_free(outbox_head);
        free(queue);
    }
}

void
buffer_enqueue(BUFFER_QUEUE *queue, GWBUF *buffer)
{
    spinlock_acquire(&queue->lock_inbox);
    if (queue->inbox_tail)
    {
        queue->inbox_tail->next = buffer;
    }
    else
    {
        queue->inbox_head = buffer;
    }
    queue->inbox_tail = buffer->tail;
    spinlock_release(&queue->lock_inbox);
}

GWBUF *
buffer_dequeue(BUFFER_QUEUE *queue)
{
    GWBUF *buffer;

    spinlock_acquire(&queue->lock_outbox);
    if (queue->outbox_head)
    {
        buffer = queue->outbox_head;
        queue->outbox_head = buffer->next;
    }
    else
    {
        spinlock_acquire(&queue->lock_inbox);
        queue->outbox_head = queue->inbox_head;
        queue->outbox_tail = queue->inbox_tail;
        queue->inbox_head = queue->inbox_tail = NULL;
        buffer = queue->outbox_head;
        if (buffer)
        {
            queue->outbox_head = buffer->next;
        }
        spinlock_release(&queue->lock_inbox);
    }
    spinlock_release(&queue->lock_outbox);
    return buffer;
}

void *
buffer_queue_head_data(BUFFER_QUEUE *queue)
{
    return GWBUF_DATA(queue->outbox_head);
}

int
buffer_queue_size(BUFFER_QUEUE *queue)
{
    GWBUF *buffer;
    int size;

    spinlock_acquire(&queue->lock_outbox);
    spinlock_acquire(&queue->lock_inbox);
    buffer = queue->inbox_head;
    while(buffer)
    {
        size++;
        buffer = buffer->next;
    }
    buffer = queue->outbox_head;
    while(buffer)
    {
        size++;
        buffer = buffer->next;
    }
    spinlock_release(&queue->lock_inbox);
    spinlock_release(&queue->lock_outbox);
    return size;
}

bool
is_buffer_queue_empty(BUFFER_QUEUE *buffer)
{
    bool empty;

    spinlock_acquire(&queue->lock_outbox);
    spinlock_acquire(&queue->lock_inbox);
    empty = (queue->inbox_head == NULL && queue->outbox_head == NULL);
    spinlock_release(&queue->lock_inbox);
    spinlock_release(&queue->lock_outbox);
    return empty;
}

int
buffer_queue_data_length(BUFFER_QUEUE *buffer)
{
    int size;

    spinlock_acquire(&queue->lock_outbox);
    spinlock_acquire(&queue->lock_inbox);
    size = gwbuf_length(queue->inbox_head) + gwbuf_length(queue->outbox_head);
    spinlock_release(&queue->lock_inbox);
    spinlock_release(&queue->lock_outbox);
    return size;

}