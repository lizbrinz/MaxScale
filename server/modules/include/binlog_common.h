#ifndef BINLOG_COMMON_H
#define BINLOG_COMMON_H

/*
 * This file is distributed as part of MaxScale.  It is free
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
 * Copyright MariaDB Corporation Ab 2016
 */

#include <stdbool.h>
#include <stdint.h>
#include <time.h>
/**
 * Packet header for replication messages
 */
typedef struct rep_header
{
    int     payload_len;    /*< Payload length (24 bits) */
    uint8_t     seqno;      /*< Response sequence number */
    uint8_t     ok;     /*< OK Byte from packet */
    uint32_t    timestamp;  /*< Timestamp - start of binlog record */
    uint8_t     event_type; /*< Binlog event type */
    uint32_t    serverid;   /*< Server id of master */
    uint32_t    event_size; /*< Size of header, post-header and body */
    uint32_t    next_pos;   /*< Position of next event */
    uint16_t    flags;      /*< Event flags */
} REP_HEADER;

/** Format Description event info */
typedef struct binlog_event_desc
{
    unsigned long long event_pos;
    uint8_t event_type;
    time_t event_time;
} BINLOG_EVENT_DESC;

int blr_file_get_next_binlogname(const char *binlog_name);
bool blr_next_binlog_exists(const char* binlogdir, const char* binlog);

#endif /* BINLOG_COMMON_H */

