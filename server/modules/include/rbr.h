#ifndef _RBR_H
#define _RBR_H

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

/**
* @file rbr.h - Row based replication handling and conversion to Avro format
*/

#include <avro.h>
#include <mxs_avro.h>
#include <binlog_common.h>
#include <mysql_binlog.h>

void handle_table_map_event(AVRO_INSTANCE *router, REP_HEADER *hdr, uint8_t *ptr);
void handle_row_event(AVRO_INSTANCE *router, REP_HEADER *hdr, uint8_t *ptr);
uint8_t* process_row_event(TABLE_MAP *map, TABLE_CREATE *create, avro_value_t *record,
                       uint8_t *ptr, uint64_t columns_present, uint64_t columns_update);
#endif
