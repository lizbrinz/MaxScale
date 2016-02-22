#ifndef MYSQL_BINLOG_H
#define MYSQL_BINLOG_H

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
 * Copyright MariaDB Corporation Ab 2016
 */

/**
 * @file mysql_binlog.h - Extracting information from binary logs
 */

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

/** Table map column types */
#define TABLE_COL_TYPE_DECIMAL 0x00
#define TABLE_COL_TYPE_TINY 0x01
#define TABLE_COL_TYPE_SHORT 0x02
#define TABLE_COL_TYPE_LONG 0x03
#define TABLE_COL_TYPE_FLOAT 0x04
#define TABLE_COL_TYPE_DOUBLE 0x05
#define TABLE_COL_TYPE_NULL 0x06
#define TABLE_COL_TYPE_TIMESTAMP 0x07
#define TABLE_COL_TYPE_LONGLONG 0x08
#define TABLE_COL_TYPE_INT24 0x09
#define TABLE_COL_TYPE_DATE 0x0a
#define TABLE_COL_TYPE_TIME 0x0b
#define TABLE_COL_TYPE_DATETIME 0x0c
#define TABLE_COL_TYPE_YEAR 0x0d
#define TABLE_COL_TYPE_NEWDATE 0x0e
#define TABLE_COL_TYPE_VARCHAR 0x0f
#define TABLE_COL_TYPE_BIT 0x10
#define TABLE_COL_TYPE_TIMESTAMP2 0x11
#define TABLE_COL_TYPE_DATETIME2 0x12
#define TABLE_COL_TYPE_TIME2 0x13
#define TABLE_COL_TYPE_NEWDECIMAL 0xf6
#define TABLE_COL_TYPE_ENUM 0xf7
#define TABLE_COL_TYPE_SET 0xf8
#define TABLE_COL_TYPE_TINY_BLOB 0xf9
#define TABLE_COL_TYPE_MEDIUM_BLOB 0xfa
#define TABLE_COL_TYPE_LONG_BLOB 0xfb
#define TABLE_COL_TYPE_BLOB 0xfc
#define TABLE_COL_TYPE_VAR_STRING 0xfd
#define TABLE_COL_TYPE_STRING 0xfe
#define TABLE_COL_TYPE_GEOMETRY 0xff

/**
 * RBR row event flags
 */
#define ROW_EVENT_END_STATEMENT 0x0001
#define ROW_EVENT_NO_FKCHECK 0x0002
#define ROW_EVENT_NO_UKCHECK 0x0004
#define ROW_EVENT_HAS_COLUMNS 0x0008

/** The table ID used for end of statement row events */
#define TABLE_DUMMY_ID 0x00ffffff

/** A representation of a table map event read from a binary log. A table map
 * maps a table to a unique ID which can be used to match row events to table map
 * events. The table map event tells us how the table is laid out and gives us
 * some meta information on the columns. */
typedef struct table_map
{
    uint64_t id;
    uint64_t columns;
    uint16_t flags;
    uint8_t *column_types;
    char *table;
    char *database;
    const char *gtid; /*< the current GTID event or NULL if GTID is not enabled */
} TABLE_MAP;

TABLE_MAP *table_map_alloc(uint8_t *ptr, uint8_t post_header_len);
void table_map_free(TABLE_MAP *map);

const char* table_type_to_string(uint8_t type);
bool column_is_string_type(uint8_t type);

/** Temporal values */
bool is_temporal_value(uint8_t type);
void unpack_temporal_value(uint8_t type, uint64_t val, struct tm *tm);
void format_temporal_value(char *str, size_t size, uint8_t type, struct tm *tm);

/** Field value extraction */
uint64_t extract_field_value(uint8_t *ptr, uint8_t type, uint64_t* val);


#endif /* MYSQL_BINLOG_H */
