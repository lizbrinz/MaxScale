#ifndef _MAXAVRO_H
#define _MAXAVRO_H

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

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

/** File magic and sync marker sizes block sizes */
#define AVRO_MAGIC_SIZE 4
#define SYNC_MARKER_SIZE 16

/** The file magic */
static const char avro_magic[] = {0x4f, 0x62, 0x6a, 0x01};


typedef struct avro_map_value
{
    char* key;
    char* value;
    struct avro_map_value *next;
} avro_map_value_t;

#define avro_decode(n) ((n >> 1) ^ -(n & 1))

/** Writing primitives */
int avro_encode_char(char n);
int avro_encode_int(int n);
long avro_encode_long(long n);

/** Reading primitives */
bool avro_read_integer(FILE *file, uint64_t *val);
char* avro_read_string(FILE *file);

/** Reading complex types */
avro_map_value_t* avro_read_map(FILE *file);
void avro_free_map(avro_map_value_t *value);

/** Utility functions */
bool avro_read_datablock_start(FILE *file, uint64_t *records, uint64_t *bytes);
bool avro_read_sync(FILE *file, char* sync);
bool avro_read_and_check_sync(FILE *file, char* orig);
FILE* avro_open_file(const char* filename);

#endif
