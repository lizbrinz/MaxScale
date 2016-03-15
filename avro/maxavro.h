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

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

/** File magic and sync marker sizes block sizes */
#define AVRO_MAGIC_SIZE 4
#define SYNC_MARKER_SIZE 16

/** The file magic */
static const char avro_magic[] = {0x4f, 0x62, 0x6a, 0x01};

typedef struct
{
    FILE *file;
    char *schema;
    char sync[SYNC_MARKER_SIZE];
} avro_file_t;

typedef struct
{
    void *buffer; /*< Buffer memory */
    size_t buffersize; /*< Size of the buffer */
    size_t datasize; /*< size of written data */
    uint64_t records; /*< Number of successfully written records */
    avro_file_t *avrofile; /*< The current open file */
} avro_datablock_t;

typedef struct avro_map_value
{
    char* key;
    char* value;
    struct avro_map_value *next;
} avro_map_value_t;

/** Data block generation */
avro_datablock_t* avro_datablock_allocate(avro_file_t *file, size_t buffersize);
void avro_datablock_free(avro_datablock_t* block);
bool avro_datablock_finalize(avro_datablock_t* block);

/** Adding values to a datablock */
bool avro_datablock_add_integer(avro_datablock_t *file, uint64_t val);
bool avro_datablock_add_string(avro_datablock_t *file, const char* str);
bool avro_datablock_add_float(avro_datablock_t *file, float val);
bool avro_datablock_add_double(avro_datablock_t *file, double val);

/** Encoding values in-memory */
uint64_t avro_encode_integer(uint8_t* buffer, uint64_t val);
uint64_t avro_encode_string(uint8_t* dest, const char* str);
uint64_t avro_encode_float(uint8_t* dest, float val);
uint64_t avro_encode_double(uint8_t* dest, double val);

bool avro_write_integer(FILE *file, uint64_t val);
bool avro_write_string(FILE *file, const char* str);
bool avro_write_float(FILE *file, float val);
bool avro_write_double(FILE *file, double val);

/** Reading primitives */
bool avro_read_integer(avro_file_t *file, uint64_t *val);
char* avro_read_string(avro_file_t *file);
bool avro_read_float(avro_file_t *file, float *dest);
bool avro_read_double(avro_file_t *file, double *dest);

/** Reading complex types */
avro_map_value_t* avro_read_map(avro_file_t *file);
void avro_free_map(avro_map_value_t *value);

/** Utility functions */
bool avro_read_datablock_start(avro_file_t *file, uint64_t *records, uint64_t *bytes);
bool avro_read_sync(FILE *file, char* sync);
bool avro_verify_block(avro_file_t *file);
avro_file_t* avro_file_open(const char* filename);
void avro_file_close(avro_file_t *file);
bool avro_file_is_eof(avro_file_t *file);

#endif
