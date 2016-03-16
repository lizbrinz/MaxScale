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
#include <jansson.h>

/** File magic and sync marker sizes block sizes */
#define AVRO_MAGIC_SIZE 4
#define SYNC_MARKER_SIZE 16

/** The file magic */
static const char avro_magic[] = {0x4f, 0x62, 0x6a, 0x01};

enum maxavro_value_type
{
    MAXAVRO_TYPE_UNKNOWN = 0,
    MAXAVRO_TYPE_INT,
    MAXAVRO_TYPE_LONG,
    MAXAVRO_TYPE_FLOAT,
    MAXAVRO_TYPE_DOUBLE,
    MAXAVRO_TYPE_BOOL,
    MAXAVRO_TYPE_STRING,
    MAXAVRO_TYPE_BYTES,
    MAXAVRO_TYPE_NULL,
    MAXAVRO_TYPE_MAX
};

typedef struct
{
    char *name;
    enum maxavro_value_type type;
} maxavro_schema_field_t;

typedef struct
{
    maxavro_schema_field_t *fields;
    size_t num_fields;
} maxavro_schema_t;

enum maxavro_error
{
    MAXAVRO_ERR_NONE,
    MAXAVRO_ERR_IO,
    MAXAVRO_ERR_MEMORY,
    MAXAVRO_ERR_VALUE_OVERFLOW
};

typedef struct
{
    FILE* file;
    maxavro_schema_t* schema;
    uint64_t blocks_read; /*< Total number of data blocks read */
    uint64_t records_read; /*< Total number of records read */
    uint64_t bytes_read; /*< Total number of bytes read */
    uint64_t records_in_block;
    uint64_t records_read_from_block;
    uint64_t bytes_read_from_block;
    uint64_t block_size; /*< Size of the block in bytes */

    /** The position @c ftell returns before the first record is read  */
    long block_start_pos;
    enum maxavro_error last_error; /*< Last error */
    char sync[SYNC_MARKER_SIZE];
} maxavro_file_t;

/** A record field value */
typedef union
{
    uint64_t integer;
    double floating;
    char *string;
    bool boolean;
    void *bytes;
} maxavro_record_value_t;

/** A record value */
typedef struct
{
    maxavro_schema_field_t *field;
    maxavro_record_value_t *value;
    size_t size;
} maxavro_record_t;

typedef struct
{
    void *buffer; /*< Buffer memory */
    size_t buffersize; /*< Size of the buffer */
    size_t datasize; /*< size of written data */
    uint64_t records; /*< Number of successfully written records */
    maxavro_file_t *avrofile; /*< The current open file */
} maxavro_datablock_t;

typedef struct avro_map_value
{
    char* key;
    char* value;
    struct avro_map_value *next;
    struct avro_map_value *tail;
    int blocks; /*< Number of added key-value blocks */
} maxavro_map_t;

/** Data block generation */
maxavro_datablock_t* maxavro_datablock_allocate(maxavro_file_t *file, size_t buffersize);
void maxavro_datablock_free(maxavro_datablock_t* block);
bool maxavro_datablock_finalize(maxavro_datablock_t* block);

/** Adding values to a datablock. The caller must ensure that the inserted
 * values conform to the file schema and that the required amount of fields
 * is added before finalizing the block. */
bool maxavro_datablock_add_integer(maxavro_datablock_t *file, uint64_t val);
bool maxavro_datablock_add_string(maxavro_datablock_t *file, const char* str);
bool maxavro_datablock_add_float(maxavro_datablock_t *file, float val);
bool maxavro_datablock_add_double(maxavro_datablock_t *file, double val);

/** Reading primitives */
bool maxavro_read_integer(maxavro_file_t *file, uint64_t *val);
char* maxavro_read_string(maxavro_file_t *file);
bool maxavro_skip_string(maxavro_file_t* file);
bool maxavro_read_float(maxavro_file_t *file, float *dest);
bool maxavro_read_double(maxavro_file_t *file, double *dest);

/** Reading complex types */
maxavro_map_t* maxavro_map_read(maxavro_file_t *file);
void maxavro_map_free(maxavro_map_t *value);

/** Reading and seeking records */
json_t* maxavro_record_read(maxavro_file_t *file);
bool maxavro_record_seek(maxavro_file_t *file, uint64_t offset);
bool maxavro_next_block(maxavro_file_t *file);

/** File operations */
maxavro_file_t* maxavro_file_open(const char* filename);
void maxavro_file_close(maxavro_file_t *file);
enum maxavro_error maxavro_get_error(maxavro_file_t *file);

/** Schema creation */
maxavro_schema_t* maxavro_schema_from_json(const char* json);
void maxavro_schema_free(maxavro_schema_t* schema);

#endif
