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
#include <string.h>
#include <stdbool.h>
#include "maxavro.h"
#include <log_manager.h>
#include <errno.h>

#define MAX_INTEGER_SIZE 10

#define avro_decode(n) ((n >> 1) ^ -(n & 1))
#define encode_long(n) ((n << 1) ^ (n >> 63))
#define more_bytes(b) (b & 0x80)

/**
 * @brief Read an Avro integer
 *
 * The integer lengths are all variable and the last bit in a byte indicates
 * if more bytes belong to the integer value. The real value of the integer is
 * the concatenation of the lowest seven bits of eacy byte. This value is encoded
 * in a zigzag patten i.e. first value is -1, second 1, third -2 and so on.
 * @param file The source FILE handle
 * @param dest Destination where the read value is written
 * @return True if value was read successfully
 */
bool maxavro_read_integer(maxavro_file_t* file, uint64_t *dest)
{
    uint64_t rval = 0;
    uint8_t nread = 0;
    uint8_t byte;
    size_t rdsz;
    do
    {
        if ((rdsz = fread(&byte, sizeof(byte), 1, file->file)) != sizeof(byte))
        {
            // TODO: Integrate log_manager
            if (rdsz != 0)
            {
                char err[200];
                printf("Failed to read value: %d %s\n", errno, strerror_r(errno, err, sizeof(err)));
            }
            return false;
        }
        rval |= (uint64_t)(byte & 0x7f) << (nread++ * 7);
    }
    while (more_bytes(byte) && nread < MAX_INTEGER_SIZE);

    *dest = avro_decode(rval);
    return true;
}

uint64_t maxavro_encode_integer(uint8_t* buffer, uint64_t val)
{
    uint64_t encval = encode_long(val);
    uint8_t nbytes = 0;

    while (more_bytes(encval))
    {
        buffer[nbytes++] = 0x80 | (0x7f & encval);
        encval >>= 7;
    }

    buffer[nbytes++] = encval;
    return nbytes;
}

uint64_t avro_length_integer(uint64_t val)
{
    uint64_t encval = encode_long(val);
    uint8_t nbytes = 0;

    while (more_bytes(encval))
    {
        nbytes++;
        encval >>= 7;
    }

    return nbytes;
}


bool maxavro_write_integer(FILE *file, uint64_t val)
{
    uint8_t buffer[MAX_INTEGER_SIZE];
    uint8_t nbytes = maxavro_encode_integer(buffer, val);
    return fwrite(buffer, 1, nbytes, file) == nbytes;
}

char* maxavro_read_string(maxavro_file_t* file)
{
    char *key = NULL;
    uint64_t len;

    if (maxavro_read_integer(file, &len))
    {
        key = malloc(len + 1);
        if (key)
        {
            if (fread(key, 1, len, file->file) == len)
            {
                key[len] = '\0';
            }
            else
            {
                free(key);
                key = NULL;
            }
        }
    }
    return key;
}

bool maxavro_skip_string(maxavro_file_t* file)
{
    uint64_t len;

    if (maxavro_read_integer(file, &len))
    {
        return fseek(file->file, len, SEEK_CUR) != -1;
    }

    return false;
}

uint64_t maxavro_encode_string(uint8_t* dest, const char* str)
{
    uint64_t slen = strlen(str);
    uint64_t ilen = maxavro_encode_integer(dest, slen);
    memcpy(dest, str, slen);
    return slen + ilen;
}

uint64_t avro_length_string(const char* str)
{
    uint64_t slen = strlen(str);
    uint64_t ilen = avro_length_integer(slen);
    return slen + ilen;
}

bool maxavro_write_string(FILE *file, const char* str)
{
    uint64_t len = strlen(str);
    return maxavro_write_integer(file, len) && fwrite(str, 1, len, file) == len;
}

bool maxavro_read_float(maxavro_file_t* file, float *dest)
{
    return fread(dest, 1, sizeof(*dest), file->file) == sizeof(*dest);
}

uint64_t maxavro_encode_float(uint8_t* dest, float val)
{
    memcpy(dest, &val, sizeof(val));
    return sizeof(val);
}

uint64_t avro_length_float(float val)
{
    return sizeof(val);
}

bool maxavro_write_float(FILE *file, float val)
{
    return fwrite(&val, 1, sizeof(val), file) == sizeof(val);
}

bool maxavro_read_double(maxavro_file_t* file, double *dest)
{
    return fread(dest, 1, sizeof(*dest), file->file) == sizeof(*dest);
}

uint64_t maxavro_encode_double(uint8_t* dest, double val)
{
    memcpy(dest, &val, sizeof(val));
    return sizeof(val);
}
uint64_t avro_length_double(double val)
{
    return sizeof(val);
}

bool maxavro_write_double(FILE *file, double val)
{
    return fwrite(&val, 1, sizeof(val), file) == sizeof(val);
}

maxavro_map_t* maxavro_map_read(maxavro_file_t *file)
{

    maxavro_map_t* rval = NULL;
    uint64_t blocks;

    if (!maxavro_read_integer(file, &blocks))
    {
        return NULL;
    }

    while (blocks > 0)
    {
        for (long i = 0; i < blocks; i++)
        {
            maxavro_map_t* val = calloc(1, sizeof(maxavro_map_t));
            if (val && (val->key = maxavro_read_string(file)) && (val->value = maxavro_read_string(file)))
            {
                val->next = rval;
                rval = val;
            }
            else
            {
                maxavro_map_free(val);
                maxavro_map_free(rval);
                return NULL;
            }
        }
        if (!maxavro_read_integer(file, &blocks))
        {
            maxavro_map_free(rval);
            return NULL;
        }
    }

    return rval;
}

void maxavro_map_free(maxavro_map_t *value)
{
    while (value)
    {
        maxavro_map_t* tmp = value;
        value = value->next;
        free(tmp->key);
        free(tmp->value);
        free(tmp);
    }
}

maxavro_map_t* avro_map_start()
{
    return (maxavro_map_t*)calloc(1, sizeof(maxavro_map_t));
}

uint64_t avro_map_encode(uint8_t *dest, maxavro_map_t* map)
{
    uint64_t len = maxavro_encode_integer(dest, map->blocks);

    while (map)
    {
        len += maxavro_encode_string(dest, map->key);
        len += maxavro_encode_string(dest, map->value);
        map = map->next;
    }

    /** Maps end with an empty block i.e. a zero integer value */
    len += maxavro_encode_integer(dest, 0);
    return len;
}

uint64_t avro_map_length(maxavro_map_t* map)
{
    uint64_t len = avro_length_integer(map->blocks);

    while (map)
    {
        len += avro_length_string(map->key);
        len += avro_length_string(map->value);
        map = map->next;
    }

    len += avro_length_integer(0);
    return len;
}

bool maxavro_read_sync(FILE *file, char* sync)
{
    return fread(sync, 1, SYNC_MARKER_SIZE, file) == SYNC_MARKER_SIZE;
}

bool maxavro_verify_block(maxavro_file_t *file)
{
    char sync[SYNC_MARKER_SIZE];
    int rc = fread(sync, 1, SYNC_MARKER_SIZE, file->file);
    if (rc != SYNC_MARKER_SIZE)
    {
        if (rc == -1)
        {
            printf("Failed to read file: %d %s\n", errno, strerror(errno));
        }
        else
        {
            printf("Short read when reading sync marker. Read %d bytes instead of %d\n",
                   rc, SYNC_MARKER_SIZE);
        }
        return false;
    }

    if (memcmp(file->sync, sync, SYNC_MARKER_SIZE))
    {
        printf("Sync marker mismatch.\n");
        return false;
    }

    /** Increment block count */
    file->blocks_read++;
    return true;
}

bool maxavro_read_datablock_start(maxavro_file_t* file, uint64_t *records, uint64_t *bytes)
{
    bool rval = maxavro_read_integer(file, records) && maxavro_read_integer(file, bytes);

    if (rval)
    {
        file->block_size = *bytes;
        file->records_in_block = *records;
        file->records_read_from_block = 0;
        file->block_start_pos = ftell(file->file);
    }

    return rval;
}
