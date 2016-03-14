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
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include "maxavro.h"
#include <log_manager.h>
#include <errno.h>

int avro_encode_char(char n)
{
    char a = n << 1;
    char b = n >> 7;
    return a ^ b;
}

int avro_encode_int(int n)
{
    int a = n << 1;
    int b = n >> 31;
    return a ^ b;
}

long avro_encode_long(long n)
{
    long a = n << 1;
    long b = n >> 63;
    return a ^ b;
}

#define more_bytes(b) (b & 0x80)

bool avro_read_integer(FILE *file, uint64_t *dest)
{
    uint64_t rval = 0;
    uint8_t nread = 0;
    uint8_t byte;

    do
    {
        if (fread(&byte, sizeof(byte), 1, file) != sizeof(byte))
        {
            // TODO: Integrate log_manager
            //char err[STRERROR_BUFSIZE];
            //MXS_ERROR("Failed to read value: %d %s", errno, strerror_r(errno, err, sizeof(err)));
            return false;
        }
        rval |= (byte & 0x7f) << (nread++ * 7);
    }
    while(more_bytes(byte) && nread < 10);

    *dest = avro_decode(rval);
    return true;
}

char* avro_read_string(FILE *file)
{
    char *key = NULL;
    uint64_t len;

    if(avro_read_integer(file, &len))
    {
        key = malloc(len + 1);
        if(key)
        {
            if(fread(key, 1, len, file) == len)
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

avro_map_value_t* avro_read_map(FILE *file)
{
        
    avro_map_value_t* rval = NULL;
    uint64_t blocks;

    if (!avro_read_integer(file, &blocks))
    {
        return NULL;
    }

    while (blocks > 0)
    {
        for (long i = 0; i < blocks; i++)
        {
            avro_map_value_t* val = calloc(1, sizeof(avro_map_value_t));
            if(val && (val->key = avro_read_string(file)) && (val->value = avro_read_string(file)))
            {
                val->next = rval;
                rval = val;
            }
            else
            {
                avro_free_map(val);
                avro_free_map(rval);
                return NULL;
            }
        }
        if (!avro_read_integer(file, &blocks))
        {
            avro_free_map(rval);
            return NULL;
        }
    }

    return rval;
}

void avro_free_map(avro_map_value_t *value)
{
    while (value)
    {
        avro_map_value_t* tmp = value;
        value = value->next;
        free(tmp->key);
        free(tmp->value);
        free(tmp);
    }
}

bool avro_read_sync(FILE *file, char* sync)
{
    return fread(sync, 1, SYNC_MARKER_SIZE, file) == SYNC_MARKER_SIZE;
}

bool avro_read_and_check_sync(FILE *file, char* orig)
{
    char sync[SYNC_MARKER_SIZE];
    if (fread(sync, 1, SYNC_MARKER_SIZE, file) != SYNC_MARKER_SIZE)
    {
        return false;
    }

    if(memcmp(orig, sync, SYNC_MARKER_SIZE))
    {
        printf("Sync marker mismatch.\n");
        return false;
    }
    return true;
}

bool avro_read_datablock_start(FILE *file, uint64_t *records, uint64_t *bytes)
{
    return avro_read_integer(file, records) && avro_read_integer(file, bytes);
}

FILE* avro_open_file(const char* filename)
{
    FILE *file = fopen(filename, "rb");
    if (!file)
    {
        printf("Failed to open file '%s': %d, %s", filename, errno, strerror(errno));
        return NULL;
    }

    char magic[AVRO_MAGIC_SIZE];

    if (fread(magic, 1, AVRO_MAGIC_SIZE, file) != AVRO_MAGIC_SIZE)
    {
        fclose(file);
        printf("Failed to read file magic marker from '%s'\n", filename);
        return NULL;
    }
    
    if(memcmp(magic, avro_magic, AVRO_MAGIC_SIZE) != 0)
    {
        fclose(file);
        printf("Error: Avro magic marker bytes are not correct.\n");
        return NULL;
    }
    return file;
}
