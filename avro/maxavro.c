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
bool avro_read_integer(avro_file_t* file, uint64_t *dest)
{
    uint64_t rval = 0;
    uint8_t nread = 0;
    uint8_t byte;

    do
    {
        if (fread(&byte, sizeof(byte), 1, file->file) != sizeof(byte))
        {
            // TODO: Integrate log_manager
            //char err[STRERROR_BUFSIZE];
            //MXS_ERROR("Failed to read value: %d %s", errno, strerror_r(errno, err, sizeof(err)));
            return false;
        }
        rval |= (byte & 0x7f) << (nread++ * 7);
    }
    while (more_bytes(byte) && nread < MAX_INTEGER_SIZE);

    *dest = avro_decode(rval);
    return true;
}

uint64_t avro_encode_integer(uint8_t* buffer, uint64_t val)
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

bool avro_write_integer(FILE *file, uint64_t val)
{
    uint8_t buffer[MAX_INTEGER_SIZE];
    uint8_t nbytes = avro_encode_integer(buffer, val);
    return fwrite(buffer, 1, nbytes, file) == nbytes;
}

char* avro_read_string(avro_file_t* file)
{
    char *key = NULL;
    uint64_t len;

    if (avro_read_integer(file, &len))
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

uint64_t avro_encode_string(uint8_t* dest, const char* str)
{
    uint64_t slen = strlen(str);
    uint64_t ilen = avro_encode_integer(dest, slen);
    memcpy(dest, str, slen);
    return slen + ilen;
}

bool avro_write_string(FILE *file, const char* str)
{
    uint64_t len = strlen(str);
    return avro_write_integer(file, len) && fwrite(str, 1, len, file) == len;
}

bool avro_read_float(avro_file_t* file, float *dest)
{
    return fread(dest, 1, sizeof(*dest), file->file) == sizeof(*dest);
}

uint64_t avro_encode_float(uint8_t* dest, float val)
{
    memcpy(dest, &val, sizeof(val));
    return sizeof(val);
}

bool avro_write_float(FILE *file, float val)
{
    return fwrite(&val, 1, sizeof(val), file) == sizeof(val);
}

bool avro_read_double(avro_file_t* file, double *dest)
{
    return fread(dest, 1, sizeof(*dest), file->file) == sizeof(*dest);
}

uint64_t avro_encode_double(uint8_t* dest, double val)
{
    memcpy(dest, &val, sizeof(val));
    return sizeof(val);
}

bool avro_write_double(FILE *file, double val)
{
    return fwrite(&val, 1, sizeof(val), file) == sizeof(val);
}

avro_map_value_t* avro_read_map(avro_file_t *file)
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
            if (val && (val->key = avro_read_string(file)) && (val->value = avro_read_string(file)))
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

bool avro_verify_block(avro_file_t *file)
{
    char sync[SYNC_MARKER_SIZE];
    if (fread(sync, 1, SYNC_MARKER_SIZE, file->file) != SYNC_MARKER_SIZE)
    {
        return false;
    }

    if (memcmp(file->sync, sync, SYNC_MARKER_SIZE))
    {
        printf("Sync marker mismatch.\n");
        return false;
    }
    return true;
}

bool avro_read_datablock_start(avro_file_t* file, uint64_t *records, uint64_t *bytes)
{
    return avro_read_integer(file, records) && avro_read_integer(file, bytes);
}

/** The header metadata is encoded as an Avro map with @c bytes encoded
 * key-value pairs. A @c bytes value is written as a length encoded string
 * where the length of the value is stored as a @c long followed by the
 * actual data. */
static char* read_schema(avro_file_t* file)
{
    char *rval = NULL;
    avro_map_value_t* head = avro_read_map(file);
    avro_map_value_t* map = head;

    while (map)
    {
        if (strcmp(map->key, "avro.schema") == 0)
        {
            rval = strdup(map->value);
            break;
        }
        map = map->next;
    }

    avro_free_map(head);
    return rval;
}

avro_file_t* avro_open_file(const char* filename)
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

    if (memcmp(magic, avro_magic, AVRO_MAGIC_SIZE) != 0)
    {
        fclose(file);
        printf("Error: Avro magic marker bytes are not correct.\n");
        return NULL;
    }

    avro_file_t* avrofile = malloc(sizeof(avro_file_t));

    if (avrofile)
    {
        avrofile->file = file;
        avrofile->schema = read_schema(avrofile);
        if (!avrofile->schema || !avro_read_sync(file, avrofile->sync))
        {
            free(avrofile->schema);
            free(avrofile);
            avrofile = NULL;
        }
    }
    else
    {
        fclose(file);
        free(avrofile);
        avrofile = NULL;
    }

    return avrofile;
}

void avro_close_file(avro_file_t *file)
{
    fclose(file->file);
    free(file->schema);
    free(file);
}
