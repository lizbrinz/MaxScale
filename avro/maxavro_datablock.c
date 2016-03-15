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

#include "maxavro.h"
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

maxavro_datablock_t* avro_datablock_allocate(maxavro_file_t *file, size_t buffersize)
{
    maxavro_datablock_t *datablock = malloc(sizeof(maxavro_datablock_t));

    if (datablock && (datablock->buffer = malloc(buffersize)))
    {
        datablock->buffersize = buffersize;
        datablock->avrofile = file;
        datablock->datasize = 0;
        datablock->records = 0;
    }

    return datablock;
}

void avro_datablock_free(maxavro_datablock_t* block)
{
    if (block)
    {
        free(block->buffer);
        free(block);
    }
}

bool avro_datablock_finalize(maxavro_datablock_t* block)
{
    bool rval = true;
    FILE *file = block->avrofile->file;

    /** Store the current position so we can truncate the file if a write fails */
    long pos = ftell(file);

    if (!avro_write_integer(file, block->records) ||
        !avro_write_integer(file, block->datasize) ||
        fwrite(block->buffer, 1, block->datasize, file) != block->datasize ||
        fwrite(block->avrofile->sync, 1, SYNC_MARKER_SIZE, file) != SYNC_MARKER_SIZE)
    {
        int fd = fileno(file);
        ftruncate(fd, pos);
        fseek(file, 0, SEEK_END);
        rval = false;
    }
    else
    {
        /** The current block is successfully written, reset datablock for
         * a new write. */
        block->buffersize = 0;
        block->records = 0;
    }
    return rval;
}

static bool reallocate_datablock(maxavro_datablock_t *block)
{
    void *tmp = realloc(block->buffer, block->buffersize * 2);
    if (tmp == NULL)
    {
        return false;
    }

    block->buffer = tmp;
    block->buffersize *= 2;
    return true;
}

bool avro_datablock_add_integer(maxavro_datablock_t *block, uint64_t val)
{
    if (block->datasize + 9 >= block->buffersize && !reallocate_datablock(block))
    {
        return false;
    }

    uint64_t added = avro_encode_integer(block->buffer + block->datasize, val);
    block->datasize += added;
    return true;
}

bool avro_datablock_add_string(maxavro_datablock_t *block, const char* str)
{
    if (block->datasize + 9 + strlen(str) >= block->buffersize && !reallocate_datablock(block))
    {
        return false;
    }

    uint64_t added = avro_encode_string(block->buffer + block->datasize, str);
    block->datasize += added;
    return true;
}

bool avro_datablock_add_float(maxavro_datablock_t *block, float val)
{
    if (block->datasize + sizeof(val) >= block->buffersize && !reallocate_datablock(block))
    {
        return false;
    }

    uint64_t added = avro_encode_float(block->buffer + block->datasize, val);
    block->datasize += added;
    return true;
}

bool avro_datablock_add_double(maxavro_datablock_t *block, double val)
{
    if (block->datasize + sizeof(val) >= block->buffersize && !reallocate_datablock(block))
    {
        return false;
    }

    uint64_t added = avro_encode_double(block->buffer + block->datasize, val);
    block->datasize += added;
    return true;
}
