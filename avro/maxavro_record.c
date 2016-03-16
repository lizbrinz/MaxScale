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
#include <skygw_debug.h>

/**
 * @brief Read a single value from a file
 * @param file File to read from
 * @param name Name of the field
 * @param type Type of the field
 * @return JSON object or NULL if an error occurred
 */
static json_t* read_and_pack_value(maxavro_file_t *file, enum maxavro_value_type type)
{
    json_t* value = NULL;
    switch (type)
    {
        case MAXAVRO_TYPE_INT:
        case MAXAVRO_TYPE_LONG:
        {
            uint64_t val = 0;
            maxavro_read_integer(file, &val);
            json_int_t jsonint = val;
            value = json_pack("I", jsonint);
        }
        break;

        case MAXAVRO_TYPE_FLOAT:
        case MAXAVRO_TYPE_DOUBLE:
        {
            double d = 0;
            maxavro_read_double(file, &d);
            value = json_pack("f",  d);
        }
        break;

        case MAXAVRO_TYPE_STRING:
        {
            char *str = maxavro_read_string(file);
            value = json_pack("s", str);
        }
        break;

        default:
            printf("Unimplemented type: %d\n", type);
            break;
    }
    return value;
}

static void skip_value(maxavro_file_t *file, enum maxavro_value_type type)
{
    switch (type)
    {
        case MAXAVRO_TYPE_INT:
        case MAXAVRO_TYPE_LONG:
        {
            uint64_t val = 0;
            maxavro_read_integer(file, &val);
        }
        break;

        case MAXAVRO_TYPE_FLOAT:
        case MAXAVRO_TYPE_DOUBLE:
        {
            double d = 0;
            maxavro_read_double(file, &d);
        }
        break;

        case MAXAVRO_TYPE_STRING:
        {
            maxavro_skip_string(file);
        }
        break;

        default:
            printf("Unimplemented type: %d\n", type);
            break;
    }
}

/**
 * @brief Read a record and convert in into JSON
 *
 * @param file File to read from
 * @return JSON value or NULL if an error occurred
 */
json_t* maxavro_record_read(maxavro_file_t *file)
{
    json_t* object = NULL;

    if (file->records_read_from_block < file->records_in_block)
    {
        object = json_object();

        if (object)
        {
            for (size_t i = 0; i < file->schema->num_fields; i++)
            {
                json_t* value = read_and_pack_value(file, file->schema->fields[i].type);
                if (value)
                {
                    json_object_set(object, file->schema->fields[i].name, value);
                }
            }
        }

        file->records_read_from_block++;
        file->records_read++;
    }

    return object;
}

static void skip_record(maxavro_file_t *file)
{
    for (size_t i = 0; i < file->schema->num_fields; i++)
    {
        skip_value(file, file->schema->fields[i].type);
    }
    file->records_read_from_block++;
    file->records_read++;
}

/**
 * @brief Read next data block
 *
 * This seeks past any unread data from the current block
 * @param file File to read from
 * @return True if reading the next block was successfully read
 */
static bool read_next_block(maxavro_file_t *file)
{
    uint64_t rec, size;

    if (file->records_read_from_block < file->records_in_block)
    {
        long curr_pos = ftell(file->file);
        long offset = (long) file->block_size - (curr_pos - file->block_start_pos);
        fseek(file->file, offset, SEEK_CUR);
    }

    return maxavro_verify_block(file) && maxavro_read_datablock_start(file, &rec, &size);
}

/**
 * @brief Seek to a position in the avro file
 *
 * This moves the current position of the file
 * @param file
 * @param position
 * @return
 */
bool maxavro_record_seek(maxavro_file_t *file, uint64_t offset)
{
    bool rval = true;

    if (offset < file->records_in_block - file->records_read_from_block)
    {
        /** Seek to the end of the block or to the position we want */
        while (offset > 0)
        {
            skip_record(file);
            offset--;
        }
    }
    else
    {
        /** We're seeking past a block boundary */
        offset -= (file->records_in_block - file->records_read_from_block);
        read_next_block(file);

        while (offset > file->records_in_block)
        {
            /** Skip full blocks that don't have the position we want */
            offset -= file->records_in_block;
            fseek(file->file, file->block_size, SEEK_CUR);
            read_next_block(file);
        }

        ss_dassert(offset <= file->records_in_block);

        while (offset > 0)
        {
            skip_record(file);
            offset--;
        }
    }

    return rval;
}
