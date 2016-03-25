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
#include "skygw_utils.h"
#include <string.h>
#include <skygw_debug.h>
#include <log_manager.h>
#include <errno.h>

bool maxavro_read_datablock_start(MAXAVRO_FILE *file, uint64_t *records,
                                  uint64_t *bytes);
bool maxavro_verify_block(MAXAVRO_FILE *file);

/**
 * @brief Read a single value from a file
 * @param file File to read from
 * @param name Name of the field
 * @param type Type of the field
 * @param field_num Field index in the schema
 * @return JSON object or NULL if an error occurred
 */
static json_t* read_and_pack_value(MAXAVRO_FILE *file, MAXAVRO_SCHEMA_FIELD *field)
{
    json_t* value = NULL;
    switch (field->type)
    {
        case MAXAVRO_TYPE_BOOL:
        {
            int i = 0;
            if (fread(&i, 1, 1, file->file) == 1)
            {
                value = json_pack("b", i);
            }
        }
        break;

        case MAXAVRO_TYPE_INT:
        case MAXAVRO_TYPE_LONG:
        {
            uint64_t val = 0;
            maxavro_read_integer(file, &val);
            json_int_t jsonint = val;
            value = json_pack("I", jsonint);
        }
        break;

        case MAXAVRO_TYPE_ENUM:
        {
            uint64_t val = 0;
            maxavro_read_integer(file, &val);

            json_t *arr = field->extra;
            ss_dassert(arr);
            ss_dassert(json_is_array(arr));

            if (json_array_size(arr) >= val)
            {
                json_t * symbol = json_array_get(arr, val);
                ss_dassert(json_is_string(symbol));
                value = json_pack("s", json_string_value(symbol));
            }
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

        case MAXAVRO_TYPE_BYTES:
        case MAXAVRO_TYPE_STRING:
        {
            char *str = maxavro_read_string(file);
            if (str)
            {
                value = json_string(str);
                free(str);
            }
        }
        break;

        default:
            MXS_ERROR("Unimplemented type: %d", field->type);
            break;
    }
    return value;
}

static void skip_value(MAXAVRO_FILE *file, enum maxavro_value_type type)
{
    switch (type)
    {
        case MAXAVRO_TYPE_INT:
        case MAXAVRO_TYPE_LONG:
        case MAXAVRO_TYPE_ENUM:
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

        case MAXAVRO_TYPE_BYTES:
        case MAXAVRO_TYPE_STRING:
        {
            maxavro_skip_string(file);
        }
        break;

        default:
            MXS_ERROR("Unimplemented type: %d", type);
            break;
    }
}

/**
 * @brief Read a record and convert in into JSON
 *
 * @param file File to read from
 * @return JSON value or NULL if an error occurred. The caller must call
 * json_decref() on the returned value to free the allocated memory.
 */
json_t* maxavro_record_read(MAXAVRO_FILE *file)
{
    json_t* object = NULL;

    if (file->records_read_from_block < file->records_in_block)
    {
        object = json_object();

        if (object)
        {
            for (size_t i = 0; i < file->schema->num_fields; i++)
            {
                json_t* value = read_and_pack_value(file, &file->schema->fields[i]);
                if (value)
                {
                    json_object_set_new(object, file->schema->fields[i].name, value);
                }
                else
                {
                    json_decref(object);
                    return NULL;
                }
            }
        }

        file->records_read_from_block++;
        file->records_read++;
    }

    return object;
}

static void skip_record(MAXAVRO_FILE *file)
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
bool maxavro_next_block(MAXAVRO_FILE *file)
{
    if (file->last_error == MAXAVRO_ERR_NONE)
    {
        uint64_t rec, size;

        if (file->records_read_from_block < file->records_in_block)
        {
            file->records_read += file->records_in_block - file->records_read_from_block;
            long curr_pos = ftell(file->file);
            long offset = (long) file->block_size - (curr_pos - file->data_start_pos);
            fseek(file->file, offset, SEEK_CUR);
        }

        return maxavro_verify_block(file) && maxavro_read_datablock_start(file, &rec, &size);
    }
    return false;
}

/**
 * @brief Seek to a position in the Avro file
 *
 * This moves the current position of the file, skipping data blocks if necessary.
 *
 * @param file
 * @param position
 * @return
 */
bool maxavro_record_seek(MAXAVRO_FILE *file, uint64_t offset)
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
        maxavro_next_block(file);

        while (offset > file->records_in_block)
        {
            /** Skip full blocks that don't have the position we want */
            offset -= file->records_in_block;
            fseek(file->file, file->block_size, SEEK_CUR);
            maxavro_next_block(file);
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

/**
 * @brief Read native Avro data
 *
 * This function reads a complete Avro data block from the disk and returns
 * the read data in its native Avro format.
 *
 * @param file File to read from
 * @return Buffer containing the complete binary data block or NULL if an error
 * occurred. Consult maxavro_get_error for more details.
 */
GWBUF* maxavro_record_read_binary(MAXAVRO_FILE *file)
{
    long data_size = (file->data_start_pos - file->block_start_pos) + file->block_size;
    char err[STRERROR_BUFLEN];
    GWBUF *rval = gwbuf_alloc(data_size);

    if (rval)
    {
        fseek(file->file, file->block_start_pos, SEEK_SET);

        if (fread(GWBUF_DATA(rval), 1, data_size, file->file) == data_size)
        {
            GWBUF *sync = gwbuf_alloc_and_load(SYNC_MARKER_SIZE, file->sync);
            if (sync)
            {
                rval = gwbuf_append(rval, sync);
                maxavro_next_block(file);
            }
        }
        else
        {
            if (ferror(file->file))
            {
                MXS_ERROR("Failed to read %ld bytes: %d, %s", data_size, errno,
                          strerror_r(errno, err, sizeof(err)));
                file->last_error = MAXAVRO_ERR_IO;
            }
            gwbuf_free(rval);
            rval = NULL;
        }
    }
    return rval;
}
