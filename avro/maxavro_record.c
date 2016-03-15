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

/**
 * @brief Read a single value from a file
 * @param file File to read from
 * @param name Name of the field
 * @param type Type of the field
 * @return JSON object or NULL if an error occurred
 */
json_t* read_and_pack_value(maxavro_file_t *file, const char *name,
                            enum maxavro_value_type type)
{
    json_t* value = NULL;
    switch (type)
    {
        case AVRO_TYPE_INT:
        case AVRO_TYPE_LONG:
        {
            uint64_t val = 0;
            avro_read_integer(file, &val);
            json_int_t jsonint = val;
            value = json_pack("{s:I}", name, jsonint);
        }
        break;
        
        case AVRO_TYPE_FLOAT:
        case AVRO_TYPE_DOUBLE:
        {
            double d = 0;
            avro_read_double(file, &d);
            value = json_pack("{s:f}", name, d);
        }
        break;
        
        case AVRO_TYPE_STRING:
        {
            char *str = avro_read_string(file);
            value = json_pack("{s:s}", name, str);
        }
        break;
        
        default:
            printf("Unimplemented type: %s %d\n", name, type);
            break;
    }
    return value;
}

/**
 * @brief Read a record and convert in into JSON
 *
 * @param file File to read from
 * @return JSON value or NULL if an error occurred
 */
json_t* avro_record_read(maxavro_file_t *file)
{
    json_t* array = json_array();

    if (array)
    {

        for (size_t i = 0; i < file->schema->size; i++)
        {
            json_t* value = read_and_pack_value(file, file->schema->fields[i].name,
                                                file->schema->fields[i].type);
            if (value)
            {
                json_array_append(array, value);
            }
        }
    }

    return array;
}
