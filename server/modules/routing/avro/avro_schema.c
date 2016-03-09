/*
 * This file is distributed as part of MaxScale.  It is free
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

/**
 * @file avro_schema.c - Avro schema related functions
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <mysql_binlog.h>
#include <jansson.h>
#include <stdio.h>
#include <limits.h>
#include <unistd.h>
#include <log_manager.h>
#include <sys/stat.h>
#include <errno.h>
#include <skygw_utils.h>
#include <string.h>

/**
 * @brief Convert the MySQL column type to a compatible Avro type
 *
 * Some fields are larger than they need to be but since the Avro integer
 * compression is quite efficient, the real loss in performance is negligible.
 * @param type MySQL column type
 * @return String representation of the Avro type
 */
static const char* column_type_to_avro_type(uint8_t type)
{
    switch (type)
    {
        case TABLE_COL_TYPE_DECIMAL:
        case TABLE_COL_TYPE_TINY:
        case TABLE_COL_TYPE_SHORT:
        case TABLE_COL_TYPE_LONG:
        case TABLE_COL_TYPE_INT24:
            return "int";

        case TABLE_COL_TYPE_FLOAT:
            return "float";

        case TABLE_COL_TYPE_DOUBLE:
            return "double";

        case TABLE_COL_TYPE_NULL:
            return "null";

        case TABLE_COL_TYPE_LONGLONG:
            return "long";

        case TABLE_COL_TYPE_TINY_BLOB:
        case TABLE_COL_TYPE_MEDIUM_BLOB:
        case TABLE_COL_TYPE_LONG_BLOB:
        case TABLE_COL_TYPE_BLOB:
            return "bytes";

        default:
            return "string";
    }
}

/**
 * Create a new JSON Avro schema from the table map and create table abstractions
 * @param map TABLE_MAP for this table
 * @param create The TABLE_CREATE for this table
 * @return New schema or NULL if an error occurred
 */
char* json_new_schema_from_table(TABLE_MAP *map, TABLE_CREATE *create)
{
    if (map->columns != create->columns)
    {
        MXS_ERROR("Column count mismatch for table %s.%s.", map->database, map->table);
        return NULL;
    }

    json_error_t err;
    json_t *schema = json_object();
    json_object_set_new(schema, "namespace", json_string("MaxScaleChangeDataSchema.avro"));
    json_object_set_new(schema, "type", json_string("record"));
    json_object_set_new(schema, "name", json_string("ChangeRecord"));

    json_t *array = json_array();
    for (uint64_t i = 0; i < map->columns; i++)
    {
        json_array_append(array, json_pack_ex(&err, 0, "{s:s, s:s}", "name",
                                              create->column_names[i], "type",
                                              column_type_to_avro_type(map->column_types[i])));
    }
    json_object_set_new(schema, "fields", array);
    return json_dumps(schema, JSON_PRESERVE_ORDER);
}

/**
 * @brief Save the Avro schema of a table to disk
 *
 * @param path Schema directory
 * @param schema Schema in JSON format
 * @param map Table map that @p schema represents
 */
void save_avro_schema(const char *path, const char* schema, TABLE_MAP *map)
{
    char filepath[PATH_MAX];
    int i = 1;
    sprintf(filepath, "%s/%s.%s.%06d.avsc", path, map->database, map->table, i);

    while (access(filepath, F_OK) == 0)
    {
        struct stat st;
        bool equal = false;

        if (stat(filepath, &st) == -1)
        {
            char errbuf[STRERROR_BUFLEN];
            MXS_ERROR("Failed to stat file '%s': %d, %s", filepath, errno,
                      strerror_r(errno, errbuf, sizeof(errbuf)));
        }
        else
        {
            char old_schema[st.st_size + 1];
            FILE *oldfile = fopen(filepath, "rb");
            if (oldfile)
            {
                fread(old_schema, 1, sizeof(old_schema), oldfile);
                if (strncmp(old_schema, schema, MIN(sizeof(old_schema), strlen(schema))) == 0)
                {
                    equal = true;
                }
                fclose(oldfile);
            }

            /** Old schema matches the new schema, no need to create a new one */
            if (equal)
            {
                return;
            }
        }
        i++;
        sprintf(filepath, "%s/%s.%s.%06d.avsc", path, map->database, map->table, i);
    }

    FILE *file = fopen(filepath, "wb");
    if (file)
    {
        fprintf(file, "%s\n", schema);
        fclose(file);
    }
}
