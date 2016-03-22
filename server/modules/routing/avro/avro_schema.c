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
#include <skygw_debug.h>
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
        case TABLE_COL_TYPE_BIT:
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
 * @brief Create a new JSON Avro schema from the table map and create table abstractions
 *
 * The schema will always have a GTID field and all records contain the current
 * GTID of the transaction.
 * @param map TABLE_MAP for this table
 * @param create The TABLE_CREATE for this table
 * @return New schema or NULL if an error occurred
 */
char* json_new_schema_from_table(TABLE_MAP *map)
{
    TABLE_CREATE *create = map->table_create;

    if (map->version != create->version)
    {
        MXS_ERROR("Version mismatch for table %s.%s. Table map version is %d and "
                  "the table definition version is %d.", map->database, map->table,
                  map->version, create->version);
        return NULL;
    }

    json_error_t err;
    memset(&err, 0, sizeof(err));
    json_t *schema = json_object();
    json_object_set_new(schema, "namespace", json_string("MaxScaleChangeDataSchema.avro"));
    json_object_set_new(schema, "type", json_string("record"));
    json_object_set_new(schema, "name", json_string("ChangeRecord"));

    json_t *array = json_array();
    json_array_append(array, json_pack_ex(&err, 0, "{s:s, s:s}", "name",
                                          "GTID", "type", "string"));
    json_array_append(array, json_pack_ex(&err, 0, "{s:s, s:s}", "name",
                                          "timestamp", "type", "int"));

    /** Enums and other complex types are defined with complete JSON objects
     * instead of string values */
    json_t *event_types = json_pack_ex(&err, 0, "{s:s, s:s, s:[s,s,s,s]}", "type", "enum",
                                       "name", "EVENT_TYPES", "symbols", "insert",
                                       "update_before", "update_after", "delete");

    json_array_append(array, json_pack_ex(&err, 0, "{s:s, s:o}", "name", "event_type",
                                          "type", event_types));

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
    snprintf(filepath, sizeof(filepath), "%s/%s.%s.%06d.avsc", path, map->database,
             map->table, map->version);

    if (access(filepath, F_OK) != 0)
    {
        FILE *file = fopen(filepath, "wb");
        if (file)
        {
            fprintf(file, "%s\n", schema);
            fclose(file);
        }
    }
    else
    {
        MXS_ERROR("Schema version %d already exists: %s", map->version, filepath);
    }
}
