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

#include <mysql_binlog.h>
#include <jansson.h>
#include <stdio.h>
#include <limits.h>
#include <unistd.h>

char* json_schema_from_table_map(TABLE_MAP *map)
{
    json_error_t err;
    json_t *schema = json_object();
    json_object_set_new(schema, "namespace", json_string("MaxScaleChangeDataSchema.avro"));
    json_object_set_new(schema, "type", json_string("record"));
    json_object_set_new(schema, "name", json_string("ChangeRecord"));

    char namebuf[256];
    json_t *array = json_array();
    for (uint64_t i = 0; i < map->columns; i++)
    {
        sprintf(namebuf, "column_%lu", i + 1);
        json_array_append(array, json_pack_ex(&err, 0, "{s:s, s:s}", "name", namebuf, "type", "string"));
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
