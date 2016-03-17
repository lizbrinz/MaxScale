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
#include <jansson.h>
#include <string.h>

static const MAXAVRO_SCHEMA_FIELD types[MAXAVRO_TYPE_MAX] =
{
    {"int", MAXAVRO_TYPE_INT},
    {"long", MAXAVRO_TYPE_LONG},
    {"float", MAXAVRO_TYPE_FLOAT},
    {"double", MAXAVRO_TYPE_DOUBLE},
    {"bool", MAXAVRO_TYPE_BOOL},
    {"bytes", MAXAVRO_TYPE_BYTES},
    {"string", MAXAVRO_TYPE_STRING},
    {"null", MAXAVRO_TYPE_NULL},
    {NULL, MAXAVRO_TYPE_UNKNOWN}
};

static enum maxavro_value_type string_to_type(const char *str)
{
    for (int i = 0; types[i].name; i++)
    {
        if (strcmp(str, types[i].name) == 0)
        {
            return types[i].type;
        }
    }
    return MAXAVRO_TYPE_UNKNOWN;
}

static const char* type_to_string(enum maxavro_value_type type)
{
    for (int i = 0; types[i].name; i++)
    {
        if (types[i].type == type)
        {
            return types[i].name;
        }
    }
    return "unknown type";
}

static enum maxavro_value_type unpack_to_type(json_t *object)
{
    enum maxavro_value_type rval = MAXAVRO_TYPE_UNKNOWN;

    if (json_is_object(object))
    {
        json_t *tmp;
        json_unpack(object, "{s:o}", "type", &tmp);
        object = tmp;
    }

    if (json_is_array(object))
    {
        json_t *tmp = json_array_get(object, 0);
        object = tmp;
    }

    if (json_is_string(object))
    {
        char *value;
        json_unpack(object, "s", &value);
        rval = string_to_type(value);
    }

    return rval;
}

/**
 * @brief Create an Avro schema from JSON
 * @param json JSON where the schema is created from
 * @return New schema or NULL if an error occurred
 */
MAXAVRO_SCHEMA* maxavro_schema_from_json(const char* json)
{
    MAXAVRO_SCHEMA* rval = malloc(sizeof(MAXAVRO_SCHEMA));

    if (rval)
    {
        json_error_t err;
        json_t *schema = json_loads(json, 0, &err);

        if (schema)
        {
            json_t *field_arr = NULL;
            json_unpack(schema, "{s:o}", "fields", &field_arr);
            char *dump = json_dumps(field_arr, JSON_PRESERVE_ORDER);
            size_t arr_size = json_array_size(field_arr);
            rval->fields = malloc(sizeof(MAXAVRO_SCHEMA_FIELD) * arr_size);
            rval->num_fields = arr_size;

            for (int i = 0; i < arr_size; i++)
            {
                json_t *object = json_array_get(field_arr, i);
                dump = json_dumps(object, JSON_PRESERVE_ORDER);
                (void) dump;
                char *key;
                json_t *value_obj;

                json_unpack(object, "{s:s s:o}", "name", &key, "type", &value_obj);
                rval->fields[i].name = strdup(key);
                rval->fields[i].type = unpack_to_type(value_obj);
            }

            json_decref(field_arr);
            json_decref(schema);
        }
        else
        {
            printf("Failed to read JSON schema: %s\n", json);
        }
    }
    else
    {
        printf("Memory allocation failed.\n");
    }
    return rval;
}

void maxavro_schema_free(MAXAVRO_SCHEMA* schema)
{
    if (schema)
    {
        for (int i = 0; i < schema->num_fields; i++)
        {
            free(schema->fields[i].name);
        }
        free(schema->fields);
        free(schema);
    }
}
