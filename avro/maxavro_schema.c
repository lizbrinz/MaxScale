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

static const maxavro_schema_field_t types[AVRO_TYPE_MAX] = {
    {"int", AVRO_TYPE_INT},
    {"long", AVRO_TYPE_LONG},
    {"float", AVRO_TYPE_FLOAT},
    {"double", AVRO_TYPE_DOUBLE},
    {"bool", AVRO_TYPE_BOOL},
    {"bytes", AVRO_TYPE_BYTES},
    {"string", AVRO_TYPE_STRING},
    {"null", AVRO_TYPE_NULL},
    {NULL, AVRO_TYPE_UNKNOWN}
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
    return AVRO_TYPE_UNKNOWN;
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
    enum maxavro_value_type rval = AVRO_TYPE_UNKNOWN;

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
maxavro_schema_t* maxavro_schema_from_json(const char* json)
{
    maxavro_schema_t* rval = malloc(sizeof(maxavro_schema_t));

    if (rval)
    {
        json_error_t err;
        json_t *schema = json_loads(json, 0, &err);
        json_t *field_arr = NULL;
        json_unpack(schema, "{s:o}", "fields", &field_arr);
        char *dump = json_dumps(field_arr, JSON_PRESERVE_ORDER);
        size_t arr_size = json_array_size(field_arr);
        rval->fields = malloc(sizeof(maxavro_schema_field_t) * arr_size);
        rval->size = arr_size;

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

    return rval;
}

void maxavro_schema_free(maxavro_schema_t* schema)
{
    if (schema)
    {
        for (int i = 0; i < schema->size; i++)
        {
            free(schema->fields[i].name);
        }
        free(schema->fields);
        free(schema);
    }
}
