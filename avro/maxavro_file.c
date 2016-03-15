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
#include <errno.h>
#include <string.h>

/** The header metadata is encoded as an Avro map with @c bytes encoded
 * key-value pairs. A @c bytes value is written as a length encoded string
 * where the length of the value is stored as a @c long followed by the
 * actual data. */
static char* read_schema(maxavro_file_t* file)
{
    char *rval = NULL;
    maxavro_map_t* head = avro_map_read(file);
    maxavro_map_t* map = head;

    while (map)
    {
        if (strcmp(map->key, "avro.schema") == 0)
        {
            rval = strdup(map->value);
            break;
        }
        map = map->next;
    }

    avro_map_free(head);
    return rval;
}

/**
 * @brief Open an avro file
 *
 * This function performs checks on the file header and creates an internal
 * representation of the file's schema. This schema can be accessed for more
 * information about the fields.
 * @param filename File to open
 * @return Pointer to opened file or NULL if an error occurred
 */
maxavro_file_t* avro_file_open(const char* filename)
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

    maxavro_file_t* avrofile = calloc(1, sizeof(maxavro_file_t));

    if (avrofile)
    {
        avrofile->file = file;
        char *schema = read_schema(avrofile);
        avrofile->schema = schema ? maxavro_schema_from_json(schema) : NULL;

        if (schema == NULL || avrofile->schema == NULL ||
            !avro_read_sync(file, avrofile->sync))
        {
            free(schema);
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

/**
 * @brief Check if the end of file has been reached
 * @param file File to check
 * @return True if end of file has been reached
 */
bool avro_file_is_eof(maxavro_file_t *file)
{
    return feof(file->file);
}

/**
 * @brief Close an avro file
 * @param file File to close
 */
void avro_file_close(maxavro_file_t *file)
{
    fclose(file->file);
    free(file->schema);
    free(file);
}
