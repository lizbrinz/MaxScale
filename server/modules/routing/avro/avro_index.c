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
 * @file avro_index.c - GTID to file position index
 *
 * This file contains functions used to store index information
 * about GTID position in an Avro file. Since all records in the Avro file
 * that avrorouter uses contain the common GTID field, we can use it to create
 * an index. This can then be used to speed up retrieval of Avro records by
 * seeking to the offset of the file and reading the record instead of iterating
 * through all the records and looking for a matching record.
 *
 * The index is stored as an SQLite3 database.
 *
 * @verbatim
 * Revision History
 *
 * Date         Who             Description
 * 2/04/2016   Markus Mäkelä   Initial implementation
 *
 * @endverbatim
 */

#include <avrorouter.h>
#include <skygw_debug.h>
#include <glob.h>

#define SQL_SIZE 2048
static const char insert_template[] = "INSERT INTO gtid(domain, server_id, "
                                      "sequence, avrofile, position) values (%lu, %lu, %lu, \"%s\", %ld);";

static void set_gtid(gtid_pos_t *gtid, json_t *row)
{
    json_t *obj = json_object_get(row, avro_sequence);
    ss_dassert(json_is_integer(obj));
    gtid->seq = json_integer_value(obj);

    obj = json_object_get(row, avro_server_id);
    ss_dassert(json_is_integer(obj));
    gtid->server_id = json_integer_value(obj);

    obj = json_object_get(row, avro_domain);
    ss_dassert(json_is_integer(obj));
    gtid->domain = json_integer_value(obj);
}

int index_query_cb(void *data, int rows, char** values, char** names)
{
    for (int i = 0; i < rows; i++)
    {
        if (values[i])
        {
            *((long*) data) = strtol(values[i], NULL, 10);
            return 0;
        }
    }
    return 0;
}

void avro_index_file(AVRO_INSTANCE *router, const char* filename)
{
    MAXAVRO_FILE *file = maxavro_file_open(filename);
    if (file)
    {
        char *name = strrchr(filename, '/');
        ss_dassert(name);

        if (name)
        {
            char sql[SQL_SIZE];
            char *errmsg;
            long pos = -1;
            name++;

            snprintf(sql, sizeof(sql), "SELECT position FROM "INDEX_TABLE_NAME
                     " WHERE filename=\"%s\";", name);
            if (sqlite3_exec(router->sqlite_handle, sql, index_query_cb, &pos, &errmsg) != SQLITE_OK)
            {
                MXS_ERROR("Failed to read last indexed position of file '%s': %s",
                          name, errmsg);
            }
            else if (pos > 0)
            {
                /** Continue from last position */
                maxavro_record_set_pos(file, pos);
            }
            sqlite3_free(errmsg);
            errmsg = NULL;

            do
            {
                json_t *row = maxavro_record_read_json(file);

                if (row)
                {
                    gtid_pos_t gtid;
                    set_gtid(&gtid, row);
                    snprintf(sql, sizeof(sql), insert_template, gtid.domain,
                             gtid.server_id, gtid.seq, name, file->block_start_pos);
                    if (sqlite3_exec(router->sqlite_handle, sql, NULL, NULL,
                                     &errmsg) != SQLITE_OK)
                    {
                        MXS_ERROR("Failed to insert GTID %lu-%lu-%lu for %s "
                                  "into index database: %s", gtid.domain,
                                  gtid.server_id, gtid.seq, name, errmsg);
                    }
                    sqlite3_free(errmsg);
                    errmsg = NULL;
                }
                else
                {
                    break;
                }
            }
            while (maxavro_next_block(file));

            snprintf(sql, sizeof(sql), "INSERT OR REPLACE INTO "INDEX_TABLE_NAME
                     " values (%lu, \"%s\");", file->block_start_pos, name);
            if (sqlite3_exec(router->sqlite_handle, sql, NULL, NULL,
                             &errmsg) != SQLITE_OK)
            {
                MXS_ERROR("Failed to update indexing progress: %s", errmsg);
            }
            sqlite3_free(errmsg);
            errmsg = NULL;
        }
        else
        {
            MXS_ERROR("Malformed filename: %s", filename);
        }

        maxavro_file_close(file);
    }
}

/**
 * @brief Avro file indexing task
 *
 * Builds an index of filenames, GTIDs and positions in the Avro file.
 * This allows all tables that contain a GTID to be fetched in an effiecent
 * manner.
 * @param data The router instance
 */
void avro_update_index(AVRO_INSTANCE* router)
{
    char path[PATH_MAX + 1];
    snprintf(path, sizeof(path), "%s/*.avro", router->avrodir);
    glob_t files;

    if (glob(path, 0, NULL, &files) != GLOB_NOMATCH)
    {
        for (int i = 0; i < files.gl_pathc; i++)
        {
            avro_index_file(router, files.gl_pathv[i]);
        }
    }

    globfree(&files);
}
