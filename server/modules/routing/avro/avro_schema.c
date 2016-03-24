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

#include <avrorouter.h>
#include <mysql_utils.h>
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
    char* rval = json_dumps(schema, JSON_PRESERVE_ORDER);
    json_decref(schema);
    return rval;
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
    snprintf(filepath, sizeof(filepath), "%s/%s.%s.%06d.avsc", path, map->database,
             map->table, map->version);

    if (access(filepath, F_OK) != 0)
    {
        if (!map->table_create->was_used)
        {
            FILE *file = fopen(filepath, "wb");
            if (file)
            {
                fprintf(file, "%s\n", schema);
                map->table_create->was_used = true;
                fclose(file);
            }
        }
    }
    else
    {
        MXS_NOTICE("Schema version %d already exists: %s", map->version, filepath);
    }
}

/**
 * @brief Persist a CREATE TABLE statement on disk
 * @param create The TABLE_CREATE object
 * @param filename File where the CREATE TABLE statement is appended to
 * @return True if persisting the statement to disk was successful
 */
bool table_create_save(TABLE_CREATE *create, const char *filename)
{
    bool rval = false;
    const char statement_template[] = "CREATE TABLE %s.%s(%s);\n";

    FILE *file = fopen(filename, "ab");
    if (file)
    {
        fprintf(file, statement_template, create->database, create->table, create->table_definition);
        fclose(file);
        rval = true;
    }

    return rval;
}

/**
 * Extract the table definition from a CREATE TABLE statement
 * @param sql The SQL statement
 * @param size Length of the statement
 * @return Pointer to the start of the definition of NULL if the query is
 * malformed.
 */
static const char* get_table_definition(const char *sql, int* size)
{
    const char *rval = NULL;
    const char *ptr = sql;
    const char *end = strchr(sql, '\0');
    while (ptr < end && *ptr != '(')
    {
        ptr++;
    }

    /** We assume at least the parentheses are in the statement */
    if (ptr < end - 2)
    {
        int depth = 0;
        ptr++;
        const char *start = ptr; // Skip first parenthesis
        while (ptr < end)
        {
            switch (*ptr)
            {
                case '(':
                    depth++;
                    break;

                case ')':
                    depth--;
                    break;

                default:
                    break;
            }

            /** We found the last closing parenthesis */
            if (depth < 0)
            {
                *size = ptr - start;
                rval = start;
                break;
            }
            ptr++;
        }
    }

    return rval;
}

/**
 * Extract the table name from a CREATE TABLE statement
 * @param sql SQL statement
 * @param dest Destination where the table name is extracted. Must be at least
 * MYSQL_TABLE_MAXLEN bytes long.
 * @return True if extraction was successful
 */
static bool get_table_name(const char* sql, char* dest)
{
    bool rval = false;
    const char* ptr = strchr(sql, '(');

    if (ptr)
    {
        ptr--;
        while (*ptr == '`' || isspace(*ptr))
        {
            ptr--;
        }

        const char* end = ptr + 1;
        while (*ptr != '`' && *ptr != '.' && !isspace(*ptr))
        {
            ptr--;
        }
        ptr++;
        memcpy(dest, ptr, end - ptr);
        dest[end - ptr] = '\0';
        rval = true;
    }

    return rval;
}

/**
 * Extract the database name from a CREATE TABLE statement
 * @param sql SQL statement
 * @param dest Destination where the database name is extracted. Must be at least
 * MYSQL_DATABASE_MAXLEN bytes long.
 * @return True if extraction was successful
 */
static bool get_database_name(const char* sql, char* dest)
{
    bool rval = false;
    const char* ptr = strchr(sql, '(');

    if (ptr)
    {
        ptr--;
        while (*ptr == '`' || isspace(*ptr))
        {
            ptr--;
        }

        while (*ptr != '`' && *ptr != '.' && !isspace(*ptr))
        {
            ptr--;
        }
        const char* end = ptr;
        ptr--;

        while (*ptr != '`' && *ptr != '.' && !isspace(*ptr))
        {
            ptr--;
        }

        ptr++;
        memcpy(dest, ptr, end - ptr);
        dest[end - ptr] = '\0';
        rval = true;
    }

    return rval;
}

/**
 * Get the start of the next field definition in a CREATE TABLE statement
 * @param sql Table definition
 * @return Start of the next field definition
 */
static const char* get_next_field_def(const char* sql)
{
    int depth = 0;

    while (*sql != '\0')
    {
        switch (*sql)
        {
            case '(':
                depth++;
                break;

            case ')':
                depth--;
                break;

            case ',':
                if (depth == 0)
                {
                    sql++;
                    while (isspace(*sql))
                    {
                        sql++;
                    }
                    return sql;
                }
                break;
        }
        sql++;
    }
    return NULL;
}

const char* get_field_name_start(const char* ptr)
{
    while (isspace(*ptr) || *ptr == '`')
    {
        ptr++;
    }
    return ptr;
}

const char* get_field_name_end(const char* ptr)
{
    while (!isspace(*ptr) && *ptr != '`')
    {
        ptr++;
    }
    return ptr;
}

/**
 * Process a table definition into an array of column names
 * @param nameptr table definition
 * @return Number of processed columns or -1 on error
 */
static int process_column_definition(const char *nameptr, char*** dest)
{
    /** Process columns in groups of 8 */
    size_t chunks = 1;
    const size_t chunk_size = 8;
    int i = 0;
    char **names = malloc(sizeof(char*) * (chunks * chunk_size + 1));

    if (names == NULL)
    {
        MXS_ERROR("Memory allocation failed when trying allocate %ld bytes of memory.",
                  sizeof(char*) * chunks);
        return -1;
    }

    while (nameptr)
    {
        nameptr = get_field_name_start(nameptr);
        if (i >= chunks * chunk_size)
        {
            char **tmp = realloc(names, (++chunks * chunk_size + 1) * sizeof(char*));
            if (tmp == NULL)
            {
                for (int x = 0; x < i; x++)
                {
                    free(names[x]);
                }
                free(names);
                MXS_ERROR("Memory allocation failed when trying allocate %ld bytes of memory.",
                          sizeof(char*) * chunks);
                return -1;
            }
            names = tmp;
        }

        char colname[128 + 1];
        const char *end = get_field_name_end(nameptr);

        if (end)
        {
            snprintf(colname, sizeof(colname), "%.*s", (int) (end - nameptr), nameptr);

            if ((names[i++] = strdup(colname)) == NULL)
            {
                for (int x = 0; x < i; x++)
                {
                    free(names[x]);
                }
                free(names);
                MXS_ERROR("Memory allocation failed when trying allocate %lu bytes of memory.",
                          strlen(colname));
                return -1;
            }
        }

        nameptr = get_next_field_def(nameptr);
    }

    *dest = names;

    return i;
}

/**
 * @brief Handle a query event which contains a CREATE TABLE statement
 * @param sql Query SQL
 * @param db Database where this query was executed
 * @return New CREATE_TABLE object or NULL if an error occurred
 */
TABLE_CREATE* table_create_alloc(const char* sql, const char* event_db,
                                 const char* gtid)
{
    /** Extract the table definition so we can get the column names from it */
    int stmt_len = 0;
    const char* statement_sql = get_table_definition(sql, &stmt_len);
    ss_dassert(statement_sql);
    char table[MYSQL_TABLE_MAXLEN + 1];
    char database[MYSQL_DATABASE_MAXLEN + 1];
    const char *db = event_db;

    MXS_DEBUG("Create table statement: %.*s", stmt_len, statement_sql);

    if (!get_table_name(sql, table))
    {
        MXS_ERROR("Malformed CREATE TABLE statement, could not extract table name: %s", sql);
        return NULL;
    }

    /** The CREATE statement contains the database name */
    if (strlen(db) == 0)
    {
        if (!get_database_name(sql, database))
        {
            MXS_ERROR("Malformed CREATE TABLE statement, could not extract "
                      "database name: %s", sql);
            return NULL;
        }
        db = database;
    }

    char **names = NULL;
    int n_columns = process_column_definition(statement_sql, &names);

    /** We have appear to have a valid CREATE TABLE statement */
    TABLE_CREATE *rval = NULL;
    if (n_columns > 0)
    {
        if ((rval = malloc(sizeof(TABLE_CREATE))))
        {
            if ((rval->table_definition = malloc(stmt_len + 1)))
            {
                memcpy(rval->table_definition, statement_sql, stmt_len);
                rval->table_definition[stmt_len] = '\0';
            }
            rval->version = 1;
            rval->was_used = false;
            rval->column_names = names;
            rval->columns = n_columns;
            rval->database = strdup(db);
            rval->table = strdup(table);
            strncpy(rval->gtid, gtid, sizeof(rval->gtid));
        }

        if (rval == NULL || rval->database == NULL || rval->table == NULL)
        {
            if (rval)
            {
                free(rval->database);
                free(rval->table);
                free(rval);
            }

            for (int i = 0; i < n_columns; i++)
            {
                free(names[i]);
            }

            free(names);
            MXS_ERROR("Memory allocation failed when processing a CREATE TABLE statement.");
            rval = NULL;
        }
    }

    return rval;
}

/**
 * Free a TABLE_CREATE structure
 * @param value Value to free
 */
void* table_create_free(TABLE_CREATE* value)
{
    if (value)
    {
        for (uint64_t i = 0; i < value->columns; i++)
        {
            free(value->column_names[i]);
        }
        free(value->table_definition);
        free(value->column_names);
        free(value->table);
        free(value->database);
        free(value);
    }
    return NULL;
}

static const char* get_next_def(const char* sql, const char* end)
{
    int depth = 0;
    while (sql < end)
    {
        if (*sql == ',' && depth == 0)
        {
            return sql + 1;
        }
        else if (*sql == '(')
        {
            depth++;
        }
        else if (*sql == ')')
        {
            depth--;
        }
        sql++;
    }

    return NULL;
}

static const char* get_tok(const char* sql, int* toklen, const char* end)
{
    const char *start = sql;

    while (isspace(*start))
    {
        start++;
    }

    int len = 0;
    int depth = 0;
    while (start + len < end)
    {
        if (isspace(start[len]) && depth == 0)
        {
            *toklen = len;
            return start;
        }
        else if (start[len] == '(')
        {
            depth++;
        }
        else if (start[len] == ')')
        {
            depth--;
        }

        len++;
    }

    if (len > 0 && start + len <= end)
    {
        *toklen = len;
        return start;
    }

    return NULL;
}

static bool tok_eq(const char *a, const char *b, size_t len)
{
    size_t i = 0;

    while (i < len)
    {
        if (tolower(a[i]) - tolower(b[i]) != 0)
        {
            return false;
        }
        i++;
    }

    return true;
}

void read_alter_identifier(const char *sql, const char *end, char *dest, int size)
{
    int len = 0;
    const char *tok = get_tok(sql, &len, end);
    if (tok && (tok = get_tok(tok + len, &len, end)) && (tok = get_tok(tok + len, &len, end)))
    {
        snprintf(dest, size, "%.*s", len, tok);
    }
}

bool table_create_alter(TABLE_CREATE *create, const char *sql, const char *end)
{
    const char *tbl = strcasestr(sql, "table"), *def;

    if ((def = strchr(tbl, ' ')))
    {
        int len = 0;
        const char *tok = get_tok(def, &len, end);
        const char *table = tok;
        int tbllen = len;

        if (tok)
        {
            MXS_DEBUG("Altering table %.*s\n", len, tok);
            def = tok + len;
        }

        int updates = 0;

        while (tok && (tok = get_tok(tok + len, &len, end)))
        {
            const char *ptok = tok;
            int plen = len;
            tok = get_tok(tok + len, &len, end);

            if (tok)
            {
                if (tok_eq(ptok, "add", plen) && tok_eq(tok, "column", len))
                {
                    tok = get_tok(tok + len, &len, end);

                    char ** tmp = realloc(create->column_names, sizeof(char*) * create->columns + 1);
                    ss_dassert(tmp);

                    if (tmp == NULL)
                    {
                        return false;
                    }

                    create->column_names = tmp;
                    create->column_names[create->columns] = strndup(tok, len);
                    create->columns++;
                    updates++;
                    tok = get_next_def(tok, end);
                    len = 0;
                }
                else if (tok_eq(ptok, "drop", plen) && tok_eq(tok, "column", len))
                {
                    tok = get_tok(tok + len, &len, end);

                    free(create->column_names[create->columns - 1]);
                    char ** tmp = realloc(create->column_names, sizeof(char*) * create->columns - 1);
                    ss_dassert(tmp);

                    if (tmp == NULL)
                    {
                        return false;
                    }

                    create->column_names = tmp;
                    create->columns--;
                    updates++;
                    tok = get_next_def(tok, end);
                    len = 0;
                }
                else if (tok_eq(ptok, "change", plen) && tok_eq(tok, "column", len))
                {
                    tok = get_tok(tok + len, &len, end);
                    free(create->column_names[create->columns - 1]);
                    create->column_names[create->columns - 1] = strndup(tok, len);
                    updates++;
                    tok = get_next_def(tok, end);
                    len = 0;
                }
            }
            else
            {
                break;
            }
        }

        /** Only increment the create version if it has an associated .avro
         * file. The .avro file is only created if it is acutally used. */
        if (updates > 0 && create->was_used)
        {
            create->version++;
            create->was_used = false;
        }
    }

    return true;
}

/**
 * @brief Read the fully qualified name of the table
 *
 * @param ptr Pointer to the start of the event payload
 * @param post_header_len Length of the event specific header, 8 or 6 bytes
 * @param dest Destination where the string is stored
 * @param len Size of destination
 */
void read_table_info(uint8_t *ptr, uint8_t post_header_len, uint64_t *tbl_id, char* dest, size_t len)
{
    uint64_t table_id = 0;
    size_t id_size = post_header_len == 6 ? 4 : 6;
    memcpy(&table_id, ptr, id_size);
    ptr += id_size;

    uint16_t flags = 0;
    memcpy(&flags, ptr, 2);
    ptr += 2;

    uint8_t schema_name_len = *ptr++;
    char schema_name[schema_name_len + 2];

    /** Copy the NULL byte after the schema name */
    memcpy(schema_name, ptr, schema_name_len + 1);
    ptr += schema_name_len + 1;

    uint8_t table_name_len = *ptr++;
    char table_name[table_name_len + 2];

    /** Copy the NULL byte after the table name */
    memcpy(table_name, ptr, table_name_len + 1);
    ptr += table_name_len + 1;

    snprintf(dest, len, "%s.%s", schema_name, table_name);
    *tbl_id = table_id;
}

/**
 * @brief Extract a table map from a table map event
 *
 * This assumes that the complete event minus the replication header is stored
 * at @p ptr
 * @param ptr Pointer to the start of the event payload
 * @param post_header_len Length of the event specific header, 8 or 6 bytes
 * @return New TABLE_MAP or NULL if memory allocation failed
 */
TABLE_MAP *table_map_alloc(uint8_t *ptr, uint8_t hdr_len, TABLE_CREATE* create,
                           const char* gtid)
{
    uint64_t table_id = 0;
    size_t id_size = hdr_len == 6 ? 4 : 6;
    memcpy(&table_id, ptr, id_size);
    ptr += id_size;

    uint16_t flags = 0;
    memcpy(&flags, ptr, 2);
    ptr += 2;

    uint8_t schema_name_len = *ptr++;
    char schema_name[schema_name_len + 2];

    /** Copy the NULL byte after the schema name */
    memcpy(schema_name, ptr, schema_name_len + 1);
    ptr += schema_name_len + 1;

    uint8_t table_name_len = *ptr++;
    char table_name[table_name_len + 2];

    /** Copy the NULL byte after the table name */
    memcpy(table_name, ptr, table_name_len + 1);
    ptr += table_name_len + 1;

    uint64_t column_count = leint_value(ptr);
    ptr += leint_bytes(ptr);

    /** Column types */
    uint8_t *column_types = ptr;
    ptr += column_count;

    size_t metadata_size = 0;
    uint8_t* metadata = (uint8_t*)lestr_consume(&ptr, &metadata_size);
    uint8_t *nullmap = ptr;
    size_t nullmap_size = (column_count + 7) / 8;
    TABLE_MAP *map = malloc(sizeof(TABLE_MAP));

    if (map)
    {
        map->id = table_id;
        map->version = create->version;
        map->flags = flags;
        map->columns = column_count;
        map->column_types = malloc(column_count);
        /** Allocate at least one byte for the metadata */
        map->column_metadata = calloc(1, metadata_size + 1);
        map->column_metadata_size = metadata_size;
        map->null_bitmap = malloc(nullmap_size);
        map->database = strdup(schema_name);
        map->table = strdup(table_name);
        map->table_create = create;
        strncpy(map->gtid, gtid, sizeof(map->gtid));
        if (map->column_types && map->database && map->table &&
            map->column_metadata && map->null_bitmap)
        {
            memcpy(map->column_types, column_types, column_count);
            memcpy(map->null_bitmap, nullmap, nullmap_size);
            memcpy(map->column_metadata, metadata, metadata_size);
        }
        else
        {
            free(map->null_bitmap);
            free(map->column_metadata);
            free(map->column_types);
            free(map->database);
            free(map->table);
            free(map);
            map = NULL;
        }
    }
    else
    {
        free(map);
        map = NULL;
    }

    return map;
}

/**
 * @brief Free a table map
 * @param map Table map to free
 */
void* table_map_free(TABLE_MAP *map)
{
    if (map)
    {
        free(map->column_types);
        free(map->database);
        free(map->table);
        free(map);
    }
    return NULL;
}
