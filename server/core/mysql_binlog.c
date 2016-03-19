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

/**
 * @file mysql_binlog.c - Extracting information from binary logs
 */

#include <mysql_binlog.h>
#include <mysql_utils.h>
#include <stdlib.h>
#include <log_manager.h>
#include <string.h>
#include <skygw_debug.h>
#include <dbusers.h>

/**
 * @brief Extract a table map from a table map event
 *
 * This assumes that the complete event minus the replication header is stored
 * at @p ptr
 * @param ptr Pointer to the start of the event payload
 * @param post_header_len Length of the event specific header, 8 or 6 bytes
 * @return New TABLE_MAP or NULL if memory allocation failed
 */
TABLE_MAP *table_map_alloc(uint8_t *ptr, uint8_t post_header_len)
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
        map->version = 1;
        snprintf(map->version_string, sizeof(map->version_string), "%06d", map->version);
        map->flags = flags;
        map->columns = column_count;
        map->column_types = malloc(column_count);
        map->column_metadata = malloc(metadata_size);
        map->column_metadata_size = metadata_size;
        map->null_bitmap = malloc(nullmap_size);
        map->database = strdup(schema_name);
        map->table = strdup(table_name);
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

/**
 * @brief Rotate a table map
 *
 * @param map Map to rotate
 */
void table_map_rotate(TABLE_MAP *map)
{
    map->version++;
    snprintf(map->version_string, sizeof(map->version_string), "%06d", map->version);
}

/**
 * @brief Convert a table column type to a string
 *
 * @param type The table column type
 * @return The type of the column in human readable format
 * @see lestr_consume
 */
const char* column_type_to_string(uint8_t type)
{
    switch (type)
    {
        case TABLE_COL_TYPE_DECIMAL:
            return "DECIMAL";
        case TABLE_COL_TYPE_TINY:
            return "TINY";
        case TABLE_COL_TYPE_SHORT:
            return "SHORT";
        case TABLE_COL_TYPE_LONG:
            return "LONG";
        case TABLE_COL_TYPE_FLOAT:
            return "FLOAT";
        case TABLE_COL_TYPE_DOUBLE:
            return "DOUBLE";
        case TABLE_COL_TYPE_NULL:
            return "NULL";
        case TABLE_COL_TYPE_TIMESTAMP:
            return "TIMESTAMP";
        case TABLE_COL_TYPE_LONGLONG:
            return "LONGLONG";
        case TABLE_COL_TYPE_INT24:
            return "INT24";
        case TABLE_COL_TYPE_DATE:
            return "DATE";
        case TABLE_COL_TYPE_TIME:
            return "TIME";
        case TABLE_COL_TYPE_DATETIME:
            return "DATETIME";
        case TABLE_COL_TYPE_YEAR:
            return "YEAR";
        case TABLE_COL_TYPE_NEWDATE:
            return "NEWDATE";
        case TABLE_COL_TYPE_VARCHAR:
            return "VARCHAR";
        case TABLE_COL_TYPE_BIT:
            return "BIT";
        case TABLE_COL_TYPE_TIMESTAMP2:
            return "TIMESTAMP2";
        case TABLE_COL_TYPE_DATETIME2:
            return "DATETIME2";
        case TABLE_COL_TYPE_TIME2:
            return "TIME2";
        case TABLE_COL_TYPE_NEWDECIMAL:
            return "NEWDECIMAL";
        case TABLE_COL_TYPE_ENUM:
            return "ENUM";
        case TABLE_COL_TYPE_SET:
            return "SET";
        case TABLE_COL_TYPE_TINY_BLOB:
            return "TINY_BLOB";
        case TABLE_COL_TYPE_MEDIUM_BLOB:
            return "MEDIUM_BLOB";
        case TABLE_COL_TYPE_LONG_BLOB:
            return "LONG_BLOB";
        case TABLE_COL_TYPE_BLOB:
            return "BLOB";
        case TABLE_COL_TYPE_VAR_STRING:
            return "VAR_STRING";
        case TABLE_COL_TYPE_STRING:
            return "STRING";
        case TABLE_COL_TYPE_GEOMETRY:
            return "GEOMETRY";
        default:
            break;
    }
    return "UNKNOWN";
}

bool column_is_blob(uint8_t type)
{
    switch (type)
    {
        case TABLE_COL_TYPE_TINY_BLOB:
        case TABLE_COL_TYPE_MEDIUM_BLOB:
        case TABLE_COL_TYPE_LONG_BLOB:
        case TABLE_COL_TYPE_BLOB:
            return true;
    }
    return false;
}

/**
 * @brief Check if the column is a string type column
 *
 * @param type Type of the column
 * @return True if the column is a string type column
 * @see lestr_consume
 */
bool column_is_variable_string(uint8_t type)
{
    switch (type)
    {
        case TABLE_COL_TYPE_DECIMAL:
        case TABLE_COL_TYPE_VARCHAR:
        case TABLE_COL_TYPE_BIT:
        case TABLE_COL_TYPE_NEWDECIMAL:
        case TABLE_COL_TYPE_VAR_STRING:
        case TABLE_COL_TYPE_GEOMETRY:
            return true;
        default:
            return false;
    }
}

/**
 * Check if a column is of a temporal type
 * @param type Column type
 * @return True if the type is temporal
 */
bool column_is_temporal(uint8_t type)
{
    switch (type)
    {
        case TABLE_COL_TYPE_YEAR:
        case TABLE_COL_TYPE_DATE:
        case TABLE_COL_TYPE_TIME:
        case TABLE_COL_TYPE_DATETIME:
        case TABLE_COL_TYPE_DATETIME2:
        case TABLE_COL_TYPE_TIMESTAMP:
        case TABLE_COL_TYPE_TIMESTAMP2:
            return true;
    }
    return false;
}

/**
 * @brief Check if the column is a string type column
 *
 * @param type Type of the column
 * @return True if the column is a string type column
 * @see lestr_consume
 */
bool column_is_fixed_string(uint8_t type)
{
    return type == TABLE_COL_TYPE_STRING;
}

/**
 * Check if a column is an ENUM or SET
 * @param type Column type
 * @return True if column is either ENUM or SET
 */
bool fixed_string_is_enum(uint8_t type)
{
    return type == TABLE_COL_TYPE_ENUM || type == TABLE_COL_TYPE_SET;
}

/**
 * @brief Unpack a YEAR type
 *
 * The value seems to be stored as an offset from the year 1900
 * @param val Stored value
 * @param dest Destination where unpacked value is stored
 */
static void unpack_year(uint8_t *ptr, struct tm *dest)
{
    memset(dest, 0, sizeof(*dest));
    dest->tm_year = *ptr;
}

#ifdef USE_OLD_DATETIME
/**
 * @brief Unpack a DATETIME
 *
 * The DATETIME is stored as a 8 byte value with the values stored as multiples
 * of 100. This means that the stored value is in the format YYYYMMDDHHMMSS.
 * @param val Value read from the binary log
 * @param dest Pointer where the unpacked value is stored
 */
static void unpack_datetime(uint8_t *ptr, uint8_t decimals, struct tm *dest)
{
    uint32_t second = val - ((val / 100) * 100);
    val /= 100;
    uint32_t minute = val - ((val / 100) * 100);
    val /= 100;
    uint32_t hour = val - ((val / 100) * 100);
    val /= 100;
    uint32_t day = val - ((val / 100) * 100);
    val /= 100;
    uint32_t month = val - ((val / 100) * 100);
    val /= 100;
    uint32_t year = val;

    memset(dest, 0, sizeof(struct tm));
    dest->tm_year = year - 1900;
    dest->tm_mon = month;
    dest->tm_mday = day;
    dest->tm_hour = hour;
    dest->tm_min = minute;
    dest->tm_sec = second;
}
#endif

/**
 * Unpack a 5 byte reverse byte order value
 * @param data pointer to data
 * @return Unpacked value
 */
static inline uint64_t unpack5(uint8_t* data)
{
    uint64_t rval = data[4];
    rval += ((uint64_t)data[3]) << 8;
    rval += ((uint64_t)data[2]) << 16;
    rval += ((uint64_t)data[1]) << 24;
    rval += ((uint64_t)data[0]) << 32;
    return rval;
}

/** The DATETIME values are stored in the binary logs with an offset */
#define DATETIME2_OFFSET 0x8000000000LL

/**
 * @brief Unpack a DATETIME2
 *
 * The DATETIME2 is only used by row based replication in newer MariaDB servers.
 * @param val Value read from the binary log
 * @param dest Pointer where the unpacked value is stored
 */
static void unpack_datetime2(uint8_t *ptr, uint8_t decimals, struct tm *dest)
{
    int64_t unpacked = unpack5(ptr) - DATETIME2_OFFSET;
    if (unpacked < 0)
    {
        unpacked = -unpacked;
    }

    uint64_t date = unpacked >> 17;
    uint64_t yearmonth = date >> 5;
    uint64_t time = unpacked % (1 << 17);

    memset(dest, 0, sizeof(*dest));
    dest->tm_sec = time % (1 << 6);
    dest->tm_min = (time >> 6) % (1 << 6);
    dest->tm_hour = time >> 12;
    dest->tm_mday = date % (1 << 5);
    dest->tm_mon = yearmonth % 13;
    dest->tm_year = yearmonth / 13;
}

/** Unpack a "reverse" byte order value */
#define unpack4(data) (data[3] + (data[2] << 8) + (data[1] << 16) + (data[0] << 24))

/**
 * @brief Unpack a TIMESTAMP
 *
 * The timestamps are stored with the high bytes first
 * @param val The stored value
 * @param dest Destination where the result is stored
 */
static void unpack_timestamp(uint8_t *ptr, uint8_t decimals, struct tm *dest)
{
    time_t t = unpack4(ptr);
    localtime_r(&t, dest);
}

#define unpack3(data) (data[2] + (data[1] << 8) + (data[0] << 16))

/**
 * @brief Unpack a TIME
 *
 * The TIME is stored as a 3 byte value with the values stored as multiples
 * of 100. This means that the stored value is in the format HHMMSS.
 * @param val Value read from the binary log
 * @param dest Pointer where the unpacked value is stored
 */
static void unpack_time(uint8_t *ptr, struct tm *dest)
{
    uint64_t val = unpack3(ptr);
    uint32_t second = val - ((val / 100) * 100);
    val /= 100;
    uint32_t minute = val - ((val / 100) * 100);
    val /= 100;
    uint32_t hour = val;

    memset(dest, 0, sizeof(struct tm));
    dest->tm_hour = hour;
    dest->tm_min = minute;
    dest->tm_sec = second;
}

/**
 * @brief Unpack a DATE value
 * @param ptr Pointer to packed value
 * @param dest Pointer where the unpacked value is stored
 */
static void unpack_date(uint8_t *ptr, struct tm *dest)
{
    uint64_t val = ptr[0] + (ptr[1] << 8) + (ptr[2] << 16);
    memset(dest, 0, sizeof(struct tm));
    dest->tm_mday = val & 31;
    dest->tm_mon = (val >> 5) & 15;
    dest->tm_year = (val >> 9) - 1900;
}

/**
 * @brief Unpack an ENUM or SET field
 * @param ptr Pointer to packed value
 * @param metadata Pointer to field metadata
 * @return Length of the processed field in bytes
 */
uint64_t unpack_enum(uint8_t *ptr, uint8_t *metadata, uint8_t *dest)
{
    memcpy(dest, ptr, metadata[1]);
    return metadata[1];
}

/**
 * @brief Unpack a BIT
 *
 * A part of the BIT values are stored in the NULL value bitmask of the row event.
 * This makes extracting them a bit more complicated since the other fields
 * in the table could have an effect on the location of the stored values.
 *
 * It is possible that the BIT value is fully stored in the NULL value bitmask
 * which means that the actual row data is zero bytes for this field.
 * @param ptr Pointer to packed value
 * @param null_mask NULL field mask
 * @param col_count Number of columns in the row event
 * @param curr_col_index Current position of the field in the row event (zero indexed)
 * @param metadata Field metadata
 * @param dest Destination where the value is stored
 * @return Length of the processed field in bytes
 */
uint64_t unpack_bit(uint8_t *ptr, uint8_t *null_mask, uint32_t col_count,
                    uint32_t curr_col_index, uint8_t *metadata, uint64_t *dest)
{
    if (metadata[1])
    {
        memcpy(ptr, dest, metadata[1]);
    }
    return metadata[1];
}


/**
 * @brief Get the length of a temporal field
 * @param type Field type
 * @param decimals How many decimals the field has
 * @return Number of bytes the temporal value takes
 */
static size_t temporal_field_size(uint8_t type, uint8_t decimals)
{
    switch (type)
    {
        case TABLE_COL_TYPE_YEAR:
            return 1;

        case TABLE_COL_TYPE_TIME:
        case TABLE_COL_TYPE_DATE:
            return 3;

        case TABLE_COL_TYPE_DATETIME:
        case TABLE_COL_TYPE_TIMESTAMP:
            return 4;

        case TABLE_COL_TYPE_TIMESTAMP2:
            return 4 + ((decimals + 1) / 2);

        case TABLE_COL_TYPE_DATETIME2:
            return 5 + ((decimals + 1) / 2);
            
        default:
            MXS_ERROR("Unknown field type: %x %s", type, column_type_to_string(type));
            break;
    }

    return 0;
}

/**
 * @brief Unpack a temporal value
 *
 * MariaDB and MySQL both store temporal values in a special format. This function
 * unpacks them from the storage format and into a common, usable format.
 * @param type Column type
 * @param val Extracted packed value
 * @param tm Pointer where the unpacked temporal value is stored
 */
uint64_t unpack_temporal_value(uint8_t type, uint8_t *ptr, uint8_t *metadata, struct tm *tm)
{
    switch (type)
    {
        case TABLE_COL_TYPE_YEAR:
            unpack_year(ptr, tm);
            break;
        
        case TABLE_COL_TYPE_DATETIME:
            // This is not used with MariaDB RBR
            //unpack_datetime(ptr, *metadata, tm);
            break;

        case TABLE_COL_TYPE_DATETIME2:
            unpack_datetime2(ptr, *metadata, tm);
            break;

        case TABLE_COL_TYPE_TIME:
            unpack_time(ptr, tm);
            break;

        case TABLE_COL_TYPE_DATE:
            unpack_date(ptr, tm);
            break;

        case TABLE_COL_TYPE_TIMESTAMP:
        case TABLE_COL_TYPE_TIMESTAMP2:
            unpack_timestamp(ptr, *metadata, tm);
            break;
    }
    return temporal_field_size(type, *metadata);
}

void format_temporal_value(char *str, size_t size, uint8_t type, struct tm *tm)
{
    const char *format = "";

    switch (type)
    {
        case TABLE_COL_TYPE_DATETIME:
        case TABLE_COL_TYPE_DATETIME2:
        case TABLE_COL_TYPE_TIMESTAMP:
        case TABLE_COL_TYPE_TIMESTAMP2:
            format = "%Y-%m-%d %H:%M:%S";
            break;

        case TABLE_COL_TYPE_TIME:
            format = "%H:%M:%S";
            break;

        case TABLE_COL_TYPE_DATE:
            format = "%Y-%m-%d";
            break;

        case TABLE_COL_TYPE_YEAR:
            format = "%Y";
            break;

        default:
            MXS_ERROR("Unexpected temporal type: %x %s", type, column_type_to_string(type));
            ss_dassert(false);
            break;
    }
    strftime(str, size, format, tm);
}

/**
 * @brief Extract a value from a row event
 *
 * This function extracts a single value from a row event and stores it for
 * further processing. Integer values are usable immediately but temporal
 * values need to be unpacked from the compact format they are stored in.
 * @param ptr Pointer to the start of the field value
 * @param type Column type of the field
 * @param metadata Pointer to the field metadata
 * @param val Destination where the extracted value is stored
 * @return Number of bytes copied
 * @see extract_temporal_value
 */
size_t unpack_numeric_field(uint8_t *src, uint8_t type, uint8_t *metadata, uint8_t *dest)
{
    size_t size = 0;
    switch (type)
    {
        case TABLE_COL_TYPE_LONG:
        case TABLE_COL_TYPE_FLOAT:
            size = 4;
            break;

        case TABLE_COL_TYPE_INT24:
            size = 3;
            break;

        case TABLE_COL_TYPE_LONGLONG:
        case TABLE_COL_TYPE_DOUBLE:
            size = 8;
            break;

        case TABLE_COL_TYPE_SHORT:
            size = 2;
            break;

        case TABLE_COL_TYPE_TINY:
            size = 1;
            break;

        default:
            MXS_ERROR("Bad column type: %x %s", type, column_type_to_string(type));
            break;
    }

    memcpy(dest, src, size);
    return size;
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
