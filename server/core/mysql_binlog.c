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
    memcpy(&table_id, ptr, post_header_len);
    ptr += post_header_len;

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

    TABLE_MAP *map = malloc(sizeof(TABLE_MAP));
    if (map)
    {
        map->id = table_id;
        map->flags = flags;
        map->columns = column_count;
        map->column_types = malloc(column_count);
        map->database = strdup(schema_name);
        map->table = strdup(table_name);
        if (map->column_types && map->database && map->table)
        {
            memcpy(map->column_types, column_types, column_count);
        }
        else
        {
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
void table_map_free(TABLE_MAP *map)
{
    if (map)
    {
        free(map->column_types);
        free(map->database);
        free(map->table);
        free(map);
    }
}

/**
 * @brief Convert a table column type to a string
 *
 * @param type The table column type
 * @return The type of the column in human readable format
 * @see lestr_consume
 */
const char* table_type_to_string(uint8_t type)
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
            MXS_ERROR("Unknown column type: %x", type);
            break;
    }
    return "";
}

/**
 * @brief Check if the column is a string type column
 *
 * @param type Type of the column
 * @return True if the column is a string type column
 * @see lestr_consume
 */
bool column_is_string_type(uint8_t type)
{
    switch (type)
    {
        case TABLE_COL_TYPE_DECIMAL:
        case TABLE_COL_TYPE_VARCHAR:
        case TABLE_COL_TYPE_BIT:
        case TABLE_COL_TYPE_NEWDECIMAL:
        case TABLE_COL_TYPE_ENUM:
        case TABLE_COL_TYPE_SET:
        case TABLE_COL_TYPE_TINY_BLOB:
        case TABLE_COL_TYPE_MEDIUM_BLOB:
        case TABLE_COL_TYPE_LONG_BLOB:
        case TABLE_COL_TYPE_BLOB:
        case TABLE_COL_TYPE_VAR_STRING:
        case TABLE_COL_TYPE_STRING:
        case TABLE_COL_TYPE_GEOMETRY:
            return true;
        default:
            return false;
    }
}

/**
 * @brief Unpack a DATETIME
 *
 * The DATETIME is stored as a 8 byte value with the values stored as multiples
 * of 100. This means that the stored value is in the format YYYYMMDDHHMMSS.
 * @param val Value read from the binary log
 * @param dest Pointer where the unpacked value is stored
 */
static void unpack_datetime(uint64_t val, struct tm *dest)
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

/**
 * @brief Unpack a DATE value
 * @param val Packed value
 * @param dest Pointer where the unpacked value is stored
 */
static void unpack_date(uint64_t val, struct tm *dest)
{
    memset(dest, 0, sizeof(struct tm));
    dest->tm_mday = val & 31;
    dest->tm_mon = (val >> 5) & 15;
    dest->tm_year = (val >> 9) - 1900;
}

/**
 * Check if a column is of a temporal type
 * @param type Column type
 * @return True if the type is temporal
 */
bool is_temporal_value(uint8_t type)
{
    return type == TABLE_COL_TYPE_DATETIME || type == TABLE_COL_TYPE_DATE ||
        type == TABLE_COL_TYPE_TIMESTAMP || type == TABLE_COL_TYPE_TIME;
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
void unpack_temporal_value(uint8_t type, uint64_t val, struct tm *tm)
{
    switch (type)
    {
        case TABLE_COL_TYPE_DATETIME:
            unpack_datetime(val, tm);
            break;

        case TABLE_COL_TYPE_TIME:
            // TODO: add TIME extraction
            memset(tm, 0, sizeof(struct tm));
            break;

        case TABLE_COL_TYPE_DATE:
            unpack_date(val, tm);
            break;

        case TABLE_COL_TYPE_TIMESTAMP:
            // TODO: add TIMESTAMP extraction
            memset(tm, 0, sizeof(struct tm));
            break;
    }
}

/**
 * @brief Extract a value from a row event
 *
 * This function extracts a single value from a row event and stores it for
 * further processing. Integer values are usable immediately but temporal
 * values need to be unpacked from the compact format they are stored in.
 * @param ptr Pointer to the start of the field value
 * @param type Column type of the field
 * @param val The extracted value is stored here
 * @return Number of bytes copied
 * @see extract_temporal_value
 */
size_t extract_field_value(uint8_t *ptr, uint8_t type, uint64_t* val)
{
    switch (type)
    {
        case TABLE_COL_TYPE_LONG:
        case TABLE_COL_TYPE_INT24:
        case TABLE_COL_TYPE_FLOAT:
            memcpy(val, ptr, 4);
            return 4;

        case TABLE_COL_TYPE_LONGLONG:
        case TABLE_COL_TYPE_DOUBLE:
            memcpy(val, ptr, 8);
            return 8;

        case TABLE_COL_TYPE_SHORT:
        case TABLE_COL_TYPE_YEAR:
            memcpy(val, ptr, 2);
            return 2;

        case TABLE_COL_TYPE_TINY:
            memcpy(val, ptr, 1);
            return 1;

            /** The following seem to differ from the MySQL documentation and
             * they are stored as some sort of binary values when tested with
             * MariaDB 10.0.23. The MariaDB source code also mentions that
             * there are differences between various versions.*/
        case TABLE_COL_TYPE_DATETIME:
            memcpy(val, ptr, 8);
            return 8;

        case TABLE_COL_TYPE_TIME:
        case TABLE_COL_TYPE_DATE:
            memcpy(val, ptr, 3);
            return 3;

        case TABLE_COL_TYPE_TIMESTAMP:
            memcpy(val, ptr, 4);
            return 4;

        default:
            MXS_ERROR("Bad column type: %x", type);
            break;
    }
    return 0;
}
