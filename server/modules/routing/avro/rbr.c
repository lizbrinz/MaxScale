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


#include <rbr.h>
#include <mysql_utils.h>
#include <jansson.h>
#include <avro_schema.h>
#include <dbusers.h>
#include <mxs_avro.h>
#include <strings.h>

/**
 * @brief Handle a table map event
 *
 * This converts a table map events into table meta data that will be used when
 * converting binlogs to Avro format.
 * @param router Avro router instance
 * @param hdr Replication header
 * @param ptr Pointer to event payload
 */
void handle_table_map_event(AVRO_INSTANCE *router, REP_HEADER *hdr, uint8_t *ptr)
{
    TABLE_MAP *map = table_map_alloc(ptr, router->event_type_hdr_lens[hdr->event_type]);

    if (map)
    {
        spinlock_acquire(&router->lock);
        TABLE_MAP *old;

        if ((old = hashtable_fetch(router->table_maps, (void*) &map->id)) &&
            old->columns == map->columns &&
            memcmp(old->column_types, map->column_types,
                   MIN(old->columns, map->columns)) == 0)
        {
            table_map_free(map);
        }
        else
        {
            /** New definition for an old table */
            if (old)
            {
                hashtable_delete(router->table_maps, &map->id);
            }
            char table_ident[MYSQL_TABLE_MAXLEN + MYSQL_DATABASE_MAXLEN + 2];
            snprintf(table_ident, sizeof(table_ident), "%s.%s", map->database, map->table);
            TABLE_CREATE* create = hashtable_fetch(router->created_tables, table_ident);
            ss_dassert(create);
            char* json_schema = json_new_schema_from_table(map, create);

            char filepath[PATH_MAX + 1];
            snprintf(filepath, sizeof(filepath), "%s/%s.%s.avro", router->avrodir,
                     table_ident, map->version_string);
            AVRO_TABLE *avro_table = avro_table_alloc(filepath, json_schema);
            strncpy(map->gtid, router->current_gtid, GTID_MAX_LEN);
            hashtable_add(router->table_maps, (void*) &map->id, map);
            hashtable_add(router->open_tables, table_ident, avro_table);
            save_avro_schema(router->avrodir, json_schema, map);
            MXS_DEBUG("%s", json_schema);
            free(json_schema);
        }
        spinlock_release(&router->lock);
    }
}

/**
 * @brief Handle a RBR row event
 *
 * These events contain the changes in the data. This function assumes that full
 * row image is sent in every row event.
 * @param router Avro router instance
 * @param hdr Replication header
 * @param pos
 */
void handle_row_event(AVRO_INSTANCE *router, REP_HEADER *hdr, uint8_t *ptr)
{
    uint8_t *start = ptr;
    uint8_t table_id_size = router->event_type_hdr_lens[hdr->event_type] == 6 ? 4 : 6;
    uint64_t table_id = 0;

    memcpy(&table_id, ptr, table_id_size);
    ptr += table_id_size;

    uint16_t flags = 0;
    memcpy(&flags, ptr, 2);
    ptr += 2;

    if (table_id == TABLE_DUMMY_ID && flags & ROW_EVENT_END_STATEMENT)
    {
        /** This is an dummy event which should release all table maps. Right
         * now we just return without processing the rows. */
        return;
    }

    if (hdr->event_type > DELETE_ROWS_EVENTv1)
    {
        /** Version 2 row event, skip extra data */
        uint16_t extra_len = 0;
        memcpy(&extra_len, ptr, 2);
        ptr += 2 + extra_len;
    }

    uint64_t ncolumns = leint_consume(&ptr);
    uint64_t col_present = 0;
    memcpy(&col_present, ptr, (ncolumns + 7) / 8);
    ptr += (ncolumns + 7) / 8;

    uint64_t col_update = 0;
    if (hdr->event_type == UPDATE_ROWS_EVENTv1 ||
        hdr->event_type == UPDATE_ROWS_EVENTv2)
    {
        memcpy(&col_update, ptr, (ncolumns + 7) / 8);
        ptr += (ncolumns + 7) / 8;
    }

    TABLE_MAP *map = hashtable_fetch(router->table_maps, (void*) &table_id);
    if (map)
    {
        char table_ident[MYSQL_TABLE_MAXLEN + MYSQL_DATABASE_MAXLEN + 2];
        snprintf(table_ident, sizeof(table_ident), "%s.%s", map->database, map->table);
        AVRO_TABLE* table = hashtable_fetch(router->open_tables, table_ident);
        TABLE_CREATE* create = hashtable_fetch(router->created_tables, table_ident);

        if (table && create && ncolumns == map->columns)
        {
            avro_value_t record;
            avro_generic_value_new(table->avro_writer_iface, &record);

            /** Each event has one or more rows in it. The number of rows is not known
             * beforehand so we must continue processing them until we reach the end
             * of the event. */
            int rows = 0;
            while (ptr - start < hdr->event_size - BINLOG_EVENT_HDR_LEN)
            {
                /** Add the current GTID and timestamp */
                avro_value_t field;
                avro_value_get_by_name(&record, "GTID", &field, NULL);
                avro_value_set_string(&field, router->current_gtid);

                avro_value_get_by_name(&record, "timestamp", &field, NULL);
                avro_value_set_int(&field, hdr->timestamp);

                ptr = process_row_event_data(map, create, &record, ptr, col_present, col_update);
                avro_file_writer_append_value(table->avro_file, &record);
                rows++;
            }
            MXS_INFO("Processed %d rows", rows);
            avro_value_decref(&record);
            avro_file_writer_flush(table->avro_file);
        }
        else if (table == NULL)
        {
            MXS_ERROR("Avro datafile to open properly for table %s.%s.", map->database, map->table);
        }
        else if (create == NULL)
        {
            MXS_ERROR("Create table statement for %s.%s was malformed.", map->database, map->table);
        }
        else
        {
            MXS_ERROR("Row event and table map event have different column counts."
                      " Only full row image is currently supported.");
        }
    }
}

void set_numeric_field_value(avro_value_t *field, uint8_t type, uint8_t *metadata, uint8_t *value)
{
    int64_t i = 0;

    switch (type)
    {
        case TABLE_COL_TYPE_TINY:
            i = *value;
            avro_value_set_int(field, i);
            break;

        case TABLE_COL_TYPE_SHORT:
            memcpy(&i, value, 2);
            avro_value_set_int(field, i);
            break;
            
        case TABLE_COL_TYPE_INT24:
            memcpy(&i, value, 3);
            avro_value_set_int(field, i);
            break;

        case TABLE_COL_TYPE_LONG:
            memcpy(&i, value, 4);
            avro_value_set_int(field, i);
            break;

        case TABLE_COL_TYPE_LONGLONG:
            memcpy(&i, value, 8);
            avro_value_set_int(field, i);
            break;

        case TABLE_COL_TYPE_FLOAT:
            memcpy(&i, value, 4);
            avro_value_set_float(field, (float)i);
            break;

        case TABLE_COL_TYPE_DOUBLE:
            memcpy(&i, value, 8);
            avro_value_set_float(field, (double)i);
            break;

        default:
            break;
    }
}

bool column_is_null(uint8_t *ptr, int columns, int current_column)
{
    while (current_column > 8)
    {
        ptr++;
        current_column -= 8;
    }

    return ((*ptr) & (1 << current_column));
}

int get_metadata_len(uint8_t type)
{
    switch (type)
    {
        case TABLE_COL_TYPE_STRING:
        case TABLE_COL_TYPE_VAR_STRING:
        case TABLE_COL_TYPE_VARCHAR:
        case TABLE_COL_TYPE_DECIMAL:
        case TABLE_COL_TYPE_NEWDECIMAL:
        case TABLE_COL_TYPE_ENUM:
        case TABLE_COL_TYPE_SET:
        case TABLE_COL_TYPE_BIT:
            return 2;

        case TABLE_COL_TYPE_LONG:
        case TABLE_COL_TYPE_LONGLONG:
        case TABLE_COL_TYPE_BLOB:
        case TABLE_COL_TYPE_FLOAT:
        case TABLE_COL_TYPE_DOUBLE:
            return 1;

        default:
            return 0;
    }
}

/**
 * @brief Extract the values from a single row  in a row event
 *
 * The newer v1 and v2 row event types have extra information in the row event.
 * This could be processed to see the complete before and after image of the row
 * in question.
 * @param map
 * @param ptr
 * @param ncolumns
 * @param columns_present
 * @param col_update
 */
uint8_t* process_row_event_data(TABLE_MAP *map, TABLE_CREATE *create, avro_value_t *record,
                                uint8_t *ptr, uint64_t columns_present, uint64_t columns_update)
{
    int npresent = 0;
    avro_value_t field;
    long ncolumns = map->columns;
    uint8_t *metadata = map->column_metadata;
    size_t metadata_offset = 0;
    int extra_bits = (((ncolumns + 7) / 8) * 8) - ncolumns;

    /** Skip the null-bitmap */
    uint8_t *null_bitmap = ptr;
    ptr += (ncolumns + 7) / 8;

    for (long i = 0; i < map->columns && npresent < ncolumns; i++)
    {
        avro_value_get_by_name(record, create->column_names[i], &field, NULL);

        if (columns_present & (1 << i))
        {
            npresent++;
            if (column_is_null(null_bitmap, ncolumns, i))
            {
                avro_value_set_null(&field);
            }
            else if (column_is_fixed_string(map->column_types[i]))
            {
                /** ENUM and SET are stored as STRING types with the type stored
                 * in the metadata. */
                if (fixed_string_is_enum(metadata[metadata_offset]))
                {
                    uint8_t val[metadata[metadata_offset + 1]];
                    uint64_t bytes = unpack_enum(ptr, &metadata[metadata_offset], val);
                    char strval[32];

                    /** Right now only ENUMs/SETs with less than 256 values
                     * are printed correctly */
                    snprintf(strval, sizeof(strval), "%hhu", val[0]);
                    avro_value_set_string(&field, strval);
                    ptr += bytes;
                }
                else
                {
                    uint8_t bytes = *ptr;
                    avro_value_set_string_len(&field, (char*)ptr + 1, bytes);
                    ptr += bytes + 1;
                }
            }
            else if (map->column_types[i] == TABLE_COL_TYPE_BIT)
            {
                uint64_t value = 0;
                int width = metadata[metadata_offset] + metadata[metadata_offset + 1] * 8;
                int bits_in_nullmap = MIN(width, extra_bits);
                extra_bits -= bits_in_nullmap;
                width -= bits_in_nullmap;
                size_t bytes = width / 8;
                // TODO: extract the bytes
                avro_value_set_int(&field, value);
                ptr += bytes;
            }
            else if (column_is_variable_string(map->column_types[i]))
            {
                size_t sz;
                char *str = lestr_consume(&ptr, &sz);
                avro_value_set_string_len(&field, str, sz);
            }
            else if (column_is_blob(map->column_types[i]))
            {
                uint8_t bytes = metadata[metadata_offset];
                uint64_t len = 0;
                memcpy(&len, ptr, bytes);
                ptr += bytes;
                avro_value_set_bytes(&field, ptr, len);
                ptr += len;
            }
            else if (column_is_temporal(map->column_types[i]))
            {
                char buf[80];
                struct tm tm;
                ptr += unpack_temporal_value(map->column_types[i], ptr, &metadata[metadata_offset], &tm);
                format_temporal_value(buf, sizeof(buf), map->column_types[i], &tm);
                avro_value_set_string(&field, buf);
            }
            else
            {
                uint8_t lval[16];
                memset(lval, 0, sizeof(lval));
                ptr += unpack_numeric_field(ptr, map->column_types[i],
                                           &metadata[metadata_offset], lval);
                set_numeric_field_value(&field, map->column_types[i], &metadata[metadata_offset], lval);
            }
            ss_dassert(metadata_offset <= map->column_metadata_size);
            metadata_offset += get_metadata_len(map->column_types[i]);
        }
    }

    // TODO: implement update row event processing
    /*
        if (columns_update != 0 && false)
        {
            ptr += (ncolumns + 7) / 8;

            for (long i = 0; i < map->columns && npresent < ncolumns; i++)
            {

                if (columns_update & (1 << i))
                {
                    if (column_is_variable_string(map->column_types[i]))
                    {
                        char *str = lestr_consume_dup(&ptr);
                        free(str);
                    }
                    else
                    {
                        uint64_t lval = 0;
                        ptr += extract_field_value(ptr, map->column_types[i], &lval);
                        if (column_is_temporal(map->column_types[i]))
                        {
                            struct tm tm;
                            unpack_temporal_value(map->column_types[i], lval, &tm);
                        }
                    }
                }
            }
        }
    */
    return ptr;
}
