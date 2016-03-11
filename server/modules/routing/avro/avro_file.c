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
 * @file avro_file.c - File operations for the Avro router
 *
 * This file contains functions that handle the low level file operations for
 * the Avro router. The handling of Avro data files is done via the Avro C API
 * but the handling of MySQL format binary logs is done manually.
 *
 * Parts of this file have been copied from blr_file.c and modified for other
 * uses.
 *
 * @verbatim
 * Revision History
 *
 * Date         Who             Description
 * 25/02/2016   Markus Mäkelä   Initial implementation
 *
 * @endverbatim
 */

#include <binlog_common.h>
#include <sys/stat.h>
#include <mxs_avro.h>
#include <log_manager.h>
#include <fcntl.h>
#include <maxscale_pcre2.h>
#include <rbr.h>
#include <ini.h>

static const char* create_table_regex =
    "(?i)^create[a-z0-9[:space:]_]+table";
static const char *statefile_section = "avro-conversion";

void avro_flush_all_tables(AVRO_INSTANCE *router);

/**
 * Prepare an existing binlog file to be appened to.
 *
 * @param router    The router instance
 * @param file      The binlog file name
 */
bool avro_open_binlog(const char *binlogdir, const char *file, int *dest)
{
    char path[PATH_MAX + 1] = "";
    int fd;

    snprintf(path, sizeof(path), "%s/%s", binlogdir, file);

    if ((fd = open(path, O_RDWR | O_APPEND, 0666)) == -1)
    {
        MXS_ERROR("Failed to open binlog file %s.", path);
        return false;
    }

    if (lseek(fd, BINLOG_MAGIC_SIZE, SEEK_SET) < 4)
    {
        /* If for any reason the file's length is between 1 and 3 bytes
         * then report an error. */
        MXS_ERROR("Binlog file %s has an invalid length.", path);
        close(fd);
        return false;
    }

    *dest = fd;
    return true;
}

void avro_close_binlog(int fd)
{
    close(fd);
}

/**
 *
 * @param table
 * @param filepath
 */
AVRO_TABLE* avro_table_alloc(const char* filepath, const char* json_schema)
{
    AVRO_TABLE *table = calloc(1, sizeof(AVRO_TABLE));
    if (table)
    {
        if (avro_schema_from_json_length(json_schema, strlen(json_schema),
                                         &table->avro_schema))
        {
            MXS_ERROR("Avro error: %s", avro_strerror());
            free(table);
            return NULL;
        }
        int rc = 0;
        if (access(filepath, F_OK) == 0)
        {
            rc = avro_file_writer_open(filepath, &table->avro_file);
        }
        else
        {
            rc = avro_file_writer_create(filepath, table->avro_schema, &table->avro_file);
        }
        if (rc)
        {
            MXS_ERROR("Avro error: %s", avro_strerror());
            avro_schema_decref(table->avro_schema);
            free(table);
            return NULL;
        }

        if ((table->avro_writer_iface = avro_generic_class_from_schema(table->avro_schema)) == NULL)
        {
            MXS_ERROR("Avro error: %s", avro_strerror());
            avro_schema_decref(table->avro_schema);
            avro_file_writer_close(table->avro_file);
            free(table);
            return NULL;
        }
        table->json_schema = strdup(json_schema);
        table->filename = strdup(filepath);
    }
    return table;
}

/**
 * @brief Write a new ini file with current conversion status
 *
 * The file is stored in the cache directory as 'avro-conversion.ini'.
 * @param router Avro router instance
 * @return True if the file was written successfully to disk
 *
 */
static bool avro_save_conversion_state(AVRO_INSTANCE *router)
{
    FILE *config_file;
    char filename[PATH_MAX + 1];
    char err_msg[STRERROR_BUFLEN];

    snprintf(filename, sizeof(filename), "%s/avro-conversion.ini.tmp", router->avrodir);

    /* open file for writing */
    config_file = fopen(filename, "wb");
    if (config_file == NULL)
    {
        MXS_ERROR("Failed to open file '%s': %d, %s", filename,
                  errno, strerror_r(errno, err_msg, sizeof(err_msg)));
        return false;
    }

    fprintf(config_file, "[%s]\n", statefile_section);
    fprintf(config_file, "position=%lu\n", router->current_pos);
    fprintf(config_file, "gtid=%s\n", router->current_gtid);
    fprintf(config_file, "file=%s\n", router->binlog_name);
    fclose(config_file);

    /* rename tmp file to right filename */
    char newname[PATH_MAX + 1];
    snprintf(newname, sizeof(newname), "%s/avro-conversion.ini", router->avrodir);
    int rc = rename(filename, newname);

    if (rc == -1)
    {
        MXS_ERROR("Failed to rename file '%s' to '%s': %d, %s", filename, newname,
                  errno, strerror_r(errno, err_msg, sizeof(err_msg)));
        return false;
    }

    return true;
}

static int conv_state_handler(void* data, const char* section, const char* key, const char* value)
{
    AVRO_INSTANCE *router = (AVRO_INSTANCE*) data;

    if (strcmp(section, statefile_section) == 0)
    {
        if (strcmp(key, "gtid") == 0)
        {
            strncpy(router->current_gtid, value, sizeof(router->current_gtid));
        }
        else if (strcmp(key, "position") == 0)
        {
            router->current_pos = strtol(value, NULL, 10);
        }
        else if (strcmp(key, "file") == 0)
        {
            strncpy(router->binlog_name, value, sizeof(router->binlog_name));
        }
        else
        {
            return 0;
        }
    }

    return 1;
}

/**
 * @brief Load a stored conversion state from file
 * @param router Avro router instance
 * @return True if the stored state was loaded successfully
 */
bool avro_load_conversion_state(AVRO_INSTANCE *router)
{
    char filename[PATH_MAX + 1];
    bool rval = false;

    snprintf(filename, sizeof(filename), "%s/avro-conversion.ini", router->avrodir);

    /** No stored state, this is the first time the router is started */
    if (access(filename, F_OK) == -1)
    {
        return true;
    }

    int rc = ini_parse(filename, conv_state_handler, router);

    switch (rc)
    {
        case 0:
            rval = true;
            MXS_NOTICE("Loaded stored binary log conversion state: File: [%s] Position: [%ld] GTID: [%s]",
                       router->binlog_name, router->current_pos, router->current_gtid);
            break;

        case -1:
            MXS_ERROR("Failed to open file '%s'. ", filename);
            break;

        case -2:
            MXS_ERROR("Failed to allocate enough memory when parsing file '%s'. ", filename);
            break;

        default:
            MXS_ERROR("Failed to parse stored conversion state '%s', error "
                      "on line %d. ", filename, rc);
            break;
    }

    return rval;
}

/**
 * Free an AVRO_TABLE
 * @param table Table to free
 * @return Always NULL
 */
void* avro_table_free(AVRO_TABLE *table)
{
    if (table)
    {
        avro_file_writer_flush(table->avro_file);
        avro_file_writer_close(table->avro_file);
        avro_value_iface_decref(table->avro_writer_iface);
        avro_schema_decref(table->avro_schema);
        free(table->json_schema);
        free(table->filename);
    }
    return NULL;
}

/**
 * @brief Rotate to next file if it exists
 *
 * @param router Avro router instance
 * @param pos Current position, used for logging
 * @param stop_seen If a stop event was seen when processing current file
 * @return AVRO_OK if the next file exists, AVRO_LAST_FILE if this is the last
 * available file.
 */
static avro_binlog_end_t rotate_to_next_file_if_exists(AVRO_INSTANCE* router, uint64_t pos, bool stop_seen)
{
    char next_binlog[BINLOG_FNAMELEN + 1];
    avro_binlog_end_t rval = AVRO_LAST_FILE;

    if (binlog_next_file_exists(router->binlogdir, router->binlog_name))
    {
        snprintf(next_binlog, sizeof(next_binlog),
                 BINLOG_NAMEFMT, router->fileroot,
                 blr_file_get_next_binlogname(router->binlog_name));

        if (stop_seen)
        {
            MXS_NOTICE("End of binlog file [%s] at %lu with a "
                       "close event. Rotating to next binlog file [%s].",
                       router->binlog_name, pos, next_binlog);
        }
        else
        {
            MXS_NOTICE("End of binlog file [%s] at %lu with no "
                       "close or rotate event. Rotating to next binlog file [%s].",
                       router->binlog_name, pos, next_binlog);
        }

        rval = AVRO_OK;
        strncpy(router->binlog_name, next_binlog, sizeof(router->binlog_name));
        router->binlog_position = 4;
        router->current_pos = 4;
    }
    else if (stop_seen)
    {
        MXS_NOTICE("End of binlog file [%s] at %lu with a close event. "
                   "Next binlog file does not exist, pausing file conversion.",
                   router->binlog_name, pos);
    }

    return rval;
}

/**
 * @brief Rotate to a specific file
 *
 * This rotates the current binlog file being processed to a specific file.
 * Currently this is only used to rotate to files that rotate events point to.
 * @param router Avro router instance
 * @param pos Current position, only used for logging
 * @param next_binlog The next file to rotate to
 */
static void rotate_to_file(AVRO_INSTANCE* router, uint64_t pos, const char *next_binlog)
{
    /** Binlog file is processed, prepare for next one */
    MXS_NOTICE("End of binlog file [%s] at %lu. Rotating to file [%s].",
               router->binlog_name, pos, next_binlog);
    strncpy(router->binlog_name, next_binlog, sizeof(router->binlog_name));
    router->binlog_position = 4;
    router->current_pos = 4;
}

static GWBUF* read_event_data(AVRO_INSTANCE *router, REP_HEADER* hdr, uint64_t pos)
{
    GWBUF* result;
    /* Allocate a GWBUF for the event */
    if ((result = gwbuf_alloc(hdr->event_size - BINLOG_EVENT_HDR_LEN)))
    {
        uint8_t *data = GWBUF_DATA(result);
        int n = pread(router->binlog_fd, data, hdr->event_size - BINLOG_EVENT_HDR_LEN,
                      pos + BINLOG_EVENT_HDR_LEN);

        if (n != hdr->event_size - BINLOG_EVENT_HDR_LEN)
        {
            if (n == -1)
            {
                char err_msg[STRERROR_BUFLEN];
                MXS_ERROR("Error reading the event at %lu in %s. "
                          "%s, expected %d bytes.",
                          pos, router->binlog_name,
                          strerror_r(errno, err_msg, sizeof(err_msg)),
                          hdr->event_size - BINLOG_EVENT_HDR_LEN);
            }
            else
            {
                MXS_ERROR("Short read when reading the event at %lu in %s. "
                          "Expected %d bytes got %d bytes.",
                          pos, router->binlog_name,
                          hdr->event_size - BINLOG_EVENT_HDR_LEN, n);
            }
            gwbuf_free(result);
            result = NULL;
        }
    }
    else
    {
        MXS_ERROR("Failed to allocate memory for binlog entry, "
                  "size %d at %lu.",
                  hdr->event_size, pos);
    }
    return result;
}

/**
 * Read all replication events from a binlog file.
 *
 * Routine detects errors and pending transactions
 *
 * @param router        The router instance
 * @param fix           Whether to fix or not errors
 * @param debug         Whether to enable or not the debug for events
 * @return              How the binlog was closed
 * @see enum avro_binlog_end
 */
avro_binlog_end_t avro_read_all_events(AVRO_INSTANCE *router)
{
    uint8_t hdbuf[BINLOG_EVENT_HDR_LEN];
    GWBUF *result;
    unsigned long long pos = router->current_pos;
    unsigned long long last_known_commit = 4;
    char next_binlog[BINLOG_FNAMELEN + 1];
    REP_HEADER hdr;
    int pending_transaction = 0;
    int n;
    int db_name_len;
    uint8_t *ptr;
    int var_block_len;
    bool found_chksum = false;

    /** For statistics */
    unsigned long events = 0;
    unsigned long total_bytes = 0;
    unsigned long event_bytes = 0;
    unsigned long max_bytes = 0;

    bool rotate_seen = false;
    bool stop_seen = false;

    if (router->binlog_fd == -1)
    {
        MXS_ERROR("Current binlog file %s is not open", router->binlog_name);
        return AVRO_BINLOG_ERROR;
    }

    while (1)
    {
        /* Read the header information from the file */
        if ((n = pread(router->binlog_fd, hdbuf, BINLOG_EVENT_HDR_LEN, pos)) != BINLOG_EVENT_HDR_LEN)
        {
            switch (n)
            {
                case 0:
                    break;
                case -1:
                {
                    char err_msg[BLRM_STRERROR_R_MSG_SIZE + 1] = "";
                    strerror_r(errno, err_msg, BLRM_STRERROR_R_MSG_SIZE);
                    MXS_ERROR("Failed to read binlog file %s at position %llu"
                              " (%s).", router->binlog_name,
                              pos, err_msg);

                    if (errno == EBADF)
                        MXS_ERROR("Bad file descriptor in read binlog for file %s"
                                  ", descriptor %d.",
                                  router->binlog_name, router->binlog_fd);
                    break;
                }
                default:
                    MXS_ERROR("Short read when reading the header. "
                              "Expected 19 bytes but got %d bytes. "
                              "Binlog file is %s, position %llu",
                              n, router->binlog_name, pos);
                    break;
            }

            router->current_pos = pos;

            if (pending_transaction > 0)
            {
                MXS_ERROR("Binlog '%s' ends at position %lu and has an incomplete transaction at %lu. "
                          "Stopping file conversion.", router->binlog_name,
                          router->current_pos, router->binlog_position);
                return AVRO_OPEN_TRANSACTION;
            }
            else
            {
                /* any error */
                if (n != 0)
                {
                    return AVRO_BINLOG_ERROR;
                }
                else
                {
                    if (rotate_seen)
                    {
                        rotate_to_file(router, pos, next_binlog);
                        return AVRO_OK;
                    }
                    else
                    {
                        return rotate_to_next_file_if_exists(router, pos, stop_seen);
                    }
                }
            }
        }

        /* fill replication header struct */
        hdr.timestamp = EXTRACT32(hdbuf);
        hdr.event_type = hdbuf[4];
        hdr.serverid = EXTRACT32(&hdbuf[5]);
        hdr.event_size = extract_field(&hdbuf[9], 32);
        hdr.next_pos = EXTRACT32(&hdbuf[13]);
        hdr.flags = EXTRACT16(&hdbuf[17]);

        /* Check event type against MAX_EVENT_TYPE */

        if (hdr.event_type > MAX_EVENT_TYPE_MARIADB10)
        {
            MXS_ERROR("Invalid MariaDB 10 event type 0x%x. "
                      "Binlog file is %s, position %llu",
                      hdr.event_type, router->binlog_name, pos);
            router->binlog_position = last_known_commit;
            router->current_pos = pos;
            return AVRO_BINLOG_ERROR;
        }

        if (hdr.event_size <= 0)
        {
            MXS_ERROR("Event size error: "
                      "size %d at %llu.",
                      hdr.event_size, pos);

            router->binlog_position = last_known_commit;
            router->current_pos = pos;
            return AVRO_BINLOG_ERROR;
        }

        if ((result = read_event_data(router, &hdr, pos)) == NULL)
        {
            router->binlog_position = last_known_commit;
            router->current_pos = pos;
            MXS_WARNING("an error has been found. "
                        "Setting safe pos to %lu, current pos %lu",
                        router->binlog_position, router->current_pos);
            return AVRO_BINLOG_ERROR;
        }

        /* check for pending transaction */
        if (pending_transaction == 0)
        {
            last_known_commit = pos;
        }

        /* get event content */
        ptr = GWBUF_DATA(result);

        /* check for FORMAT DESCRIPTION EVENT */
        if (hdr.event_type == FORMAT_DESCRIPTION_EVENT)
        {
            int event_header_length;
            int event_header_ntypes;
            int n_events;
            int check_alg;
            uint8_t *checksum;

            /** Extract the event header lengths */
            event_header_length = ptr[2 + 50 + 4];
            event_header_ntypes = hdr.event_size - event_header_length - (2 + 50 + 4 + 1);
            memcpy(router->event_type_hdr_lens, ptr + 2 + 50 + 5, event_header_ntypes);
            router->event_types = event_header_ntypes;

            switch (event_header_ntypes)
            {
                case 168: /* mariadb 10 LOG_EVENT_TYPES*/
                    event_header_ntypes -= 163;
                    break;

                case 165: /* mariadb 5 LOG_EVENT_TYPES*/
                    event_header_ntypes -= 160;
                    break;

                default: /* mysql 5.6 LOG_EVENT_TYPES = 35 */
                    event_header_ntypes -= 35;
                    break;
            }

            n_events = hdr.event_size - event_header_length - (2 + 50 + 4 + 1);

            if (event_header_ntypes < n_events)
            {
                checksum = ptr + hdr.event_size - event_header_length - event_header_ntypes;
                if (checksum[0] == 1)
                {
                    found_chksum = true;
                }
            }
        }
        /* Decode CLOSE/STOP Event */
        else if (hdr.event_type == STOP_EVENT)
        {
            char next_file[BLRM_BINLOG_NAME_STR_LEN + 1];
            stop_seen = true;
            snprintf(next_file, sizeof(next_file), BINLOG_NAMEFMT, router->fileroot,
                     blr_file_get_next_binlogname(router->binlog_name));
        }
        else if (hdr.event_type == TABLE_MAP_EVENT)
        {
            handle_table_map_event(router, &hdr, ptr);
        }
        else if ((hdr.event_type >= WRITE_ROWS_EVENTv0 && hdr.event_type <= DELETE_ROWS_EVENTv1) ||
                 (hdr.event_type >= WRITE_ROWS_EVENTv2 && hdr.event_type <= DELETE_ROWS_EVENTv2))
        {
            handle_row_event(router, &hdr, ptr);
        }
        /* Decode ROTATE EVENT */
        else if (hdr.event_type == ROTATE_EVENT)
        {
            int len = hdr.event_size - BINLOG_EVENT_HDR_LEN - 8;

            if (found_chksum)
            {
                len -= 4;
            }

            if (len > BINLOG_FNAMELEN)
            {
                MXS_WARNING("Truncated binlog name from %d to %d characters.",
                            len, BINLOG_FNAMELEN);
                len = BINLOG_FNAMELEN;
            }

            memcpy(next_binlog, ptr + 8, len);
            next_binlog[len] = 0;
            rotate_seen = true;

        }
        else if (hdr.event_type == MARIADB10_GTID_EVENT)
        {
            uint64_t n_sequence; /* 8 bytes */
            uint32_t domainid; /* 4 bytes */
            unsigned int flags; /* 1 byte */
            n_sequence = extract_field(ptr, 64);
            domainid = extract_field(ptr + 8, 32);
            flags = *(ptr + 8 + 4);
            snprintf(router->current_gtid, sizeof(router->current_gtid), "%u-%u-%lu", domainid,
                     hdr.serverid, n_sequence);
            if (flags == 0)
            {
                pending_transaction++;
                // TODO: Handle GTID transactions
                if (pending_transaction > 0)
                {
                    MXS_ERROR("In binlog file '%s' at position %llu: Missing XID Event before GTID Event.",
                              router->binlog_name, pos);
                }
            }
        }
        /**
         * Check QUERY_EVENT
         *
         * Check for BEGIN ( ONLY for mysql 5.6, mariadb 5.5 )
         * Check for COMMIT (not transactional engines)
         */
        else if (hdr.event_type == QUERY_EVENT)
        {
            char *statement_sql;
            db_name_len = ptr[4 + 4];
            var_block_len = ptr[4 + 4 + 1 + 2];
            const int post_header_len = 4 + 4 + 1 + 2 + 2;
            int statement_len = hdr.event_size - BINLOG_EVENT_HDR_LEN - (post_header_len + var_block_len + 1 +
                                                                         db_name_len);
            statement_sql = malloc(statement_len + 1);
            strncpy(statement_sql, (char *) ptr + 4 + 4 + 1 + 2 + 2 + var_block_len + 1 + db_name_len, statement_len);
            statement_sql[statement_len] = '\0';

            /** Very simple detection of CREATE TABLE statements */
            int reg_err = 0;

            if (mxs_pcre2_simple_match(create_table_regex, statement_sql, 0, &reg_err) == MXS_PCRE2_MATCH)
            {
                char db[db_name_len + 1];
                strncpy(db, (char*) ptr + post_header_len + var_block_len, sizeof(db));
                TABLE_CREATE *created = table_create_alloc(statement_sql, db, router->current_gtid);

                if (created)
                {
                    char createlist[PATH_MAX + 1];
                    snprintf(createlist, sizeof(createlist), "%s/table-ddl.list", router->avrodir);
                    if (!table_create_save(created, createlist))
                    {
                        MXS_ERROR("Failed to store CREATE TABLE statement to disk: %s",
                                  statement_sql);
                    }

                    char table_ident[MYSQL_TABLE_MAXLEN + MYSQL_DATABASE_MAXLEN + 2];
                    snprintf(table_ident, sizeof(table_ident), "%s.%s", created->database, created->table);

                    spinlock_acquire(&router->lock);
                    TABLE_CREATE *old = hashtable_fetch(router->created_tables, table_ident);
                    if (old)
                    {
                        hashtable_delete(router->created_tables, table_ident);
                    }
                    hashtable_add(router->created_tables, table_ident, created);
                    spinlock_release(&router->lock);
                }
            }
            /* A transaction starts with this event */
            if (strncmp(statement_sql, "BEGIN", 5) == 0)
            {
                // TODO: Handle BEGIN
                if (pending_transaction > 0)
                {
                    MXS_ERROR("In binlog file '%s' at position %llu: Missing COMMIT before BEGIN.",
                              router->binlog_name, pos);
                }
                pending_transaction++;
            }

            /* Commit received for non transactional tables, i.e. MyISAM */
            if (strncmp(statement_sql, "COMMIT", 6) == 0)
            {
                // TODO: Handle COMMIT
                pending_transaction--;
            }
            free(statement_sql);

        }
        else if (hdr.event_type == XID_EVENT)
        {
            // TODO: Handle XID Event
            avro_flush_all_tables(router);
            avro_save_conversion_state(router);
            pending_transaction--;
        }

        gwbuf_free(result);

        /* pos and next_pos sanity checks */
        if (hdr.next_pos > 0 && hdr.next_pos < pos)
        {
            MXS_INFO("Binlog %s: next pos %u < pos %llu, truncating to %llu",
                     router->binlog_name,
                     hdr.next_pos,
                     pos,
                     pos);

            break;
        }

        if (hdr.next_pos > 0 && hdr.next_pos != (pos + hdr.event_size))
        {
            MXS_INFO("Binlog %s: next pos %u != (pos %llu + event_size %u), truncating to %llu",
                     router->binlog_name,
                     hdr.next_pos,
                     pos,
                     hdr.event_size,
                     pos);

            break;
        }

        /* set pos to new value */
        if (hdr.next_pos > 0)
        {

            if (pending_transaction)
            {
                total_bytes += hdr.event_size;
                event_bytes += hdr.event_size;

                if (event_bytes > max_bytes)
                {
                    max_bytes = event_bytes;
                }
            }

            pos = hdr.next_pos;
            router->current_pos = pos;
        }
        else
        {

            MXS_ERROR("Current event type %d @ %llu has nex pos = %u : exiting",
                      hdr.event_type, pos, hdr.next_pos);
            break;
        }

        events++;
    }

    return AVRO_BINLOG_ERROR;
}

/**
 * @brief Load stored CREATE TABLE statements from file
 * @param router Avro router instance
 * @return True on success
 */
bool avro_load_created_tables(AVRO_INSTANCE *router)
{
    bool rval = false;
    char createlist[PATH_MAX + 1];
    snprintf(createlist, sizeof(createlist), "%s/table-ddl.list", router->avrodir);
    struct stat st;

    if (stat(createlist, &st) == 0)
    {
        size_t len = st.st_size;
        char* buffer = malloc(len + 1);
        FILE *file = fopen(createlist, "rb");

        if (file)
        {
            if (fread(buffer, 1, len, file) == len)
            {
                buffer[len] = '\0';
                char *saveptr;
                char *tok = strtok_r(buffer, "\n", &saveptr);
                rval = true;

                while (tok)
                {
                    int reg_err;
                    if (mxs_pcre2_simple_match(create_table_regex, tok, 0, &reg_err) == MXS_PCRE2_MATCH)
                    {
                        TABLE_CREATE *created = table_create_alloc(tok, "", router->current_gtid);

                        if (created)
                        {
                            char table_ident[MYSQL_TABLE_MAXLEN + MYSQL_DATABASE_MAXLEN + 2];
                            snprintf(table_ident, sizeof(table_ident), "%s.%s", created->database, created->table);

                            if (hashtable_fetch(router->created_tables, table_ident))
                            {
                                hashtable_delete(router->created_tables, table_ident);
                            }
                            hashtable_add(router->created_tables, table_ident, created);
                        }
                        else
                        {
                            rval = false;
                            break;
                        }
                    }
                    tok = strtok_r(NULL, "\n", &saveptr);
                }
            }

            fclose(file);
        }
    }
    return rval;
}

/**
 * @brief Flush all Avro records to disk
 * @param router Avro router instance
 */
void avro_flush_all_tables(AVRO_INSTANCE *router)
{
    HASHITERATOR *iter = hashtable_iterator(router->open_tables);

    if (iter)
    {
        char *key;
        while ((key = (char*)hashtable_next(iter)))
        {
            AVRO_TABLE *table = hashtable_fetch(router->open_tables, key);

            if (table)
            {
                avro_file_writer_flush(table->avro_file);
            }
        }
        hashtable_iterator_free(iter);
    }
}
