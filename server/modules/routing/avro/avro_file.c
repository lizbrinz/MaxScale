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

static const char* create_table_regex =
    "(?i)create[[:space:]]+table([[:space:]]+if[[:space:]]+not[[:space:]]+exists)?[(]";

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
        MXS_ERROR("Failed to open binlog file %s for append.", path);
        return false;
    }

    if ( lseek(fd, BINLOG_MAGIC_SIZE, SEEK_SET) < 4)
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
avro_binlog_end_t avro_read_events_all_events(AVRO_INSTANCE *router)
{
    unsigned long filelen = 0;
    struct stat statb;
    uint8_t hdbuf[BINLOG_EVENT_HDR_LEN];
    uint8_t *data;
    GWBUF *result;
    unsigned long long pos = 4;
    unsigned long long last_known_commit = 4;
    char next_binlog[BINLOG_FNAMELEN + 1];
    REP_HEADER hdr;
    int pending_transaction = 0;
    int n;
    int db_name_len;
    uint8_t *ptr;
    int var_block_len;
    int statement_len;
    int found_chksum = 0;
    unsigned long transaction_events = 0;
    unsigned long total_bytes = 0;
    unsigned long event_bytes = 0;
    unsigned long max_bytes = 0;
    BINLOG_EVENT_DESC first_event;
    BINLOG_EVENT_DESC last_event;
    BINLOG_EVENT_DESC fde_event;
    int fde_seen = 0;
    bool rotate_seen = false;
    bool stop_seen = false;

    memset(&first_event, '\0', sizeof(first_event));
    memset(&last_event, '\0', sizeof(last_event));
    memset(&fde_event, '\0', sizeof(fde_event));

    if (router->binlog_fd == -1)
    {
        MXS_ERROR("Current binlog file %s is not open", router->binlog_name);
        return AVRO_BINLOG_ERROR;
    }

    if (fstat(router->binlog_fd, &statb) == 0)
    {
        filelen = statb.st_size;
    }

    router->current_pos = 4;
    router->binlog_position = 4;

    while (1)
    {
        /* Read the header information from the file */
        if ((n = pread(router->binlog_fd, hdbuf, BINLOG_EVENT_HDR_LEN, pos)) != BINLOG_EVENT_HDR_LEN)
        {
            switch (n)
            {
                case 0:
                    MXS_NOTICE("End of binlog file [%s] at %llu.",
                               router->binlog_name, pos);

                    if (!rotate_seen && !stop_seen)
                    {
                        MXS_NOTICE("No STOP or ROTATE event seen.");
                        /* create a new routine for this */
                    }
                    else if (rotate_seen)
                    {
                        /** Binlog file is processed, prepare for next one */
                        strncpy(router->binlog_name, next_binlog, sizeof(router->binlog_name));   
                    }

                    if (pending_transaction > 0)
                    {
                        MXS_WARNING("Binlog file %s contains a previously opened "
                                    "transaction @ %llu. This pos is safe for slaves",
                                    router->binlog_name,
                                    last_known_commit);

                    }

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

            /**
             * Check for errors and force last_known_commit position
             * and current pos
             */

            if (pending_transaction > 0)
            {
                router->binlog_position = last_known_commit;
                router->current_pos = pos;
                MXS_ERROR("Binlog '%s' ends at position %lu and has an incomplete transaction at %lu. ",
                          router->binlog_name, router->current_pos, router->binlog_position);
                return AVRO_OPEN_TRANSACTION;
            }
            else
            {
                /* any error */
                if (n != 0)
                {
                    router->binlog_position = last_known_commit;
                    router->current_pos = pos;
                    return AVRO_BINLOG_ERROR;
                }
                else
                {
                    router->binlog_position = pos;
                    router->current_pos = pos;
                    if (rotate_seen || stop_seen)
                    {
                        return AVRO_OK;
                    }
                    else
                    {
                        return AVRO_NO_ROTATE_CLOSE;
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

        /* Allocate a GWBUF for the event */
        if ((result = gwbuf_alloc(hdr.event_size)) == NULL)
        {
            MXS_ERROR("Failed to allocate memory for binlog entry, "
                      "size %d at %llu.",
                      hdr.event_size, pos);

            router->binlog_position = last_known_commit;
            router->current_pos = pos;
            return AVRO_BINLOG_ERROR;
        }

        /* Copy the header in the buffer */
        data = GWBUF_DATA(result);
        memcpy(data, hdbuf, BINLOG_EVENT_HDR_LEN); // Copy the header in

        /* Read event data */
        if ((n = pread(router->binlog_fd, &data[BINLOG_EVENT_HDR_LEN], hdr.event_size - BINLOG_EVENT_HDR_LEN,
                       pos + BINLOG_EVENT_HDR_LEN)) != hdr.event_size - BINLOG_EVENT_HDR_LEN)
        {
            if (n == -1)
            {
                char err_msg[BLRM_STRERROR_R_MSG_SIZE + 1] = "";
                strerror_r(errno, err_msg, BLRM_STRERROR_R_MSG_SIZE);
                MXS_ERROR("Error reading the event at %llu in %s. "
                          "%s, expected %d bytes.",
                          pos, router->binlog_name,
                          err_msg, hdr.event_size - BINLOG_EVENT_HDR_LEN);
            }
            else
            {
                MXS_ERROR("Short read when reading the event at %llu in %s. "
                          "Expected %d bytes got %d bytes.",
                          pos, router->binlog_name,
                          hdr.event_size - BINLOG_EVENT_HDR_LEN, n);

                if (filelen > 0 && filelen - pos < hdr.event_size)
                {
                    MXS_ERROR("Binlog event is close to the end of the binlog file %s, "
                              " size is %lu.",
                              router->binlog_name, filelen);
                }
            }

            gwbuf_free(result);

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

        /* get firts event timestamp, after FDE */
        if (fde_seen)
        {
            first_event.event_time = (unsigned long) hdr.timestamp;
            first_event.event_type = hdr.event_type;
            first_event.event_pos = pos;
            fde_seen = 0;
        }

        /* get event content */
        ptr = data + BINLOG_EVENT_HDR_LEN;

        /* check for FORMAT DESCRIPTION EVENT */
        if (hdr.event_type == FORMAT_DESCRIPTION_EVENT)
        {
            int event_header_length;
            int event_header_ntypes;
            int n_events;
            int check_alg;
            uint8_t *checksum;
            char buf_t[40];
            struct tm tm_t;

            fde_seen = 1;
            fde_event.event_time = (unsigned long) hdr.timestamp;
            fde_event.event_type = hdr.event_type;
            fde_event.event_pos = pos;

            localtime_r(&fde_event.event_time, &tm_t);
            asctime_r(&tm_t, buf_t);

            if (buf_t[strlen(buf_t) - 1] == '\n')
            {
                buf_t[strlen(buf_t) - 1] = '\0';
            }

            /** Extract the event header lengths */
            event_header_length = ptr[2 + 50 + 4];
            event_header_ntypes = hdr.event_size - event_header_length - (2 + 50 + 4 + 1);
            memcpy(router->event_type_hdr_lens, ptr + 2 + 50 + 5, event_header_ntypes);
            router->event_types = event_header_ntypes;

            if (event_header_ntypes == 168)
            {
                /* mariadb 10 LOG_EVENT_TYPES*/
                event_header_ntypes -= 163;
            }
            else
            {
                if (event_header_ntypes == 165)
                {
                    /* mariadb 5 LOG_EVENT_TYPES*/
                    event_header_ntypes -= 160;
                }
                else
                {
                    /* mysql 5.6 LOG_EVENT_TYPES = 35 */
                    event_header_ntypes -= 35;
                }
            }

            n_events = hdr.event_size - event_header_length - (2 + 50 + 4 + 1);

            if (event_header_ntypes < n_events)
            {
                checksum = ptr + hdr.event_size - event_header_length - event_header_ntypes;
                check_alg = checksum[0];

                if (check_alg == 1)
                {
                    found_chksum = 1;
                }
                else
                {
                    found_chksum = 0;
                }
            }
        }

        /* set last event time, pos and type */
        last_event.event_time = (unsigned long) hdr.timestamp;
        last_event.event_type = hdr.event_type;
        last_event.event_pos = pos;

        /* Decode CLOSE/STOP Event */
        if (hdr.event_type == STOP_EVENT)
        {
            char  next_file[BLRM_BINLOG_NAME_STR_LEN + 1];

            stop_seen = true;
            snprintf(next_file, sizeof(next_file), BINLOG_NAMEFMT, router->fileroot,
                     blr_file_get_next_binlogname(router->binlog_name));
        }
        else if (hdr.event_type == TABLE_MAP_EVENT)
        {
            // TODO: Replace blr instance with avro instance
            //handle_table_map_event(router, &hdr, pos);
        }
        else if ((hdr.event_type >= WRITE_ROWS_EVENTv0 && hdr.event_type <= DELETE_ROWS_EVENTv1) ||
                 (hdr.event_type >= WRITE_ROWS_EVENTv2 && hdr.event_type <= DELETE_ROWS_EVENTv2))
        {
            // TODO: Replace blr instance with avro instance
            //handle_row_event(router, &hdr, router->table_maps, pos);
        }
        /* Decode ROTATE EVENT */
        else if (hdr.event_type == ROTATE_EVENT)
        {
            int len, slen;
            uint64_t new_pos;

            len = hdr.event_size - BINLOG_EVENT_HDR_LEN;
            new_pos = extract_field(ptr + 4, 32);
            new_pos <<= 32;
            new_pos |= extract_field(ptr, 32);
            slen = len - (8 + 4); // Allow for position and CRC
            if (found_chksum == 0)
            {
                slen += 4;
            }
            if (slen > BINLOG_FNAMELEN)
            {
                slen = BINLOG_FNAMELEN;
            }
            memcpy(next_binlog, ptr + 8, slen);
            next_binlog[slen] = 0;

            rotate_seen = true;

        }
        else  if (hdr.event_type == MARIADB10_GTID_EVENT)
        {
            uint64_t n_sequence; /* 8 bytes */
            uint32_t domainid; /* 4 bytes */
            unsigned int flags; /* 1 byte */
            n_sequence = extract_field(ptr, 64);
            domainid = extract_field(ptr + 8, 32);
            flags = *(ptr + 8 + 4);

            if (flags == 0)
            {
                snprintf(router->current_gtid, sizeof(router->current_gtid), "%u-%u-%lu", domainid,
                         hdr.serverid, n_sequence);
                // TODO: Handle GTID transactions
                if(pending_transaction > 0)
                {
                    MXS_ERROR("In binlog file '%s' at position %llu: Missing XID Event before GTID Event.",
                              router->binlog_name, pos);
                }
                pending_transaction++;
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

            statement_len = hdr.event_size - BINLOG_EVENT_HDR_LEN - (4 + 4 + 1 + 2 + 2 + var_block_len + 1 + db_name_len);
            statement_sql = malloc(statement_len + 1);
            strncpy(statement_sql, (char *) ptr + 4 + 4 + 1 + 2 + 2 + var_block_len + 1 + db_name_len, statement_len);
            statement_sql[statement_len] = '\0';

            /** Very simple detection of CREATE TABLE statements */
            int reg_err = 0;

            if (mxs_pcre2_simple_match(create_table_regex, statement_sql, 0, &reg_err) == MXS_PCRE2_MATCH)
            {
                //hande_create_table_event(statement_sql);
            }
            /* A transaction starts with this event */
            if (strncmp(statement_sql, "BEGIN", 5) == 0)
            {
                // TODO: Handle BEGIN
                if(pending_transaction > 0)
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
        }
        else
        {

            MXS_ERROR("Current event type %d @ %llu has nex pos = %u : exiting",
                      hdr.event_type, pos, hdr.next_pos);
            break;
        }

        transaction_events++;
    }

    return AVRO_BINLOG_ERROR;
}
