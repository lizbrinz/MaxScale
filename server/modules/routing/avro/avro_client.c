/*
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
 * Copyright MariaDB Corporation Ab 2015-2016
 */

/**
 * @file avro_client.c - contains code for the AVRO router to client communication
 *
 * @verbatim
 * Revision History
 *
 * Date     Who         Description
 * 10/03/2016   Massimiliano Pinto   Initial implementation
 * 11/03/2016   Massimiliano Pinto   Addition of JSON output
 *
 * @endverbatim
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <service.h>
#include <server.h>
#include <router.h>
#include <atomic.h>
#include <spinlock.h>
#include <blr.h>
#include <dcb.h>
#include <spinlock.h>
#include <housekeeper.h>
#include <sys/stat.h>
#include <skygw_types.h>
#include <skygw_utils.h>
#include <log_manager.h>
#include <version.h>
#include <zlib.h>

/* AVRO */
#include <avrorouter.h>
#include <maxavro.h>

extern int load_mysql_users(SERVICE *service);
extern int blr_save_dbusers(ROUTER_INSTANCE *router);
extern void blr_master_close(ROUTER_INSTANCE* router);
extern void blr_file_use_binlog(ROUTER_INSTANCE *router, char *file);
extern int blr_file_new_binlog(ROUTER_INSTANCE *router, char *file);
extern int blr_file_write_master_config(ROUTER_INSTANCE *router, char *error);
extern char *blr_extract_column(GWBUF *buf, int col);
extern uint32_t extract_field(uint8_t *src, int bits);
extern int MaxScaleUptime();

/* AVRO */
static int avro_client_do_registration(AVRO_INSTANCE *, AVRO_CLIENT *, GWBUF *);
static int avro_client_binlog_dump(ROUTER_INSTANCE *router, ROUTER_SLAVE *slave, GWBUF *queue);
int avro_client_callback(DCB *dcb, DCB_REASON reason, void *data);
static void avro_client_process_command(AVRO_INSTANCE *router, AVRO_CLIENT *client, GWBUF *queue);
static void avro_client_avro_to_json_output(AVRO_INSTANCE *router, AVRO_CLIENT *client,
                                            char *avro_file, uint64_t offset);

void poll_fake_write_event(DCB *dcb);

/**
 * Process a request packet from the slave server.
 *
 * @param router    The router instance this defines the master for this replication chain
 * @param client    The client specific data
 * @param queue     The incoming request packet
 */
int
avro_client_handle_request(AVRO_INSTANCE *router, AVRO_CLIENT *client, GWBUF *queue)
{
    int reg_ret;

    switch (client->state)
    {
        case AVRO_CLIENT_ERRORED:
            /* force disconnection */
            return 1;
            break;
        case AVRO_CLIENT_UNREGISTERED:
            /* Cal registration routine */
            reg_ret = avro_client_do_registration(router, client, queue);

            /* discard data in incoming buffer */
            gwbuf_free(queue);

            if (reg_ret == 0)
            {
                client->state = AVRO_CLIENT_ERRORED;
                dcb_printf(client->dcb, "ERR, code 12, msg: abcd");
                /* force disconnection */
                dcb_close(client->dcb);
                return 0;
                break;
            }
            else
            {
                /* Send OK ack to client */
                dcb_printf(client->dcb, "OK");

                client->state = AVRO_CLIENT_REGISTERED;
                MXS_INFO("%s: Client [%s] has completd REGISTRATION action",
                         client->dcb->service->name,
                         client->dcb->remote != NULL ? client->dcb->remote : "");

                break;
            }
        case AVRO_CLIENT_REGISTERED:
        case AVRO_CLIENT_REQUEST_DATA:
            if (client->state == AVRO_CLIENT_REGISTERED)
            {
                client->state = AVRO_CLIENT_REQUEST_DATA;
            }

            /* Process command from client */
            avro_client_process_command(router, client, queue);

            break;
        default:
            client->state = AVRO_CLIENT_ERRORED;
            return 1;
            break;
    }

    return 0;
}

/**
 * Handle the REGISTRATION command
 *
 * @param dcb    DCB with allocateid protocol
 * @param data   GWBUF with registration message
 * @return       1 for successful registration 0 otherwise
 *
 */
static int
avro_client_do_registration(AVRO_INSTANCE *router, AVRO_CLIENT *client, GWBUF *data)
{
    const char reg_uuid[] = "REGISTER UUID=";
    const int reg_uuid_len = strlen(reg_uuid);
    int data_len = GWBUF_LENGTH(data) - reg_uuid_len;
    char *request = GWBUF_DATA(data);
    /* 36 +1 */
    char uuid[CDC_UUID_LEN + 1];
    int ret = 0;

    if (strstr(request, reg_uuid) != NULL)
    {
        char *tmp_ptr;
        char *sep_ptr;
        int uuid_len = (data_len > CDC_UUID_LEN) ? CDC_UUID_LEN : data_len;
        strncpy(uuid, request + reg_uuid_len, uuid_len);
        uuid[uuid_len] = '\0';

        if ((sep_ptr = strchr(uuid, ',')) != NULL)
        {
            *sep_ptr = '\0';
        }
        if ((sep_ptr = strchr(uuid + strlen(uuid), ' ')) != NULL)
        {
            *sep_ptr = '\0';
        }
        if ((sep_ptr = strchr(uuid, ' ')) != NULL)
        {
            *sep_ptr = '\0';
        }

        if (strlen(uuid) < uuid_len)
        {
            data_len -= (uuid_len - strlen(uuid));
        }

        uuid_len = strlen(uuid);

        client->uuid = strdup(uuid);

        if (data_len > 0)
        {
            /* Check for CDC request type */
            tmp_ptr = strstr(request + sizeof(reg_uuid) + uuid_len, "TYPE=");
            if (tmp_ptr)
            {
                int cdc_type_len = (data_len > CDC_TYPE_LEN) ? CDC_TYPE_LEN : data_len;
                int typelen = strnlen(tmp_ptr, GWBUF_LENGTH(data) - (tmp_ptr - request));
                if (typelen < data_len)
                {
                    cdc_type_len -= (data_len - typelen);
                }

                cdc_type_len -= strlen("TYPE=");

                if (memcmp(tmp_ptr + 5, "AVRO", 4) == 0)
                {
                    ret = 1;
                    client->state = AVRO_CLIENT_REGISTERED;
                }
                else
                {
                    fprintf(stderr, "Registration TYPE not supported, only AVRO\n");
                }
            }
            else
            {
                fprintf(stderr, "TYPE not found in Registration\n");
            }
        }
        else
        {
            fprintf(stderr, "Registration data_len = 0\n");
        }
    }
    return ret;
}

/**
 * Process command from client
 *
 * @param router     The router instance
 * @param client     The specific client data
 * @param data       GWBUF with command
 *
 */
static void
avro_client_process_command(AVRO_INSTANCE *router, AVRO_CLIENT *client, GWBUF *queue)
{
    const char req_data[] = "REQUEST-DATA";
    const size_t req_data_len = sizeof(req_data) - 1;
    uint8_t *data = GWBUF_DATA(queue);
    uint8_t *ptr;
    char *command_ptr = strstr((char *)data, req_data);

    if (command_ptr != NULL)
    {
        char avro_file[AVRO_MAX_FILENAME_LEN + 1];
        char *file_ptr = command_ptr + req_data_len;
        int data_len = GWBUF_LENGTH(queue) - req_data_len;

        if (data_len > 1)
        {
            char *cmd_sep;

            strncpy(avro_file, file_ptr + 1, data_len - 1);
            avro_file[data_len - 1] = '\0';
            cmd_sep = strchr(avro_file, ' ');

            if (cmd_sep)
            {
                *cmd_sep++ = '\0';
                cmd_sep = strchr(cmd_sep, ' ');

                if (cmd_sep)
                {
                    *cmd_sep = '\0';
                }
            }

            strncpy(client->avro_binfile, avro_file, AVRO_MAX_FILENAME_LEN);
            client->avro_binfile[AVRO_MAX_FILENAME_LEN] = '\0';


            /* set callback routine for data sending */
            dcb_add_callback(client->dcb, DCB_REASON_DRAINED, avro_client_callback, client);

            /* Add fake event that will call the avro_client_callback() routine */
            poll_fake_write_event(client->dcb);
        }
        else
        {
            dcb_printf(client->dcb, "ERR REQUEST-DATA with no data");
        }
    }
    else
    {
        GWBUF *reply = gwbuf_alloc(5);
        ptr = GWBUF_DATA(reply);
        memcpy(ptr, "ECHO:", 5);
        reply = gwbuf_append(reply, queue);
        client->dcb->func.write(client->dcb, reply);
    }
}

/**
 * Print JSON output from selected AVRO file
 *
 * @param router     The router instance
 * @param client     The specific client data
 * @param avro_file  The requested AVRO file
 *
 */
static void
avro_client_avro_to_json_output(AVRO_INSTANCE *router, AVRO_CLIENT *client,
                                char *avro_file, uint64_t start_record)
{
    uint64_t offset = start_record;
    ss_dassert(router && client && avro_file);

    if (strnlen(avro_file, 1))
    {
        char filename[PATH_MAX + 1];
        snprintf(filename, PATH_MAX, "%s/%s.avro", router->avrodir, avro_file);

        MAXAVRO_FILE *file = maxavro_file_open(filename);

        if (file)
        {
            if (offset > 0)
            {
                maxavro_record_seek(file, offset);
            }

            do
            {
                json_t *row;
                int rc = 1;
                while (rc > 0 && (row = maxavro_record_read(file)))
                {
                    char *json = json_dumps(row, JSON_PRESERVE_ORDER);
                    GWBUF *buf;
                    if (json && (buf = gwbuf_alloc_and_load(strlen(json), (void*)json)))
                    {
                        rc = client->dcb->func.write(client->dcb, buf);
                    }
                    else
                    {
                        MXS_ERROR("Failed to dump JSON value.");
                    }
                    free(json);
                    json_decref(row);
                }
            }
            while (maxavro_next_block(file));

            if (maxavro_get_error(file) != MAXAVRO_ERR_NONE)
            {
                MXS_ERROR("Reading Avro file failed with error '%s'.",
                          maxavro_get_error_string(file));
            }

            /* update client struct */
            memcpy(&client->avro_file, file, sizeof(client->avro_file));

            /* may be just use client->avro_file->records_read and remove this var */
            client->last_sent_pos = client->avro_file.records_read;

            maxavro_file_close(file);
        }
    }
    else
    {
        fprintf(stderr, "No file specified\n");
        dcb_printf(client->dcb, "ERR avro file not specified");
    }
}

int avro_client_callback(DCB *dcb, DCB_REASON reason, void *userdata)
{
    /* Notes:
     * 1 - Currently not following next file, aka kind of rotate.
     * 2 - As there is no live distribution to clients, for new events,
     * the routine is continuosly checking last record in AVRO file.
     * Kind of last event time check could be done in order to avoid file
     * reading.
     * Or we could add in avro.c the current avro file being written, with last record.
     * The routine could also check this.
     */
    if (reason == DCB_REASON_DRAINED)
    {
        AVRO_CLIENT *client = (AVRO_CLIENT*)userdata;

        spinlock_acquire(&client->catch_lock);
        if (client->cstate & AVRO_CS_BUSY)
        {
            spinlock_release(&client->catch_lock);
            return 0;
        }

        client->cstate |= AVRO_CS_BUSY;
        spinlock_release(&client->catch_lock);

        spinlock_acquire(&client->router->lock);
        uint64_t last_pos = client->router->current_pos;
        spinlock_release(&client->router->lock);

        /* send current file content */
        if (last_pos > client->last_sent_pos)
        {
            avro_client_avro_to_json_output(client->router, client,
                                            client->avro_binfile,
                                            client->last_sent_pos);
        }

        spinlock_acquire(&client->catch_lock);
        client->cstate &= ~AVRO_CS_BUSY;
        client->cstate |= AVRO_WAIT_DATA;
        spinlock_release(&client->catch_lock);
    }

    return 0;
}

/**
 * @brief Notify a client that new data is available
 *
 * The client catch_lock must be held when calling this function.
 * @param client Client to notify
 */
void avro_notify_client(AVRO_CLIENT *client)
{
    /* Add fake event that will call the avro_client_callback() routine */
    poll_fake_write_event(client->dcb);
    client->cstate &= ~AVRO_WAIT_DATA;
}
