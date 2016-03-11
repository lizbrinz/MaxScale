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
#include <mxs_avro.h>

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
static void avro_client_avro_to_json_ouput(AVRO_INSTANCE *router, AVRO_CLIENT *client, char *avro_file);

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
    GWBUF *reply = gwbuf_alloc(5);
    uint8_t *ptr = GWBUF_DATA(reply);
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
            while ((queue = gwbuf_consume(queue, GWBUF_LENGTH(queue))) != NULL);

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
                client->state = AVRO_CLIENT_REQUEST_DATA;

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
    int reg_rc = 0;
    int data_len = GWBUF_LENGTH(data) - strlen("REGISTER UUID=");
    char *request = GWBUF_DATA(data);
    /* 36 +1 */
    char uuid[CDC_UUID_LEN + 1];
    int ret = 0;

    if (strstr(request, "REGISTER UUID=") != NULL)
    {
        char *tmp_ptr;
        char *sep_ptr;
        int uuid_len = (data_len > CDC_UUID_LEN) ? CDC_UUID_LEN : data_len;
        strncpy(uuid, request + strlen("REGISTER UUID="), uuid_len);
        uuid[uuid_len] = '\0';

        if ((sep_ptr = strchr(uuid, ',')) != NULL)
        {
            *sep_ptr='\0';
        }
        if ((sep_ptr = strchr(uuid+strlen(uuid), ' ')) != NULL)
        {
            *sep_ptr='\0';
        }
        if ((sep_ptr = strchr(uuid, ' ')) != NULL)
        {
            *sep_ptr='\0';
        }

        if (strlen(uuid) < uuid_len)
          data_len -= (uuid_len - strlen(uuid));

        uuid_len = strlen(uuid);

        client->uuid = strdup(uuid);

        if (data_len > 0)
        {
            /* Check for CDC request type */
            tmp_ptr = strstr(request + strlen("REGISTER UUID=") + uuid_len, "TYPE=");
            if (tmp_ptr)
            {
                int cdc_type_len = (data_len > CDC_TYPE_LEN) ? CDC_TYPE_LEN : data_len;
                if (strlen(tmp_ptr) < data_len)
                    cdc_type_len -= (data_len - strlen(tmp_ptr));

                cdc_type_len -= strlen("TYPE=");

                fprintf(stderr, "tmp_ptr + 5 [%s]\n", tmp_ptr + 5);
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
 * Encode a value into a number of bits in a AVRO format
 *
 * @param   data    Pointer to location in target packet
 * @param   value   The value to encode into the buffer
 * @param   len Number of bits to encode value into
 */
static void
avro_encode_value(unsigned char *data, unsigned int value, int len)
{
    while (len > 0)
    {
        *data++ = value & 0xff;
        value >>= 8;
        len -= 8;
    }
}

/**
 * Process commmand from client
 * 
 * @param router     The router instance
 * @param client     The specific client data
 * @param data       GWBUF with command
 *
 */
static void
avro_client_process_command(AVRO_INSTANCE *router, AVRO_CLIENT *client, GWBUF *queue)
{
    uint8_t *data = GWBUF_DATA(queue);
    uint8_t *ptr; 
    char *command_ptr; 

    command_ptr = strstr((char *)data, "REQUEST-DATA");

    if (command_ptr != NULL)
    {
        char avro_file[AVRO_MAX_FILENAME_LEN + 1];
        char *file_ptr = command_ptr + strlen("REQUEST-DATA");
        int data_len = GWBUF_LENGTH(queue) - strlen("REQUEST-DATA");

        fprintf(stderr, "AVRO file name len is %i, packet size %lu\n", data_len, GWBUF_LENGTH(queue));

        if (data_len > 1)
        {
            char *cmd_sep;

            strncpy(avro_file, file_ptr + 1, data_len - 1);
            avro_file[data_len - 1] = '\0';
            fprintf(stderr, "AVRO file name len is %lu\n", strlen(avro_file));
            cmd_sep = strchr(avro_file, ' ');
            if (cmd_sep)
               *cmd_sep = '\0'; 

            fprintf(stderr, "AVRO file name len is %lu\n", strlen(avro_file));
            avro_client_avro_to_json_ouput(router, client, avro_file);
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
avro_client_avro_to_json_ouput(AVRO_INSTANCE *router, AVRO_CLIENT *client, char *avro_file)
{
        avro_file_reader_t  reader;
        FILE *fp;
        int  should_close;
        char filename[PATH_MAX +1];

        if (avro_file == NULL) {
                fp = stdin;
                avro_file = "<stdin>";
                should_close = 0;
        } else {
                if (strlen(avro_file))
                {
                    snprintf(filename, PATH_MAX, "%s/%s.avro", router->avrodir, avro_file);
                    filename[PATH_MAX] = '\0';
                    fprintf(stderr, "Reading from [%s]\n", filename);
                }
                else
                {
                    fprintf(stderr, "No file specified\n");
                    dcb_printf(client->dcb, "ERR avro file not specified");
                    return;
                }

                fp = fopen(filename, "rb");
                should_close = 1;

                if (fp == NULL) {
                        fprintf(stderr, "Error opening %s:\n  %s\n",
                                avro_file, strerror(errno));
                        dcb_printf(client->dcb, "ERR opening %s", avro_file);
                        return;
                }
        }

        if (avro_file_reader_fp(fp, avro_file, 0, &reader)) {
                fprintf(stderr, "Error opening %s:\n  %s\n",
                        avro_file, avro_strerror());
                dcb_printf(client->dcb, "ERR first read in %s", avro_file);

                if (should_close) {
                        fclose(fp);
                }
                return;
        }
        avro_schema_t  wschema;
        avro_value_iface_t  *iface;
        avro_value_t  value;

        wschema = avro_file_reader_get_writer_schema(reader);
        iface = avro_generic_class_from_schema(wschema);
        avro_generic_value_new(iface, &value);

        int rval;

        while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
                char  *json;

                if (avro_value_to_json(&value, 1, &json)) {
                        fprintf(stderr, "Error converting value to JSON: %s\n",
                                avro_strerror());
                        dcb_printf(client->dcb, "ERR converting to Json %s", avro_file);
                } else {
                        dcb_printf(client->dcb, "%s\n", json);
                        free(json);
                }

                avro_value_reset(&value);
        }

        /* If it was not an EOF that caused it to fail,
         * print the error.
         */
        if (rval != EOF) {
                fprintf(stderr, "Error: %s\n", avro_strerror());
                dcb_printf(client->dcb, "ERR error while reading %s", avro_file);
        }

        avro_file_reader_close(reader);
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(wschema);

        if (should_close) {
                fclose(fp);
        }
}

