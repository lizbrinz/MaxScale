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
 * Copyright MariaDB Corporation Ab 2014-2015
 */

/**
 * @file avro_client.c - contains code for the router to slave communication
 *
 * The binlog router is designed to be used in replication environments to
 * increase the replication fanout of a master server. It provides a transparant
 * mechanism to read the binlog entries for multiple slaves while requiring
 * only a single connection to the actual master to support the slaves.
 *
 * The current prototype implement is designed to support MySQL 5.6 and has
 * a number of limitations. This prototype is merely a proof of concept and
 * should not be considered production ready.
 *
 * @verbatim
 * Revision History
 *
 * Date     Who         Description
 * 14/04/2014   Mark Riddoch        Initial implementation
 * 18/02/2015   Massimiliano Pinto  Addition of DISCONNECT ALL and DISCONNECT SERVER server_id
 * 18/03/2015   Markus Makela       Better detection of CRC32 | NONE  checksum
 * 19/03/2015   Massimiliano Pinto  Addition of basic MariaDB 10 compatibility support
 * 07/05/2015   Massimiliano Pinto  Added MariaDB 10 Compatibility
 * 11/05/2015   Massimiliano Pinto  Only MariaDB 10 Slaves can register to binlog router with a MariaDB 10 Master
 * 25/05/2015   Massimiliano Pinto  Addition of BLRM_SLAVE_STOPPED state and blr_start/stop_slave.
 *                  New commands STOP SLAVE, START SLAVE added.
 * 29/05/2015   Massimiliano Pinto  Addition of CHANGE MASTER TO ...
 * 05/06/2015   Massimiliano Pinto  router->service->dbref->sever->name instead of master->remote
 *                  in blr_slave_send_slave_status()
 * 08/06/2015   Massimiliano Pinto  blr_slave_send_slave_status() shows mysql_errno and error_msg
 * 15/06/2015   Massimiliano Pinto  Added constraints to CHANGE MASTER TO MASTER_LOG_FILE/POS
 * 23/06/2015   Massimiliano Pinto  Added utility routines for blr_handle_change_master
 *                  Call create/use binlog in blr_start_slave() (START SLAVE)
 * 29/06/2015   Massimiliano Pinto  Successfully CHANGE MASTER results in updating master.ini
 *                  in blr_handle_change_master()
 * 20/08/2015   Massimiliano Pinto  Added parsing and validation for CHANGE MASTER TO
 * 21/08/2015   Massimiliano Pinto  Added support for new config options:
 *                  master_uuid, master_hostname, master_version
 *                  If set those values are sent to slaves instead of
 *                  saved master responses
 * 03/09/2015   Massimiliano Pinto  Added support for SHOW [GLOBAL] VARIABLES LIKE
 * 04/09/2015   Massimiliano Pinto  Added support for SHOW WARNINGS
 * 15/09/2015   Massimiliano Pinto  Added support for SHOW [GLOBAL] STATUS LIKE 'Uptime'
 * 25/09/2015   Massimiliano Pinto  Addition of slave heartbeat:
 *                  the period set during registration is checked
 *                  and heartbeat event might be sent to the affected slave.
 * 25/09/2015   Martin Brampton         Block callback processing when no router session in the DCB
 * 23/10/15 Markus Makela       Added current_safe_event
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
// AVRO
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
// AVRO
static int avro_client_do_registration(AVRO_INSTANCE *, AVRO_CLIENT *, GWBUF *);
static int avro_client_binlog_dump(ROUTER_INSTANCE *router, ROUTER_SLAVE *slave, GWBUF *queue);
int avro_client_callback(DCB *dcb, DCB_REASON reason, void *data);

void poll_fake_write_event(DCB *dcb);

/**
 * Process a request packet from the slave server.
 *
 * The router can handle a limited subset of requests from the slave, these
 * include a subset of general SQL queries, a slave registeration command and
 * the binlog dump command.
 *
 * The strategy for responding to these commands is to use caches responses
 * for the the same commands that have previously been made to the real master
 * if this is possible, if it is not then the router itself will synthesize a
 * response.
 *
 * @param router    The router instance this defines the master for this replication chain
 * @param slave     The slave specific data
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
                // force disconnection;
                return 1;
                break;
        case AVRO_CLIENT_UNREGISTERED:
            reg_ret = avro_client_do_registration(router, client, queue);
            /* discard data in incoming buffer */
            while ((queue = gwbuf_consume(queue, GWBUF_LENGTH(queue))) != NULL);

            if (reg_ret == 0)
            {
                client->state = AVRO_CLIENT_ERRORED;
                dcb_printf(client->dcb, "ERR, code 12, msg: abcd");
                // force disconnection;
                dcb_close(client->dcb);
                return 0;
                break;
            }
            else
            {
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

             memcpy(ptr, "ECHO:", 5);
             reply = gwbuf_append(reply, queue);
             client->dcb->func.write(client->dcb, reply);
             break;
        default:
            client->state = AVRO_CLIENT_ERRORED;
            return 1;
            break;
    }

    return 0;
}

/**
 * Hande the REGISTRATION command
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

                if (strncmp(tmp_ptr + 5, "AVRO", 5) != 0)
                { 
                    ret = 1;
                    client->state = AVRO_CLIENT_REGISTERED;
                }
            }
        }
    }
    return ret;
}

/**
 * Process a COM_BINLOG_DUMP message from the slave. This is the
 * final step in the process of registration. The new master, MaxScale
 * must send a response packet and generate a fake BINLOG_ROTATE event
 * with the binlog file requested by the slave. And then send a
 * FORMAT_DESCRIPTION_EVENT that has been saved from the real master.
 *
 * Once send MaxScale must continue to send binlog events to the slave.
 *
 * @param   router      The router instance
 * @param   slave       The slave server
 * @param   queue       The BINLOG_DUMP packet
 * @return          The number of bytes written to the slave
 */
static int
avro_client_binlog_dump(ROUTER_INSTANCE *router, ROUTER_SLAVE *slave, GWBUF *queue)
{
    GWBUF       *resp;
    uint8_t     *ptr;
    int     len, rval, binlognamelen;
    REP_HEADER  hdr;
    uint32_t    chksum;

    dcb_add_callback(slave->dcb, DCB_REASON_DRAINED, avro_client_callback, slave);

    slave->state = BLRS_DUMPING;


    if (slave->binlog_pos != router->binlog_position ||
        strcmp(slave->binlogfile, router->binlog_name) != 0)
    {
        spinlock_acquire(&slave->catch_lock);
        slave->cstate &= ~CS_UPTODATE;
        slave->cstate |= CS_EXPECTCB;
        spinlock_release(&slave->catch_lock);
        poll_fake_write_event(slave->dcb);
    }

    return 1;
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
 * We have a registered slave that is behind the current leading edge of the
 * binlog. We must replay the log entries to bring this node up to speed.
 *
 * There may be a large number of records to send to the slave, the process
 * is triggered by the slave COM_BINLOG_DUMP message and all the events must
 * be sent without receiving any new event. This measn there is no trigger into
 * MaxScale other than this initial message. However, if we simply send all the
 * events we end up with an extremely long write queue on the DCB and risk
 * running the server out of resources.
 *
 * The slave catchup routine will send a burst of replication events per single
 * call. The paramter "long" control the number of events in the burst. The
 * short burst is intended to be used when the master receive an event and
 * needs to put the slave into catchup mode. This prevents the slave taking
 * too much time away from the thread that is processing the master events.
 *
 * At the end of the burst a fake EPOLLOUT event is added to the poll event
 * queue. This ensures that the slave callback for processing DCB write drain
 * will be called and future catchup requests will be handled on another thread.
 *
 * @param   router      The binlog router
 * @param   slave       The slave that is behind
 * @param   large       Send a long or short burst of events
 * @return          The number of bytes written
 */
int
avro_client_catchup(ROUTER_INSTANCE *router, ROUTER_SLAVE *slave, bool large)
{
    GWBUF       *head, *record;
    REP_HEADER  hdr;
    int     written, rval = 1, burst;
    int     rotating = 0;
    long    burst_size;
    uint8_t     *ptr;
    char read_errmsg[BINLOG_ERROR_MSG_LEN + 1];

    return rval;
}

/**
 * The DCB callback used by the slave to obtain DCB_REASON_LOW_WATER callbacks
 * when the server sends all the the queue data for a DCB. This is the mechanism
 * that is used to implement the flow control mechanism for the sending of
 * large quantities of binlog records during the catchup process.
 *
 * @param dcb       The DCB of the slave connection
 * @param reason    The reason the callback was called
 * @param data      The user data, in this case the server structure
 */
int
avro_client_callback(DCB *dcb, DCB_REASON reason, void *data)
{
    ROUTER_SLAVE        *slave = (ROUTER_SLAVE *)data;
    ROUTER_INSTANCE     *router = slave->router;
    unsigned int cstate;

    if (NULL == dcb->session->router_session)
    {
        /*
         * The following processing will fail if there is no router session,
         * because the "data" parameter will not contain meaningful data,
         * so we have no choice but to stop here.
         */
        return 0;
    }
    if (reason == DCB_REASON_DRAINED)
    {
        if (slave->state == BLRS_DUMPING)
        {
            int do_return;

            spinlock_acquire(&router->binlog_lock);

            do_return = 0;
            cstate = slave->cstate;

            /* check for a pending transaction and not rotating */
            if (router->pending_transaction && strcmp(router->binlog_name, slave->binlogfile) == 0 &&
                (slave->binlog_pos > router->binlog_position) && !router->rotating)
            {
                do_return = 1;
            }

            spinlock_release(&router->binlog_lock);

            if (do_return)
            {
                spinlock_acquire(&slave->catch_lock);
                slave->cstate |= CS_EXPECTCB;
                spinlock_release(&slave->catch_lock);
                poll_fake_write_event(slave->dcb);

                return 0;
            }

            spinlock_acquire(&slave->catch_lock);
            cstate = slave->cstate;
            slave->cstate &= ~(CS_UPTODATE | CS_EXPECTCB);
            spinlock_release(&slave->catch_lock);

            if ((cstate & CS_UPTODATE) == CS_UPTODATE)
            {
#ifdef STATE_CHANGE_LOGGING_ENABLED
                MXS_NOTICE("%s: Slave %s:%d, server-id %d transition from up-to-date to catch-up in blr_slave_callback, binlog file '%s', position %lu.",
                           router->service->name,
                           slave->dcb->remote,
                           ntohs((slave->dcb->ipv4).sin_port),
                           slave->serverid,
                           slave->binlogfile, (unsigned long)slave->binlog_pos);
#endif
            }

            slave->stats.n_dcb++;
            avro_client_catchup(router, slave, true);
        }
        else
        {
            MXS_DEBUG("Ignored callback due to slave state %s",
                      blrs_states[slave->state]);
        }
    }

    if (reason == DCB_REASON_LOW_WATER)
    {
        if (slave->state == BLRS_DUMPING)
        {
            slave->stats.n_cb++;
            avro_client_catchup(router, slave, true);
        }
        else
        {
            slave->stats.n_cbna++;
        }
    }
    return 0;
}

/**
 * Rotate the slave to the new binlog file
 *
 * @param slave     The slave instance
 * @param ptr       The rotate event (minus header and OK byte)
 */
void
avro_client_rotate(AVRO_INSTANCE *router, AVRO_CLIENT *client, uint8_t *ptr)
{
    int len = EXTRACT24(ptr + 9);   // Extract the event length

    len = len - (BINLOG_EVENT_HDR_LEN + 8);     // Remove length of header and position
    if (len > BINLOG_FNAMELEN)
    {
        len = BINLOG_FNAMELEN;
    }
    ptr += BINLOG_EVENT_HDR_LEN;    // Skip header
    memcpy(client->avrofile, ptr + 8, len);
    client->avrofile[len] = 0;
}
