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
 * Copyright MariaDB Corporation Ab 2013-2014
 */

/**
 * @file mysql_client.c
 *
 * MySQL Protocol module for handling the protocol between the gateway
 * and the client.
 *
 * Revision History
 * Date         Who                     Description
 * 14/06/2013   Mark Riddoch            Initial version
 * 17/06/2013   Massimiliano Pinto      Added Client To MaxScale routines
 * 24/06/2013   Massimiliano Pinto      Added: fetch passwords from service users' hashtable
 * 02/09/2013   Massimiliano Pinto      Added: session refcount
 * 16/12/2013   Massimiliano Pinto      Added: client closed socket detection with recv(..., MSG_PEEK)
 * 24/02/2014   Massimiliano Pinto      Added: on failed authentication a new users' table is loaded
 *                                      with time and frequency limitations
 *                                      If current user is authenticated the new users' table will
 *                                      replace the old one
 * 28/02/2014   Massimiliano Pinto      Added: client IPv4 in dcb->ipv4 and inet_ntop for string
 *                                      representation
 * 11/03/2014   Massimiliano Pinto      Added: Unix socket support
 * 07/05/2014   Massimiliano Pinto      Added: specific version string in server handshake
 * 09/09/2014   Massimiliano Pinto      Added: 777 permission for socket path
 * 13/10/2014   Massimiliano Pinto      Added: dbname authentication check
 * 10/11/2014   Massimiliano Pinto      Added: client charset added to protocol struct
 * 29/05/2015   Markus Makela           Added SSL support
 * 11/06/2015   Martin Brampton         COM_QUIT suppressed for persistent connections
 * 04/09/2015   Martin Brampton         Introduce DUMMY session to fulfill guarantee DCB always has session
 * 09/09/2015   Martin Brampton         Modify error handler calls
 * 11/01/2016   Martin Brampton         Remove SSL write code, now handled at lower level;
 *                                      replace gwbuf_consume by gwbuf_free (multiple).
 * 07/02/2016   Martin Brampton         Split off authentication and SSL.
 */
#include <gw_protocol.h>
#include <skygw_utils.h>
#include <log_manager.h>
#include <mysql_client_server_protocol.h>
#include <mysql_auth.h>
#include <gw_ssl.h>
#include <gw.h>
#include <modinfo.h>
#include <sys/stat.h>
#include <modutil.h>
#include <netinet/tcp.h>

MODULE_INFO info =
{
    MODULE_API_PROTOCOL,
    MODULE_GA,
    GWPROTOCOL_VERSION,
    "The client to MaxScale MySQL protocol implementation"
};

static char *version_str = "V1.0.0";

static int gw_MySQLAccept(DCB *listener);
static int gw_MySQLListener(DCB *listener, char *config_bind);
static int gw_read_client_event(DCB* dcb);
static int gw_write_client_event(DCB *dcb);
static int gw_MySQLWrite_client(DCB *dcb, GWBUF *queue);
static int gw_error_client_event(DCB *dcb);
static int gw_client_close(DCB *dcb);
static int gw_client_hangup_event(DCB *dcb);
static int mysql_send_ok(DCB *dcb, int packet_number, int in_affected_rows, const char* mysql_message);
static int MySQLSendHandshake(DCB* dcb);
static int route_by_statement(SESSION *, GWBUF **);
static void mysql_client_auth_error_handling(DCB *dcb, int auth_val);
extern char* create_auth_fail_str(char *username, char *hostaddr, char *sha1, char *db,int);

/*
 * The "module object" for the mysqld client protocol module.
 */
static GWPROTOCOL MyObject =
{
    gw_read_client_event,                   /* Read - EPOLLIN handler        */
    gw_MySQLWrite_client,                   /* Write - data from gateway     */
    gw_write_client_event,                  /* WriteReady - EPOLLOUT handler */
    gw_error_client_event,                  /* Error - EPOLLERR handler      */
    gw_client_hangup_event,                 /* HangUp - EPOLLHUP handler     */
    gw_MySQLAccept,                         /* Accept                        */
    NULL,                                   /* Connect                       */
    gw_client_close,                        /* Close                         */
    gw_MySQLListener,                       /* Listen                        */
    NULL,                                   /* Authentication                */
    NULL                                    /* Session                       */
};

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 */
char* version()
{
    return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void ModuleInit()
{
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
GWPROTOCOL* GetModuleObject()
{
    return &MyObject;
}

/**
 * mysql_send_ok
 *
 * Send a MySQL protocol OK message to the dcb (client)
 *
 * @param dcb Descriptor Control Block for the connection to which the OK is sent
 * @param packet_number
 * @param in_affected_rows
 * @param mysql_message
 * @return packet length
 *
 */
int mysql_send_ok(DCB *dcb, int packet_number, int in_affected_rows, const char* mysql_message)
{
    uint8_t *outbuf = NULL;
    uint32_t mysql_payload_size = 0;
    uint8_t mysql_packet_header[4];
    uint8_t *mysql_payload = NULL;
    uint8_t field_count = 0;
    uint8_t affected_rows = 0;
    uint8_t insert_id = 0;
    uint8_t mysql_server_status[2];
    uint8_t mysql_warning_count[2];
    GWBUF *buf;

    affected_rows = in_affected_rows;

    mysql_payload_size =
        sizeof(field_count) +
        sizeof(affected_rows) +
        sizeof(insert_id) +
        sizeof(mysql_server_status) +
        sizeof(mysql_warning_count);

    if (mysql_message != NULL)
    {
        mysql_payload_size += strlen(mysql_message);
    }

    // allocate memory for packet header + payload
    if ((buf = gwbuf_alloc(sizeof(mysql_packet_header) + mysql_payload_size)) == NULL)
    {
        return 0;
    }
    outbuf = GWBUF_DATA(buf);

    // write packet header with packet number
    gw_mysql_set_byte3(mysql_packet_header, mysql_payload_size);
    mysql_packet_header[3] = packet_number;

    // write header
    memcpy(outbuf, mysql_packet_header, sizeof(mysql_packet_header));

    mysql_payload = outbuf + sizeof(mysql_packet_header);

    mysql_server_status[0] = 2;
    mysql_server_status[1] = 0;
    mysql_warning_count[0] = 0;
    mysql_warning_count[1] = 0;

    // write data
    memcpy(mysql_payload, &field_count, sizeof(field_count));
    mysql_payload = mysql_payload + sizeof(field_count);

    memcpy(mysql_payload, &affected_rows, sizeof(affected_rows));
    mysql_payload = mysql_payload + sizeof(affected_rows);

    memcpy(mysql_payload, &insert_id, sizeof(insert_id));
    mysql_payload = mysql_payload + sizeof(insert_id);

    memcpy(mysql_payload, mysql_server_status, sizeof(mysql_server_status));
    mysql_payload = mysql_payload + sizeof(mysql_server_status);

    memcpy(mysql_payload, mysql_warning_count, sizeof(mysql_warning_count));
    mysql_payload = mysql_payload + sizeof(mysql_warning_count);

    if (mysql_message != NULL)
    {
        memcpy(mysql_payload, mysql_message, strlen(mysql_message));
    }

    // writing data in the Client buffer queue
    dcb->func.write(dcb, buf);

    return sizeof(mysql_packet_header) + mysql_payload_size;
}

/**
 * MySQLSendHandshake
 *
 * @param dcb The descriptor control block to use for sending the handshake request
 * @return      The packet length sent
 */
int MySQLSendHandshake(DCB* dcb)
{
    uint8_t *outbuf = NULL;
    uint32_t mysql_payload_size = 0;
    uint8_t mysql_packet_header[4];
    uint8_t mysql_packet_id = 0;
    uint8_t mysql_filler = GW_MYSQL_HANDSHAKE_FILLER;
    uint8_t mysql_protocol_version = GW_MYSQL_PROTOCOL_VERSION;
    uint8_t *mysql_handshake_payload = NULL;
    uint8_t mysql_thread_id[4];
    uint8_t mysql_scramble_buf[9] = "";
    uint8_t mysql_plugin_data[13] = "";
    uint8_t mysql_server_capabilities_one[2];
    uint8_t mysql_server_capabilities_two[2];
    uint8_t mysql_server_language = 8;
    uint8_t mysql_server_status[2];
    uint8_t mysql_scramble_len = 21;
    uint8_t mysql_filler_ten[10];
    uint8_t mysql_last_byte = 0x00;
    char server_scramble[GW_MYSQL_SCRAMBLE_SIZE + 1]="";
    char *version_string;
    int len_version_string = 0;

    MySQLProtocol *protocol = DCB_PROTOCOL(dcb, MySQLProtocol);
    GWBUF *buf;

    /* get the version string from service property if available*/
    if (dcb->service->version_string != NULL)
    {
        version_string = dcb->service->version_string;
        len_version_string = strlen(version_string);
    }
    else
    {
        version_string = GW_MYSQL_VERSION;
        len_version_string = strlen(GW_MYSQL_VERSION);
    }

    gw_generate_random_str(server_scramble, GW_MYSQL_SCRAMBLE_SIZE);

    // copy back to the caller
    memcpy(protocol->scramble, server_scramble, GW_MYSQL_SCRAMBLE_SIZE);

    // fill the handshake packet

    memset(&mysql_filler_ten, 0x00, sizeof(mysql_filler_ten));

    // thread id, now put thePID
    gw_mysql_set_byte4(mysql_thread_id, getpid() + dcb->fd);

    memcpy(mysql_scramble_buf, server_scramble, 8);

    memcpy(mysql_plugin_data, server_scramble + 8, 12);

    mysql_payload_size =
        sizeof(mysql_protocol_version) + (len_version_string + 1) + sizeof(mysql_thread_id) + 8 +
        sizeof(mysql_filler) + sizeof(mysql_server_capabilities_one) + sizeof(mysql_server_language) +
        sizeof(mysql_server_status) + sizeof(mysql_server_capabilities_two) + sizeof(mysql_scramble_len) +
        sizeof(mysql_filler_ten) + 12 + sizeof(mysql_last_byte) + strlen("mysql_native_password") +
        sizeof(mysql_last_byte);

    // allocate memory for packet header + payload
    if ((buf = gwbuf_alloc(sizeof(mysql_packet_header) + mysql_payload_size)) == NULL)
    {
        ss_dassert(buf != NULL);
        return 0;
    }
    outbuf = GWBUF_DATA(buf);

    // write packet heder with mysql_payload_size
    gw_mysql_set_byte3(mysql_packet_header, mysql_payload_size);

    // write packent number, now is 0
    mysql_packet_header[3]= mysql_packet_id;
    memcpy(outbuf, mysql_packet_header, sizeof(mysql_packet_header));

    // current buffer pointer
    mysql_handshake_payload = outbuf + sizeof(mysql_packet_header);

    // write protocol version
    memcpy(mysql_handshake_payload, &mysql_protocol_version, sizeof(mysql_protocol_version));
    mysql_handshake_payload = mysql_handshake_payload + sizeof(mysql_protocol_version);

    // write server version plus 0 filler
    strcpy((char *)mysql_handshake_payload, version_string);
    mysql_handshake_payload = mysql_handshake_payload + len_version_string;

    *mysql_handshake_payload = 0x00;

    mysql_handshake_payload++;

    // write thread id
    memcpy(mysql_handshake_payload, mysql_thread_id, sizeof(mysql_thread_id));
    mysql_handshake_payload = mysql_handshake_payload + sizeof(mysql_thread_id);

    // write scramble buf
    memcpy(mysql_handshake_payload, mysql_scramble_buf, 8);
    mysql_handshake_payload = mysql_handshake_payload + 8;
    *mysql_handshake_payload = GW_MYSQL_HANDSHAKE_FILLER;
    mysql_handshake_payload++;

    // write server capabilities part one
    mysql_server_capabilities_one[0] = GW_MYSQL_SERVER_CAPABILITIES_BYTE1;
    mysql_server_capabilities_one[1] = GW_MYSQL_SERVER_CAPABILITIES_BYTE2;


    mysql_server_capabilities_one[0] &= ~GW_MYSQL_CAPABILITIES_COMPRESS;

    if (ssl_required_by_dcb(dcb))
    {
        mysql_server_capabilities_one[1] |= GW_MYSQL_CAPABILITIES_SSL >> 8;
    }

    memcpy(mysql_handshake_payload, mysql_server_capabilities_one, sizeof(mysql_server_capabilities_one));
    mysql_handshake_payload = mysql_handshake_payload + sizeof(mysql_server_capabilities_one);

    // write server language
    memcpy(mysql_handshake_payload, &mysql_server_language, sizeof(mysql_server_language));
    mysql_handshake_payload = mysql_handshake_payload + sizeof(mysql_server_language);

    //write server status
    mysql_server_status[0] = 2;
    mysql_server_status[1] = 0;
    memcpy(mysql_handshake_payload, mysql_server_status, sizeof(mysql_server_status));
    mysql_handshake_payload = mysql_handshake_payload + sizeof(mysql_server_status);

    //write server capabilities part two
    mysql_server_capabilities_two[0] = 15;
    mysql_server_capabilities_two[1] = 128;

    memcpy(mysql_handshake_payload, mysql_server_capabilities_two, sizeof(mysql_server_capabilities_two));
    mysql_handshake_payload = mysql_handshake_payload + sizeof(mysql_server_capabilities_two);

    // write scramble_len
    memcpy(mysql_handshake_payload, &mysql_scramble_len, sizeof(mysql_scramble_len));
    mysql_handshake_payload = mysql_handshake_payload + sizeof(mysql_scramble_len);

    //write 10 filler
    memcpy(mysql_handshake_payload, mysql_filler_ten, sizeof(mysql_filler_ten));
    mysql_handshake_payload = mysql_handshake_payload + sizeof(mysql_filler_ten);

    // write plugin data
    memcpy(mysql_handshake_payload, mysql_plugin_data, 12);
    mysql_handshake_payload = mysql_handshake_payload + 12;

    //write last byte, 0
    *mysql_handshake_payload = 0x00;
    mysql_handshake_payload++;

    // to be understanded ????
    memcpy(mysql_handshake_payload, "mysql_native_password", strlen("mysql_native_password"));
    mysql_handshake_payload = mysql_handshake_payload + strlen("mysql_native_password");

    //write last byte, 0
    *mysql_handshake_payload = 0x00;

    mysql_handshake_payload++;

    // writing data in the Client buffer queue
    dcb->func.write(dcb, buf);

    return sizeof(mysql_packet_header) + mysql_payload_size;
}

/**
 * Write function for client DCB: writes data from MaxScale to Client
 *
 * @param dcb   The DCB of the client
 * @param queue Queue of buffers to write
 */
int gw_MySQLWrite_client(DCB *dcb, GWBUF *queue)
{
    return dcb_write(dcb, queue);
}

/**
 * @brief Client read event triggered by EPOLLIN
 *
 * @param dcb   Descriptor control block
 * @return 0 if succeed, 1 otherwise
 */
int gw_read_client_event(DCB* dcb)
{
    MySQLProtocol *protocol = NULL;
    GWBUF *read_buffer = NULL;
    int rc = 0;
    int nbytes_read = 0;
    int max_bytes = 0;

    CHK_DCB(dcb);
    protocol = DCB_PROTOCOL(dcb, MySQLProtocol);
    CHK_PROTOCOL(protocol);

#ifdef SS_DEBUG
    MXS_DEBUG("[gw_read_client_event] Protocol state: %s",
              gw_mysql_protocol_state2string(protocol->protocol_auth_state));

#endif

    /**
     * The use of max_bytes seems like a hack, but no better option is available
     * at the time of writing. When a MySQL server receives a new connection
     * request, it sends an Initial Handshake Packet. Where the client wants to
     * use SSL, it responds with an SSL Request Packet (in place of a Handshake
     * Response Packet). The SSL Request Packet contains only the basic header,
     * and not the user credentials. It is 36 bytes long.  The server then
     * initiates the SSL handshake (via calls to OpenSSL).
     *
     * In many cases, this is what happens. But occasionally, the client seems
     * to send a packet much larger than 36 bytes (in tests it was 333 bytes).
     * If the whole of the packet is read, it is then lost to the SSL handshake
     * process. Why this happens is presently unknown. Reading just 36 bytes
     * when the server requires SSL and SSL has not yet been negotiated seems
     * to solve the problem.
     *
     * If a neater solution can be found, so much the better.
     */
    if (ssl_required_but_not_negotiated(dcb))
    {
        max_bytes = 36;
    }
    rc = dcb_read(dcb, &read_buffer, max_bytes);
    if (rc < 0)
    {
        dcb_close(dcb);
    }
    if (0 == (nbytes_read = gwbuf_length(read_buffer)))
    {
        goto return_rc;
    }

    switch (protocol->protocol_auth_state)
    {
        /**
         *
         * When a listener receives a new connection request, it creates a
         * request handler DCB to for the client connection. The listener also
         * sends the initial authentication request to the client. The first
         * time this function is called from the poll loop, the client reply
         * to the authentication request should be available.
         *
         * If the authentication is successful the protocol authentication state
         * will be changed to MYSQL_IDLE (see below).
         *
         */
    case MYSQL_AUTH_SENT:
        {
            /* int compress = -1; */
            int auth_val, packet_number;
            MySQLProtocol *protocol = DCB_PROTOCOL(dcb, MySQLProtocol);

            packet_number = ssl_required_by_dcb(dcb) ? 3 : 2;

            /**
             * The first step in the authentication process is to extract the
             * relevant information from the buffer supplied and place it
             * into a data structure pointed to by the DCB.  The "success"
             * result is not final, it implies only that the process is so
             * far successful, not that authentication has completed.  If the
             * data extraction succeeds, then a call is made to
             * mysql_auth_authenticate to carry out the actual user checks.
             */
            if (MYSQL_AUTH_SUCCEEDED == (
                auth_val = mysql_auth_set_protocol_data(dcb, read_buffer)))
            {
                /*
                  compress =
                  GW_MYSQL_CAPABILITIES_COMPRESS & gw_mysql_get_byte4(
                  &protocol->client_capabilities);
                */
                auth_val = mysql_auth_authenticate(dcb, &read_buffer);
            }

            /**
             * At this point, if the auth_val return code indicates success
             * the user authentication has been successfully completed.
             * But in order to have a working connection, a session has to
             * be created.  Provided that is successful (indicated by a
             * non-null session) then the whole process has succeeded. In all
             * other cases an error return is made.
             */
            if (MYSQL_AUTH_SUCCEEDED == auth_val)
            {
                SESSION *session;

                protocol->protocol_auth_state = MYSQL_AUTH_RECV;
                /**
                 * Create session, and a router session for it.
                 * If successful, there will be backend connection(s)
                 * after this point.
                 */
                session = session_alloc(dcb->service, dcb);

                if (session != NULL)
                {
                    CHK_SESSION(session);
                    ss_dassert(session->state != SESSION_STATE_ALLOC &&
                               session->state != SESSION_STATE_DUMMY);

                    protocol->protocol_auth_state = MYSQL_IDLE;
                    /**
                     * Send an AUTH_OK packet to the client,
                     * packet sequence is # packet_number
                     */
                    mysql_send_ok(dcb, packet_number, 0, NULL);
                }
                else
                {
                    auth_val = MYSQL_AUTH_NO_SESSION;
                }
            }
            if (MYSQL_AUTH_SUCCEEDED != auth_val && MYSQL_AUTH_SSL_INCOMPLETE != auth_val)
            {
                protocol->protocol_auth_state = MYSQL_AUTH_FAILED;
                mysql_client_auth_error_handling(dcb, auth_val);
                /**
                 * Close DCB and which will release MYSQL_session
                 */
                dcb_close(dcb);
            }
            /* One way or another, the buffer is now fully processed */
            gwbuf_free(read_buffer);
            read_buffer = NULL;
        }
        break;

        /**
         *
         * Once a client connection is authenticated, the protocol authentication
         * state will be MYSQL_IDLE and so every event of data received will
         * result in a call that comes to this section of code.
         *
         */
    case MYSQL_IDLE:
        {
    ROUTER_OBJECT *router = NULL;
    ROUTER *router_instance = NULL;
    void *rsession = NULL;
    uint8_t cap = 0;
    bool stmt_input = false; /*< router input type */
    SESSION *session = dcb->session;
    if (session != NULL && SESSION_STATE_DUMMY != session->state)
    {
        CHK_SESSION(session);
        router = session->service->router;
        router_instance = session->service->router_instance;
        rsession = session->router_session;

        if (NULL == router_instance || NULL == rsession)
        {
            /** Send ERR 1045 to client */
            mysql_send_auth_error(dcb,
                                  2,
                                  0,
                                  "failed to create new session");
            gwbuf_free(read_buffer);
            read_buffer = NULL;
            return 0;
        }

        /** Ask what type of input the router expects */
        cap = router->getCapabilities(router_instance, rsession);

        if (cap & RCAP_TYPE_STMT_INPUT)
        {
            stmt_input = true;
            /** Mark buffer to as MySQL type */
            gwbuf_set_type(read_buffer, GWBUF_TYPE_MYSQL);
        }
    }

    /** If the router requires statement input or we are still authenticating
     * we need to make sure that a complete SQL packet is read before continuing */
    if (stmt_input || protocol->protocol_auth_state == MYSQL_AUTH_SENT)
    {

        /**
         * if read queue existed appent read to it.
         * if length of read buffer is less than 3 or less than mysql packet
         *  then return.
         * else copy mysql packets to separate buffers from read buffer and
         * continue.
         * else
         * if read queue didn't exist, length of read is less than 3 or less
         * than mysql packet then
         * create read queue and append to it and return.
         * if length read is less than mysql packet length append to read queue
         * append to it and return.
         * else (complete packet was read) continue.
         */
        if (dcb->dcb_readqueue)
        {
            uint8_t* data;

            dcb->dcb_readqueue = gwbuf_append(dcb->dcb_readqueue, read_buffer);
            nbytes_read = gwbuf_length(dcb->dcb_readqueue);
            data = (uint8_t *)GWBUF_DATA(dcb->dcb_readqueue);
            int plen = MYSQL_GET_PACKET_LEN(data);
            if (nbytes_read < 3 || nbytes_read < MYSQL_GET_PACKET_LEN(data) + 4)
            {
                rc = 0;
                goto return_rc;
            }
            else
            {
                /**
                 * There is at least one complete mysql packet in
                 * read_buffer.
                 */
                read_buffer = dcb->dcb_readqueue;
                dcb->dcb_readqueue = NULL;
            }
        }
        else
        {
            uint8_t* data = (uint8_t *)GWBUF_DATA(read_buffer);

            if (nbytes_read < 3 || nbytes_read < MYSQL_GET_PACKET_LEN(data) + 4)
            {
                dcb->dcb_readqueue = gwbuf_append(dcb->dcb_readqueue, read_buffer);
                rc = 0;
                goto return_rc;
            }
        }

    }

    /**
     * Now there should be at least one complete mysql packet in read_buffer.
     */
            uint8_t* payload = NULL;
            session_state_t ses_state;

            session = dcb->session;
            ss_dassert(session!= NULL && SESSION_STATE_DUMMY != session->state);

            if (session != NULL)
            {
                CHK_SESSION(session);
            }
            spinlock_acquire(&session->ses_lock);
            ses_state = session->state;
            spinlock_release(&session->ses_lock);
            /* Now, we are assuming in the first buffer there is
             * the information form mysql command */
            payload = GWBUF_DATA(read_buffer);

            if (ses_state == SESSION_STATE_ROUTER_READY)
            {
                /** Route COM_QUIT to backend */
                if (MYSQL_IS_COM_QUIT(payload))
                {
                    /**
                     * Sends COM_QUIT packets since buffer is already
                     * created. A BREF_CLOSED flag is set so dcb_close won't
                     * send redundant COM_QUIT.
                     */
                    /* Temporarily suppressed: SESSION_ROUTE_QUERY(session, read_buffer); */
                    /* Replaced with freeing the read buffer. */
                    gwbuf_free(read_buffer);
                    read_buffer = NULL;
                    /**
                     * Close router session which causes closing of backends.
                     */
                    dcb_close(dcb);
                }
                else
                {
                    /** Reset error handler when routing of the new query begins */
                    dcb->dcb_errhandle_called = false;

                    if (stmt_input)
                    {
                        /**
                         * Feed each statement completely and separately
                         * to router.
                         */
                        rc = route_by_statement(session, &read_buffer);

                        if (read_buffer != NULL)
                        {
                            /** add incomplete mysql packet to read queue */
                            dcb->dcb_readqueue = gwbuf_append(dcb->dcb_readqueue, read_buffer);
                            read_buffer = NULL;
                        }
                    }
                    else if (NULL != session->router_session || cap & RCAP_TYPE_NO_RSESSION)
                    {
                        /** Feed whole packet to router */
                        rc = SESSION_ROUTE_QUERY(session, read_buffer);
                        read_buffer = NULL;
                    }
                    else
                    {
                        rc = 0;
                    }

                    /** Routing succeed */
                    if (rc)
                    {
                        rc = 0; /**< here '0' means success */
                    }
                    else
                    {
                        bool succp;
                        GWBUF* errbuf;
                        /**
                         * Create error to be sent to client if session
                         * can't be continued.
                         */
                        errbuf = mysql_create_custom_error(1,
                                                           0,
                                                           "Routing failed. Session is closed.");
                        /**
                         * Ensure that there are enough backends
                         * available.
                         */
                        router->handleError(router_instance,
                                            session->router_session,
                                            errbuf,
                                            dcb,
                                            ERRACT_NEW_CONNECTION,
                                            &succp);
                        gwbuf_free(errbuf);
                        /**
                         * If there are not enough backends close
                         * session
                         */
                        if (!succp)
                        {
                            MXS_ERROR("Routing the query failed. "
                                      "Session will be closed.");

                        }
                        gwbuf_free(read_buffer);
                        read_buffer = NULL;
                    }
                }
            }
            else
            {
                MXS_INFO("Session received a query in state %s",
                         STRSESSIONSTATE(ses_state));
                while ((read_buffer = GWBUF_CONSUME_ALL(read_buffer)) != NULL)
                {
                    ;
                }
                goto return_rc;
            }
            goto return_rc;
        } /*  MYSQL_IDLE */
        break;

    default:
        break;
    }
    rc = 0;

return_rc:
#if defined(SS_DEBUG)
    if (dcb->state == DCB_STATE_POLLING ||
        dcb->state == DCB_STATE_NOPOLLING ||
        dcb->state == DCB_STATE_ZOMBIE)
    {
        CHK_PROTOCOL(protocol);
    }
#endif
    return rc;
}

/**
 * @brief Analyse authentication errors and write appropriate log messages
 *
 * @param dcb Request handler DCB connected to the client
 * @param auth_val The type of authentication failure
 * @note Authentication status codes are defined in mysql_client_server_protocol.h
 */
static void
mysql_client_auth_error_handling(DCB *dcb, int auth_val)
{
    int packet_number, message_len;
    char *fail_str = NULL;

    packet_number = ssl_required_by_dcb(dcb) ? 3 : 2;

    switch (auth_val)
    {
    case MYSQL_AUTH_NO_SESSION:
        MXS_DEBUG("%lu [gw_read_client_event] session "
            "creation failed. fd %d, "
            "state = MYSQL_AUTH_NO_SESSION.",
            pthread_self(),
            dcb->fd);

        /** Send ERR 1045 to client */
        mysql_send_auth_error(dcb,
            packet_number,
            0,
            "failed to create new session");
    case MYSQL_FAILED_AUTH_DB:
        MXS_DEBUG("%lu [gw_read_client_event] database "
            "specified was not valid. fd %d, "
            "state = MYSQL_FAILED_AUTH_DB.",
            pthread_self(),
            dcb->fd);
        /** Send error 1049 to client */
        message_len = 25 + MYSQL_DATABASE_MAXLEN;

        fail_str = calloc(1, message_len+1);
        snprintf(fail_str, message_len, "Unknown database '%s'",
        (char*)((MYSQL_session *)dcb->data)->db);

        modutil_send_mysql_err_packet(dcb, packet_number, 0, 1049, "42000", fail_str);
    case MYSQL_FAILED_AUTH_SSL:
        MXS_DEBUG("%lu [gw_read_client_event] client is "
            "not SSL capable for SSL listener. fd %d, "
            "state = MYSQL_FAILED_AUTH_SSL.",
            pthread_self(),
            dcb->fd);

        /** Send ERR 1045 to client */
        mysql_send_auth_error(dcb,
            packet_number,
            0,
            "failed to complete SSL authentication");
    case MYSQL_AUTH_SSL_INCOMPLETE:
        MXS_DEBUG("%lu [gw_read_client_event] unable to "
            "complete SSL authentication. fd %d, "
            "state = MYSQL_AUTH_SSL_INCOMPLETE.",
            pthread_self(),
            dcb->fd);

        /** Send ERR 1045 to client */
        mysql_send_auth_error(dcb,
            packet_number,
            0,
            "failed to complete SSL authentication");
    case MYSQL_FAILED_AUTH:
        MXS_DEBUG("%lu [gw_read_client_event] authentication failed. fd %d, "
            "state = MYSQL_FAILED_AUTH.",
            pthread_self(),
            dcb->fd);
        /** Send error 1045 to client */
        fail_str = create_auth_fail_str((char *)((MYSQL_session *)dcb->data)->user,
            dcb->remote,
            (char*)((MYSQL_session *)dcb->data)->client_sha1,
            (char*)((MYSQL_session *)dcb->data)->db, auth_val);
        modutil_send_mysql_err_packet(dcb, packet_number, 0, 1045, "28000", fail_str);
    default:
        MXS_DEBUG("%lu [gw_read_client_event] authentication failed. fd %d, "
            "state unrecognized.",
            pthread_self(),
            dcb->fd);
        /** Send error 1045 to client */
        fail_str = create_auth_fail_str((char *)((MYSQL_session *)dcb->data)->user,
            dcb->remote,
            (char*)((MYSQL_session *)dcb->data)->client_sha1,
            (char*)((MYSQL_session *)dcb->data)->db, auth_val);
        modutil_send_mysql_err_packet(dcb, packet_number, 0, 1045, "28000", fail_str);
    }
    free(fail_str);
}

///////////////////////////////////////////////
// client write event to Client triggered by EPOLLOUT
//////////////////////////////////////////////
/**
 * @node Client's fd became writable, and EPOLLOUT event
 * arrived. As a consequence, client input buffer (writeq) is flushed.
 *
 * Parameters:
 * @param dcb - in, use
 *          client dcb
 *
 * @return constantly 1
 *
 *
 * @details (write detailed description here)
 *
 */
int gw_write_client_event(DCB *dcb)
{
    MySQLProtocol *protocol = NULL;

    CHK_DCB(dcb);

    ss_dassert(dcb->state != DCB_STATE_DISCONNECTED);

    if (dcb == NULL)
    {
        goto return_1;
    }

    if (dcb->state == DCB_STATE_DISCONNECTED)
    {
        goto return_1;
    }

    if (dcb->protocol == NULL)
    {
        goto return_1;
    }
    protocol = (MySQLProtocol *)dcb->protocol;
    CHK_PROTOCOL(protocol);

    if (protocol->protocol_auth_state == MYSQL_IDLE)
    {
        dcb_drain_writeq(dcb);
        goto return_1;
    }

return_1:
#if defined(SS_DEBUG)
    if (dcb->state == DCB_STATE_POLLING ||
        dcb->state == DCB_STATE_NOPOLLING ||
        dcb->state == DCB_STATE_ZOMBIE)
    {
        CHK_PROTOCOL(protocol);
    }
#endif
    return 1;
}

/**
 * Bind the DCB to a network port or a UNIX Domain Socket.
 * @param listen_dcb Listener DCB
 * @param config_bind Bind address in either IP:PORT format for network sockets or PATH
 *                    for UNIX Domain Sockets
 * @return 1 on success, 0 on error
 */
int gw_MySQLListener(DCB *listen_dcb, char *config_bind)
{
    if (dcb_listen(listen_dcb, config_bind) < 0)
    {
        return 0;
    }
#if defined(FAKE_CODE)
    conn_open[l_so] = true;
#endif /* FAKE_CODE */
    listen_dcb->func.accept = gw_MySQLAccept;

    return 1;
}


/**
 * @node Accept a new connection, using the DCB code for the basic work
 *
 * For as long as dcb_accept can return new client DCBs for new connections,
 * continue to loop. The code will always give a failure return, since it
 * continues to try to create new connections until a failure occurs.
 *
 * @param listener - The Listener DCB that picks up new connection requests
 * @return 0 in success, 1 in failure
 *
 */
int gw_MySQLAccept(DCB *listener)
{
    DCB *client_dcb;
    MySQLProtocol *protocol;

    CHK_DCB(listener);

    while ((client_dcb = dcb_accept(listener)) != NULL)
    {
        CHK_DCB(client_dcb);
        protocol = mysql_protocol_init(client_dcb, client_dcb->fd);

        if (protocol == NULL)
        {
            /** delete client_dcb */
            dcb_close(client_dcb);
            MXS_ERROR("%lu [gw_MySQLAccept] Failed to create "
                      "protocol object for client connection.",
                      pthread_self());
            break;
        }
        CHK_PROTOCOL(protocol);
        client_dcb->protocol = protocol;
        // assign function pointers to "func" field
        memcpy(&client_dcb->func, &MyObject, sizeof(GWPROTOCOL));
        //send handshake to the client_dcb
        MySQLSendHandshake(client_dcb);

        // client protocol state change
        protocol->protocol_auth_state = MYSQL_AUTH_SENT;

        /**
         * Set new descriptor to event set. At the same time,
         * change state to DCB_STATE_POLLING so that
         * thread which wakes up sees correct state.
         */
        if (poll_add_dcb(client_dcb) == -1)
        {
            /* Send a custom error as MySQL command reply */
            mysql_send_custom_error(client_dcb,
                                    1,
                                    0,
                                    "MaxScale encountered system limit while "
                                    "attempting to register on an epoll instance.");

            /** close client_dcb */
            dcb_close(client_dcb);

            /** Previous state is recovered in poll_add_dcb. */
            MXS_ERROR("%lu [gw_MySQLAccept] Failed to add dcb %p for "
                      "fd %d to epoll set.",
                      pthread_self(),
                      client_dcb,
                      client_dcb->fd);
            break;
        }
        else
        {
            MXS_DEBUG("%lu [gw_MySQLAccept] Added dcb %p for fd "
                      "%d to epoll set.",
                      pthread_self(),
                      client_dcb,
                      client_dcb->fd);
        }
    } /**< while client_dcb != NULL */

    /* Must have broken out of while loop or received NULL client_dcb */
    return 1;
}

static int gw_error_client_event(DCB* dcb)
{
    SESSION* session;

    CHK_DCB(dcb);

    session = dcb->session;

    MXS_DEBUG("%lu [gw_error_client_event] Error event handling for DCB %p "
              "in state %s, session %p.",
              pthread_self(),
              dcb,
              STRDCBSTATE(dcb->state),
              (session != NULL ? session : NULL));

    if (session != NULL && session->state == SESSION_STATE_STOPPING)
    {
        goto retblock;
    }

#if defined(SS_DEBUG)
    MXS_DEBUG("Client error event handling.");
#endif
    dcb_close(dcb);

retblock:
    return 1;
}

static int
gw_client_close(DCB *dcb)
{
    SESSION* session;
    ROUTER_OBJECT* router;
    void* router_instance;
#if defined(SS_DEBUG)
    MySQLProtocol* protocol = (MySQLProtocol *)dcb->protocol;

    if (dcb->state == DCB_STATE_POLLING ||
        dcb->state == DCB_STATE_NOPOLLING ||
        dcb->state == DCB_STATE_ZOMBIE)
    {
        if (!DCB_IS_CLONE(dcb))
        {
            CHK_PROTOCOL(protocol);
        }
    }
#endif
    MXS_DEBUG("%lu [gw_client_close]", pthread_self());
    mysql_protocol_done(dcb);
    session = dcb->session;
    /**
     * session may be NULL if session_alloc failed.
     * In that case, router session wasn't created.
     */
    if (session != NULL && SESSION_STATE_DUMMY != session->state)
    {
        CHK_SESSION(session);
        spinlock_acquire(&session->ses_lock);

        if (session->state != SESSION_STATE_STOPPING)
        {
            session->state = SESSION_STATE_STOPPING;
        }
        router_instance = session->service->router_instance;
        router = session->service->router;
        /**
         * If router session is being created concurrently router
         * session might be NULL and it shouldn't be closed.
         */
        if (session->router_session != NULL)
        {
            spinlock_release(&session->ses_lock);
            /** Close router session and all its connections */
            router->closeSession(router_instance, session->router_session);
        }
        else
        {
            spinlock_release(&session->ses_lock);
        }
    }
    return 1;
}

/**
 * Handle a hangup event on the client side descriptor.
 *
 * We simply close the DCB, this will propogate the closure to any
 * backend descriptors and perform the session cleanup.
 *
 * @param dcb           The DCB of the connection
 */
static int gw_client_hangup_event(DCB *dcb)
{
    SESSION* session;

    CHK_DCB(dcb);
    session = dcb->session;

    if (session != NULL && session->state == SESSION_STATE_ROUTER_READY)
    {
        CHK_SESSION(session);
    }

    if (session != NULL && session->state == SESSION_STATE_STOPPING)
    {
        goto retblock;
    }

    dcb_close(dcb);

retblock:
    return 1;
}


/**
 * Detect if buffer includes partial mysql packet or multiple packets.
 * Store partial packet to dcb_readqueue. Send complete packets one by one
 * to router.
 *
 * It is assumed readbuf includes at least one complete packet.
 * Return 1 in success. If the last packet is incomplete return success but
 * leave incomplete packet to readbuf.
 *
 * @param session       Session pointer
 * @param p_readbuf     Pointer to the address of GWBUF including the query
 *
 * @return 1 if succeed,
 */
static int route_by_statement(SESSION* session, GWBUF** p_readbuf)
{
    int rc;
    GWBUF* packetbuf;
#if defined(SS_DEBUG)
    GWBUF* tmpbuf;

    tmpbuf = *p_readbuf;
    while (tmpbuf != NULL)
    {
        ss_dassert(GWBUF_IS_TYPE_MYSQL(tmpbuf));
        tmpbuf=tmpbuf->next;
    }
#endif
    do
    {
        ss_dassert(GWBUF_IS_TYPE_MYSQL((*p_readbuf)));

        /**
         * Collect incoming bytes to a buffer until complete packet has
         * arrived and then return the buffer.
         */
        packetbuf = gw_MySQL_get_next_packet(p_readbuf);

        if (packetbuf != NULL)
        {
            CHK_GWBUF(packetbuf);
            ss_dassert(GWBUF_IS_TYPE_MYSQL(packetbuf));
            /**
             * This means that buffer includes exactly one MySQL
             * statement.
             * backend func.write uses the information. MySQL backend
             * protocol, for example, stores the command identifier
             * to protocol structure. When some other thread reads
             * the corresponding response the command tells how to
             * handle response.
             *
             * Set it here instead of gw_read_client_event to make
             * sure it is set to each (MySQL) packet.
             */
            gwbuf_set_type(packetbuf, GWBUF_TYPE_SINGLE_STMT);
            /** Route query */
            rc = SESSION_ROUTE_QUERY(session, packetbuf);
        }
        else
        {
            rc = 1;
            goto return_rc;
        }
    }
    while (rc == 1 && *p_readbuf != NULL);

return_rc:
    return rc;
}
