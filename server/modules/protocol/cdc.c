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
 * Copyright MariaDB Corporation Ab 2013-2016
 */

/**
 * @file cdc.c - Change Data Capture Listener protocol module
 *
 * The change data capture protocol module is intended as a mechanism to allow connections
 * into maxscale for the purpose of accessing information within
 * the maxscale with a Change Data Capture API interface (supporting Avro right now)
 * databases.
 *
 * In the first instance it is intended to connect, authenticate and retieve data in the Avro format
 * as requested by compatible clients.
 *
 * @verbatim
 * Revision History
 * Date		Who			Description
 * 11/01/2016	Massimiliano Pinto	Initial version
 *
 * @endverbatim
 */

#include <cdc.h>
#include <gw.h>
#include <modinfo.h>
#include <log_manager.h>

MODULE_INFO info = {
	MODULE_API_PROTOCOL,
	MODULE_IN_DEVELOPMENT,
	GWPROTOCOL_VERSION,
	"A Change Data Cappure Listener implementation for use in binlog events retrievial"
};

#define ISspace(x) isspace((int)(x))
#define CDC_SERVER_STRING "MaxScale(c) v.1.0.0"
static char *version_str = "V1.0.1";

static int cdc_read_event(DCB* dcb);
static int cdc_write_event(DCB *dcb);
static int cdc_write(DCB *dcb, GWBUF *queue);
static int cdc_error(DCB *dcb);
static int cdc_hangup(DCB *dcb);
static int cdc_accept(DCB *dcb);
static int cdc_close(DCB *dcb);
static int cdc_listen(DCB *dcb, char *config);
static CDC_protocol *cdc_protocol_init(DCB* dcb);
static void cdc_protocol_done(DCB* dcb);
static int cdc_do_registration(DCB *dcb, GWBUF *data);
static int do_auth(DCB *dcb, GWBUF *buffer, void *data);
static void write_auth_ack(DCB *dcb);
static void write_auth_err(DCB *dcb);

/**
 * The "module object" for the CDC protocol module.
 */
static GWPROTOCOL MyObject =
{ 
    cdc_read_event,      /* Read - EPOLLIN handler        */
    cdc_write,           /* Write - data from gateway     */
    cdc_write_event,     /* WriteReady - EPOLLOUT handler */
    cdc_error,           /* Error - EPOLLERR handler      */
    cdc_hangup,          /* HangUp - EPOLLHUP handler     */
    cdc_accept,          /* Accept                        */
    NULL,                /* Connect                       */
    cdc_close,           /* Close                         */
    cdc_listen,          /* Create a listener             */
    NULL,                /* Authentication                */
    NULL                 /* Session                       */
};

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 */
char *
version()
{
    return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void
ModuleInit()
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
GWPROTOCOL *
GetModuleObject()
{
    return &MyObject;
}

/**
 * Read event for EPOLLIN on the CDC protocol module.
 *
 * @param dcb    The descriptor control block
 * @return
 */
static int
cdc_read_event(DCB* dcb)
{
    SESSION *session = dcb->session;
    CDC_protocol *protocol = (CDC_protocol *)dcb->protocol;
    int n;
    GWBUF *head = NULL;
    int rc;
    CDC_session *client_data;
    // AUTH_OBJ *auth;
    int auth_rc;

    client_data = (CDC_session *)dcb->data;

    if ((n = dcb_read(dcb, &head, 0)) != -1)
    {
        if (head)
        {
            if (GWBUF_LENGTH(head))
            {
                switch (protocol->state)
                {
                    case CDC_STATE_WAIT_FOR_AUTH:
                        auth_rc = do_auth(dcb, head, (void *)client_data);

                        while ((head = gwbuf_consume(head, GWBUF_LENGTH(head))) != NULL);

                        if (auth_rc == 1)
                        {
                             protocol->state = CDC_STATE_REGISTRATION;

                             write_auth_ack(dcb);

                             MXS_INFO("%s: Client [%s] authenticated with user [%s]",
                                      dcb->service->name, dcb->remote != NULL ? dcb->remote : "",
                                      client_data->user);
                             break;
                        }
                        else
                        {
                            protocol->state = CDC_STATE_AUTH_ERR;

                            write_auth_err(dcb);
                            MXS_ERROR("%s: authentication failure from [%s], user [%s]",
                                       dcb->service->name, dcb->remote != NULL ? dcb->remote : "", client_data->user);

                            /* force the client connecton close */
                            dcb_close(dcb);

                            return 0;
                        }
                    case CDC_STATE_REGISTRATION:
                        {
                           /**
                            * Registration in CDC is not part of authentication:
                            * 
                            * It culd be done in protocol or in router.
                            * Levaing in the protocol for now.
                            *
                            * If registration succeeds it should set:
                            * uuid and type
                            */

                           if (cdc_do_registration(dcb, head))
                           {
                               MXS_INFO("%s: Client [%s] has completd REGISTRATION action",
                                         dcb->service->name,
                                         dcb->remote != NULL ? dcb->remote : "");

                               protocol->state = CDC_STATE_HANDLE_REQUEST;

                               dcb_printf(dcb, "OK");

                               // start a real session
                               session = session_alloc(dcb->service, dcb);

                               // discard data
                               while ((head = gwbuf_consume(head, GWBUF_LENGTH(head))) != NULL);

                               break;
                           }
                           else
                           {
                               // discard data
                               while ((head = gwbuf_consume(head, GWBUF_LENGTH(head))) != NULL);
                               dcb_printf(dcb, "ERR, code 12, msg: abcd");
                               
                               /* force the client connecton close */
                               dcb_close(dcb);
                               return 0;
                           }
                        }
                    case CDC_STATE_HANDLE_REQUEST:
                        // handle CLOSE command, it shoudl be routed as well and closed after last transmission
                        if (strncmp(GWBUF_DATA(head), "CLOSE", GWBUF_LENGTH(head)) == 0)
                        {
                            MXS_INFO("%s: Client [%s] has requested CLOSE action",
                                     dcb->service->name, dcb->remote != NULL ? dcb->remote : "");

                            // gwbuf_set_type(head, GWBUF_TYPE_CDC);
                            // the router will close the client connection
                            //rc = SESSION_ROUTE_QUERY(session, head);
                            
                            // buffer not handled by router right now
                            while ((head = gwbuf_consume(head, GWBUF_LENGTH(head))) != NULL);

                            /* right now, just force the client connecton close */
                            dcb_close(dcb);
                            return 0;
                        }
                        else
                        {
                            char *request = strndup(GWBUF_DATA(head), GWBUF_LENGTH(head));

                            // gwbuf_set_type(head, GWBUF_TYPE_CDC);
                            rc = SESSION_ROUTE_QUERY(session, head);

                            MXS_INFO("%s: Client [%s] requested [%s] action", dcb->service->name,
                                     dcb->remote != NULL ? dcb->remote : "", request);

                            free(request);

                            // buffer not handled by router right now
                            while ((head = gwbuf_consume(head, GWBUF_LENGTH(head))) != NULL);

                            break;
                        }
                    default:
                        MXS_INFO("%s: Client [%s] in unknown state %d", dcb->service->name,
                                 dcb->remote != NULL ? dcb->remote : "", protocol->state);
                        while ((head = gwbuf_consume(head, GWBUF_LENGTH(head))) != NULL);

                        return 0;
                }
            }
            else
            {
                // Force the free of the buffer header
                while ((head = gwbuf_consume(head, GWBUF_LENGTH(head))) != NULL);
            }
        }
    }

    return n;
}

/**
 * EPOLLOUT handler for the CDC protocol module.
 *
 * @param dcb    The descriptor control block
 * @return
 */
static int
cdc_write_event(DCB *dcb)
{
    return dcb_drain_writeq(dcb);
}

/**
 * Write routine for the CDC protocol module.
 *
 * Writes the content of the buffer queue to the socket
 * observing the non-blocking principles of the gateway.
 *
 * @param dcb	Descriptor Control Block for the socket
 * @param queue	Linked list of buffes to write
 */
static int
cdc_write(DCB *dcb, GWBUF *queue)
{
    int rc;
    rc = dcb_write(dcb, queue);
    return rc;
}

/**
 * Handler for the EPOLLERR event.
 *
 * @param dcb    The descriptor control block
 */
static int
cdc_error(DCB *dcb)
{
    dcb_close(dcb);
    return 0;
}

/**
 * Handler for the EPOLLHUP event.
 *
 * @param dcb    The descriptor control block
 */
static int
cdc_hangup(DCB *dcb)
{
    dcb_close(dcb);
    return 0;
}

/**
 * Handler for the EPOLLIN event when the DCB refers to the listening
 * socket for the protocol.
 *
 * @param dcb    The descriptor control block
 */
static int
cdc_accept(DCB *dcb)
{
int n_connect = 0;

    while (1)
    {
        int so = -1;
        DCB *client = NULL;
        CDC_session *client_data = NULL;
        socklen_t client_len = sizeof(struct sockaddr_storage);
        struct sockaddr client_conn;
        CDC_protocol *protocol;

        if ((so = accept(dcb->fd, (struct sockaddr *)&client_conn, &client_len)) == -1)
            return n_connect;
        else
        {
            atomic_add(&dcb->stats.n_accepts, 1);

            /* set NONBLOCKING mode */
            setnonblocking(so);

            /* create DCB for new connection */
            if ((client = dcb_alloc(DCB_ROLE_REQUEST_HANDLER)))
            {
                client->service = dcb->session->service;	
                client->fd = so;

                /* Dummy session */
                client->session = session_set_dummy(client);

                /* Add new DCB to polling queue */
                if (NULL == client->session || poll_add_dcb(client) == -1)
                {
                    close(so);
                    dcb_close(client);
                    return n_connect;
                }

                // get client address
                if (client_conn.sa_family == AF_UNIX)
                {
                    // set client address
                    client->remote = strdup("localhost_from_socket");
                    // set localhost IP for user authentication
                    (client->ipv4).sin_addr.s_addr = 0x0100007F;
                }
                else
                {
                    /* client IPv4 in raw data*/
                    memcpy(&client->ipv4,
                           (struct sockaddr_in *)&client_conn,
                           sizeof(struct sockaddr_in));

                    /* client IPv4 in string representation */
                    client->remote = (char *)calloc(INET_ADDRSTRLEN + 1, sizeof(char));

                    if (client->remote != NULL)
                    {
                        inet_ntop(AF_INET,
                                  &(client->ipv4).sin_addr,
                                  client->remote,
                                  INET_ADDRSTRLEN);
                    }
                }

                /* allocating CDC protocol */
                protocol = cdc_protocol_init(client);
                if (!protocol)
                {
                    client->protocol = NULL;
                    close(so);
                    dcb_close(client);

                    MXS_ERROR("%lu [cdc_accept] Failed to create "
                              "protocol object for client connection.",
                              pthread_self());
                    return n_connect;
                }
                client->protocol = (CDC_protocol *)protocol;

                /* copy protocol function pointers into new DCB */
                memcpy(&client->func, &MyObject, sizeof(GWPROTOCOL));

                /* create the session data for CDC */
                /* this coud be done in anothe routine, let's keep it here for now */
                client_data = (CDC_session *)calloc(1, sizeof(CDC_session));
                if (client_data == NULL)
                {
                    dcb_close(dcb);
                    return n_connect;
                }

                client->data = client_data;

                /* client protocol state change to CDC_STATE_WAIT_FOR_AUTH */
                protocol->state = CDC_STATE_WAIT_FOR_AUTH;
				
                MXS_NOTICE("%s: new connection from [%s]", client->service->name,
                           client->remote != NULL ? client->remote : "");

                n_connect++;
            }
            else
            {
                close(so);
            }
        }
    }
	
    return n_connect;
}

/**
 * The close handler for the descriptor. Called by the gateway to
 * explicitly close a connection.
 *
 * @param dcb   The descriptor control block
 */
static int
cdc_close(DCB *dcb)
{
    CDC_protocol *p = (CDC_protocol *)dcb->protocol;

    if (!p)
       return 0;

    /* Add deallocate protocol items*/
    cdc_protocol_done(dcb);

    return 1;
}

/**
 * CDC protocol listener entry point
 *
 * @param   listener    The Listener DCB
 * @param   config      Configuration (ip:port)
 */
static int
cdc_listen(DCB *listener, char *config)
{
    int one = 1;
    int syseno = 0;
    int rc;
    struct sockaddr_in addr;

    memcpy(&listener->func, &MyObject, sizeof(GWPROTOCOL));
    if (!parse_bindconfig(config, 6442, &addr))
        return 0;

    if ((listener->fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        return 0;
    }

    /* socket options */
    syseno = setsockopt(listener->fd,
                        SOL_SOCKET,
                        SO_REUSEADDR,
                        (char *)&one,
                        sizeof(one));

    if(syseno != 0){
        char errbuf[STRERROR_BUFLEN];
        MXS_ERROR("Failed to set socket options. Error %d: %s",
                  errno, strerror_r(errno, errbuf, sizeof(errbuf)));
        return 0;
    }
    /* set NONBLOCKING mode */
    setnonblocking(listener->fd);

    /* bind address and port */
    if (bind(listener->fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        return 0;
    }

    rc = listen(listener->fd, SOMAXCONN);
        
    if (rc == 0)
    {
        MXS_NOTICE("Listening CDC connections at %s", config);
    }
    else
    {
        int eno = errno;
        errno = 0;
        char errbuf[STRERROR_BUFLEN];
        MXS_ERROR("Failed to start listening for maxscale CDC connections "
                  "due error %d, %s",
                  eno, strerror_r(eno, errbuf, sizeof(errbuf)));
        return 0;
    }

        
    if (poll_add_dcb(listener) == -1)
    {
        return 0;
    }
    return 1;
}

/**
 * Allocate a new CDC protocol structure
 *
 * @param  dcb    The DCB where protocol is added
 * @return        New allocated protocol or NULL on errors
 *
 */
static CDC_protocol *
cdc_protocol_init(DCB* dcb)
{
    CDC_protocol* p;

    p = (CDC_protocol *) calloc(1, sizeof(CDC_protocol));

    if (p == NULL)
    {
        int eno = errno;
        errno = 0;
        char errbuf[STRERROR_BUFLEN];
        MXS_ERROR("%lu [CDC_protocol_init] CDC protocol init failed : "
                  "memory allocation due error  %d, %s.",
                  pthread_self(),
                  eno,
                  strerror_r(eno, errbuf, sizeof(errbuf)));
        return NULL;
    }

    p->state = CDC_ALLOC;

    spinlock_init(&p->lock);

   /* memory allocation here */
    p->state = CDC_STATE_WAIT_FOR_AUTH;

    CHK_PROTOCOL(p);

    return p;
}

/**
 * Free resources in CDC protocol
 *
 * @param dcb    DCB with allocateid protocol
 *
 */
static void
cdc_protocol_done(DCB* dcb)
{
   CDC_protocol* p = (CDC_protocol *)dcb->protocol;

   if (!p)
      return;

   p = (CDC_protocol *)dcb->protocol;

   spinlock_acquire(&p->lock);

   /* deallocate memory here */

   p->state = CDC_STATE_CLOSE;

   spinlock_release(&p->lock);
}

/**
 * Hande the REGISTRATION comannd in CDC protocol
 *
 * @param dcb    DCB with allocateid protocol
 * @param data   GWBUF with registration message
 * @return       1 for successful registration 0 otherwise
 *
 */
static int
cdc_do_registration(DCB *dcb, GWBUF *data)
{
    int reg_rc = 0;
    int data_len = GWBUF_LENGTH(data) - strlen("REGISTER UUID=");
    char *request = GWBUF_DATA(data);
    // 36 +1
    char uuid[CDC_UUID_LEN + 1];

    if (strstr(request, "REGISTER UUID=") != NULL)
    {
        CDC_session *client_data = (CDC_session *)dcb->data;
        CDC_protocol *protocol = (CDC_protocol *)dcb->protocol;
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

        strcpy(client_data->uuid, uuid);

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

                strncpy(protocol->type, tmp_ptr + 5, cdc_type_len);
                protocol->type[cdc_type_len] = '\0';
                
            }
            else 
                strcpy(protocol->type, "AVRO");
        }
        else
        {
            strcpy(protocol->type, "AVRO");
        }

        return 1;
    }
    else
    {
        return 0;
    } 
}

/**
 * Do authentication against client data
 *
 * @param dcb    Current client DCB
 * @param buffer Authenticatio data from client
 * @param data   Structure that holds auht data
 * @return       1 for successful authentication, 0 otherwise
 *
 */
static int
do_auth(DCB *dcb, GWBUF *buffer, void *data)
{
    CDC_session *client_data = (CDC_session *)data;

    if (strncmp(GWBUF_DATA(buffer), "massi", GWBUF_LENGTH(buffer)) == 0)
    {
	strcpy(client_data->user, "massi");
        return 1;
    }
    else
    {
	strcpy(client_data->user, "foobar");
        return 0;
    }
}

/**
 * Writes Authentication ACK, success
 *
 * @param dcb    Current client DCB
 *
 */
static void
write_auth_ack(DCB *dcb)
{
    dcb_printf(dcb, "OK");
}

/**
 * Writes Authentication ERROR
 *
 * @param dcb    Current client DCB
 *
 */
static void
write_auth_err(DCB *dcb)
{
    dcb_printf(dcb, "ERR, code 11, msg: abcd");
}

