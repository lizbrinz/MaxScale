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
 * Copyright MariaDB Corporation Ab 2014
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <dcb.h>
#include <buffer.h>
#include <gw_protocol.h>
#include <service.h>
#include <session.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <router.h>
#include <maxscale/poll.h>
#include <atomic.h>
#include <gw.h>
#include <adminusers.h>
#include <skygw_utils.h>
#include <log_manager.h>
#include <modinfo.h>
#include <maxscaled.h>

 /* @see function load_module in load_utils.c for explanation of the following
  * lint directives.
 */
/*lint -e14 */
MODULE_INFO info =
{
    MODULE_API_PROTOCOL,
    MODULE_GA,
    GWPROTOCOL_VERSION,
    "A maxscale protocol for the administration interface"
};
/*lint +e14 */

/**
 * @file maxscaled.c - MaxScale administration protocol
 *
 *
 * @verbatim
 * Revision History
 * Date         Who                     Description
 * 13/06/2014   Mark Riddoch            Initial implementation
 * 07/07/15     Martin Brampton         Correct failure handling
 *
 * @endverbatim
 */

static char *version_str = "V1.1.0";

static int maxscaled_read_event(DCB* dcb);
static int maxscaled_write_event(DCB *dcb);
static int maxscaled_write(DCB *dcb, GWBUF *queue);
static int maxscaled_error(DCB *dcb);
static int maxscaled_hangup(DCB *dcb);
static int maxscaled_accept(DCB *dcb);
static int maxscaled_close(DCB *dcb);
static int maxscaled_listen(DCB *dcb, char *config);
static char *mxsd_default_auth();

/**
 * The "module object" for the maxscaled protocol module.
 */
static GWPROTOCOL MyObject =
{
    maxscaled_read_event,           /**< Read - EPOLLIN handler        */
    maxscaled_write,                /**< Write - data from gateway     */
    maxscaled_write_event,          /**< WriteReady - EPOLLOUT handler */
    maxscaled_error,                /**< Error - EPOLLERR handler      */
    maxscaled_hangup,               /**< HangUp - EPOLLHUP handler     */
    maxscaled_accept,               /**< Accept                        */
    NULL,                           /**< Connect                       */
    maxscaled_close,                /**< Close                         */
    maxscaled_listen,               /**< Create a listener             */
    NULL,                           /**< Authentication                */
    NULL,                           /**< Session                       */
    mxsd_default_auth               /**< Default authenticator         */
};

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 *
 * @see function load_module in load_utils.c for explanation of the following
 * lint directives.
 */
/*lint -e14 */
char*  version()
{
    return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void ModuleInit()
{
    MXS_INFO("Initialise MaxScaled Protocol module.");;
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
/*lint +e14 */

/**
 * The default authenticator name for this protocol
 *
 * @return name of authenticator
 */
static char *mxsd_default_auth()
{
    return "MaxAdminAuth";
}

/**
 * Read event for EPOLLIN on the maxscaled protocol module.
 *
 * @param dcb   The descriptor control block
 * @return
 */
static int maxscaled_read_event(DCB* dcb)
{
    int n;
    GWBUF *head = NULL;
    MAXSCALED *maxscaled = (MAXSCALED *)dcb->protocol;

    if ((n = dcb_read(dcb, &head, 0)) != -1)
    {
        if (head)
        {
            if (GWBUF_LENGTH(head))
            {
                switch (maxscaled->state)
                {
                case MAXSCALED_STATE_LOGIN:
                    gwbuf_free(dcb->dcb_readqueue);
                    dcb->dcb_readqueue = head;
                    maxscaled->state = MAXSCALED_STATE_PASSWD;
                    dcb_printf(dcb, "PASSWORD");
                    break;

                case MAXSCALED_STATE_PASSWD:
                    dcb->dcb_readqueue = gwbuf_append(dcb->dcb_readqueue, head);
                    if (0 == dcb->authfunc.extract(dcb, dcb->dcb_readqueue) &&
                        0 == dcb->authfunc.authenticate(dcb))
                    {
                        dcb_printf(dcb, "OK----");
                        maxscaled->state = MAXSCALED_STATE_DATA;
                    }
                    else
                    {
                        dcb_printf(dcb, "FAILED");
                        maxscaled->state = MAXSCALED_STATE_LOGIN;
                    }
                    gwbuf_free(dcb->dcb_readqueue);
                    dcb->dcb_readqueue = NULL;
                    break;

                case MAXSCALED_STATE_DATA:
                    SESSION_ROUTE_QUERY(dcb->session, head);
                    dcb_printf(dcb, "OK");
                    break;
                }
            }
            else
            {
                // Force the free of the buffer header
                gwbuf_free(head);
            }
        }
    }
    return n;
}

/**
 * EPOLLOUT handler for the maxscaled protocol module.
 *
 * @param dcb   The descriptor control block
 * @return
 */
static int maxscaled_write_event(DCB *dcb)
{
    return dcb_drain_writeq(dcb);
}

/**
 * Write routine for the maxscaled protocol module.
 *
 * Writes the content of the buffer queue to the socket
 * observing the non-blocking principles of MaxScale.
 *
 * @param dcb   Descriptor Control Block for the socket
 * @param queue Linked list of buffes to write
 */
static int maxscaled_write(DCB *dcb, GWBUF *queue)
{
    int rc;
    rc = dcb_write(dcb, queue);
    return rc;
}

/**
 * Handler for the EPOLLERR event.
 *
 * @param dcb   The descriptor control block
 */
static int maxscaled_error(DCB *dcb)
{
    return 0;
}

/**
 * Handler for the EPOLLHUP event.
 *
 * @param dcb   The descriptor control block
 */
static int maxscaled_hangup(DCB *dcb)
{
    dcb_close(dcb);
    return 0;
}

/**
 * Handler for the EPOLLIN event when the DCB refers to the listening
 * socket for the protocol.
 *
 * @param dcb   The descriptor control block
 * @return The number of new connections created
 */
static int maxscaled_accept(DCB *listener)
{
    int n_connect = 0;
    DCB *client_dcb;

    while ((client_dcb = dcb_accept(listener, &MyObject)) != NULL)
    {
        MAXSCALED *maxscaled_protocol = NULL;

        if ((maxscaled_protocol = (MAXSCALED *)calloc(1, sizeof(MAXSCALED))) == NULL)
        {
            dcb_close(client_dcb);
            continue;
        }
        maxscaled_protocol->username = NULL;
        spinlock_init(&maxscaled_protocol->lock);
        client_dcb->protocol = (void *)maxscaled_protocol;

        client_dcb->session = session_alloc(listener->session->service, client_dcb);

        if (NULL == client_dcb->session || poll_add_dcb(client_dcb))
        {
            dcb_close(client_dcb);
            continue;
        }
        n_connect++;
        maxscaled_protocol->state = MAXSCALED_STATE_LOGIN;
        dcb_printf(client_dcb, "USER");
    }
    return n_connect;
}

/**
 * The close handler for the descriptor. Called by the gateway to
 * explicitly close a connection.
 *
 * @param dcb   The descriptor control block
 */

static int maxscaled_close(DCB *dcb)
{
    MAXSCALED *maxscaled = dcb->protocol;

    if (!maxscaled)
    {
        return 0;
    }

    spinlock_acquire(&maxscaled->lock);
    if (maxscaled->username)
    {
        free(maxscaled->username);
        maxscaled->username = NULL;
    }
    spinlock_release(&maxscaled->lock);

    return 0;
}

/**
 * Maxscale daemon listener entry point
 *
 * @param       listener        The Listener DCB
 * @param       config          Configuration (ip:port)
 * @return      0 on failure, 1 on success
 */
static int maxscaled_listen(DCB *listener, char *config)
{
    return (dcb_listen(listener, config, "MaxScale Admin") < 0) ? 0 : 1;
}
