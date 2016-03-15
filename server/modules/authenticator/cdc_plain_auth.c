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
 * @file cdc_auth.c
 *
 * CDC Authentication module for handling the checking of clients credentials
 * in the CDC protocol.
 *
 * @verbatim
 * Revision History
 * Date         Who                     Description
 * 11/03/2016   Massimiliano Pinto      Initial version
 *
 * @endverbatim
 */

#include <gw_authenticator.h>
#include <cdc.h>

MODULE_INFO info =
{
    MODULE_API_AUTHENTICATOR,
    MODULE_GA,
    GWAUTHENTICATOR_VERSION,
    "The CDC client to MaxScale authenticator implementation"
};

static char *version_str = "V1.0.0";

static int  cdc_auth_set_protocol_data(DCB *dcb, GWBUF *buf);
static bool cdc_auth_is_client_ssl_capable(DCB *dcb);
static int  cdc_auth_authenticate(DCB *dcb);
static void cdc_auth_free_client_data(DCB *dcb);

/*
 * The "module object" for mysql client authenticator module.
 */
static GWAUTHENTICATOR MyObject =
{
    cdc_auth_set_protocol_data,           /* Extract data into structure   */
    cdc_auth_is_client_ssl_capable,       /* Check if client supports SSL  */
    cdc_auth_authenticate,                /* Authenticate user credentials */
    cdc_auth_free_client_data,            /* Free the client data held in DCB */
};

static int cdc_auth_check(
    DCB           *dcb,
    CDC_protocol  *protocol,
    char          *username,
    uint8_t       *auth_data,
    unsigned int  *flags
);

static int cdc_auth_set_client_data(
    CDC_session *client_data,
    CDC_protocol *protocol,
    uint8_t *client_auth_packet,
    int client_auth_packet_size
);

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
GWAUTHENTICATOR* GetModuleObject()
{
    return &MyObject;
}

/**
 * @brief Function to easily call authentication check.
 *
 * @param dcb Request handler DCB connected to the client
 * @param protocol  The protocol structure for the connection
 * @param username  String containing username
 * @param auth_data  The encrypted password for authentication
 * @return Authentication status
 * @note Authentication status codes are defined in cdc.h
 */
static int cdc_auth_check(DCB *dcb, CDC_protocol *protocol, char *username, uint8_t *auth_data, unsigned int *flags)
{
    if (strcmp(username, "massi") == 0)
        return CDC_STATE_AUTH_OK;
    else  
        return CDC_STATE_AUTH_FAILED;
}

/**
 * @brief Authenticates a CDC user who is a client to MaxScale.
 *
 * @param dcb Request handler DCB connected to the client
 * @return Authentication status
 * @note Authentication status codes are defined in cdc.h
 */
static int
cdc_auth_authenticate(DCB *dcb)
{
    CDC_protocol *protocol = DCB_PROTOCOL(dcb, CDC_protocol);
    CDC_session *client_data = (CDC_session *)dcb->data;
    int auth_ret;

    if (0 == strlen(client_data->user))
    {
        auth_ret = CDC_STATE_AUTH_ERR;
    }
    else
    {
        MXS_DEBUG("Receiving connection from '%s'",
                   client_data->user);

        auth_ret = cdc_auth_check(dcb, protocol, client_data->user, client_data->auth_data, client_data->flags);

        /* On failed authentication try to load user table from backend database */
        /* Success for service_refresh_users returns 0 */

        /*
        if (CDC_STATE_AUTH_OK != auth_ret && 0 == service_refresh_users(dcb->service))
        {
            auth_ret = cdc_auth_check(dcb, client_data->auth_token, client_data->auth_token_len, protocol,
                client_data->user, client_data->client_sha1, client_data->db);
        }
        */

        /* on successful authentication, set user into dcb field */
        if (CDC_STATE_AUTH_OK == auth_ret)
        {
            dcb->user = strdup(client_data->user);
        }
        else if (dcb->service->log_auth_warnings)
        {
            MXS_NOTICE("%s: login attempt for user '%s', authentication failed.",
                   dcb->service->name, client_data->user);
            if (dcb->ipv4.sin_addr.s_addr == 0x0100007F &&
                !dcb->service->localhost_match_wildcard_host)
            {
                MXS_NOTICE("If you have a wildcard grant that covers"
                       " this address, try adding "
                       "'localhost_match_wildcard_host=true' for "
                       "service '%s'. ", dcb->service->name);
            }
        }
    }

    return auth_ret;
}

/**
 * @brief Transfer data from the authentication request to the DCB.
 *
 * The request handler DCB has a field called data that contains protocol
 * specific information. This function examines a buffer containing CDC 
 * authentication data and puts it into a structure that is referred to
 * by the DCB. If the information in the buffer is invalid, then a failure
 * code is returned. A call to cdc_auth_set_client_data does the
 * detailed work.
 *
 * @param dcb Request handler DCB connected to the client
 * @param buffer Pointer to pointer to buffer containing data from client
 * @return Authentication status
 * @note Authentication status codes are defined in cdc.h
 */
static int
cdc_auth_set_protocol_data(DCB *dcb, GWBUF *buf)
{
    uint8_t *client_auth_packet = GWBUF_DATA(buf);
    CDC_protocol *protocol = NULL;
    CDC_session *client_data = NULL;
    int client_auth_packet_size = 0;

    protocol = DCB_PROTOCOL(dcb, CDC_protocol);
    CHK_PROTOCOL(protocol);
    if (dcb->data == NULL)
    {
        if (NULL == (client_data = (CDC_session *)calloc(1, sizeof(CDC_session))))
        {
            return CDC_STATE_AUTH_ERR;
        }
        dcb->data = client_data;
    }
    else
    {
        client_data = (CDC_session *)dcb->data;
    }

    client_auth_packet_size = gwbuf_length(buf);

    return cdc_auth_set_client_data(client_data, protocol, client_auth_packet,
        client_auth_packet_size);
}

/**
 * @brief Transfer detailed data from the authentication request to the DCB.
 *
 * The caller has created the data structure pointed to by the DCB, and this
 * function fills in the details. If problems are found with the data, the
 * return code indicates failure.
 *
 * @param client_data The data structure for the DCB
 * @param protocol The protocol structure for this connection
 * @param client_auth_packet The data from the buffer received from client
 * @param client_auth_packet size An integer giving the size of the data
 * @return Authentication status
 * @note Authentication status codes are defined in cdc.h
 */
static int
cdc_auth_set_client_data(
    CDC_session *client_data,
    CDC_protocol *protocol,
    uint8_t *client_auth_packet,
    int client_auth_packet_size)
{
    char username[CDC_USER_MAXLEN + 1] = "";
    char auth_data[SHA_DIGEST_LENGTH] = "";
    unsigned int flags[2];
    int decoded_size = client_auth_packet_size / 2;
    int user_len = (client_auth_packet_size <= CDC_USER_MAXLEN) ? client_auth_packet_size : CDC_USER_MAXLEN;
    uint8_t *decoded_buffer = malloc(decoded_size);
    uint8_t *tmp_ptr;

    /* decode input data */
    gw_hex2bin(decoded_buffer, (const char *)client_auth_packet, decoded_size);
    if ((tmp_ptr = (uint8_t *)strchr((char *)decoded_buffer, ':')) != NULL)
    {
        *tmp_ptr++ = '\0';
    }
    else
        return CDC_STATE_AUTH_ERR;

    strncpy(client_data->user, (char *)decoded_buffer, user_len);
    client_data->user[user_len] = '\0';

    return CDC_STATE_AUTH_OK;
}

/**
 * @brief Determine whether the client is SSL capable
 *
 * The authentication request from the client will indicate whether the client
 * is expecting to make an SSL connection. The information has been extracted
 * in the previous functions.
 *
 * @param dcb Request handler DCB connected to the client
 * @return Boolean indicating whether client is SSL capable
 */
static bool
cdc_auth_is_client_ssl_capable(DCB *dcb)
{
    return false;
}

/**
 * @brief Free the client data pointed to by the passed DCB.
 *
 * Currently all that is required is to free the storage pointed to by
 * dcb->data.  But this is intended to be implemented as part of the
 * authentication API at which time this code will be moved into the
 * CDC authenticator.  If the data structure were to become more complex
 * the mechanism would still work and be the responsibility of the authenticator.
 * The DCB should not know authenticator implementation details.
 *
 * @param dcb Request handler DCB connected to the client
 */
static void
cdc_auth_free_client_data(DCB *dcb)
{
    free(dcb->data);
}
