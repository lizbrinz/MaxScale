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
 * @file null_auth.c
 *
 * Null Authentication module for handling the checking of clients credentials
 * for protocols that do not have authentication, either temporarily or
 * permanently
 *
 * @verbatim
 * Revision History
 * Date         Who                     Description
 * 14/03/2016   Martin Brampton         Initial version
 *
 * @endverbatim
 */

#include <gw_authenticator.h>
#include <modinfo.h>
#include <dcb.h>
#include <buffer.h>

MODULE_INFO info =
{
    MODULE_API_AUTHENTICATOR,
    MODULE_GA,
    GWAUTHENTICATOR_VERSION,
    "The Null client authenticator implementation"
};

static char *version_str = "V1.0.0";

static int null_auth_set_protocol_data(DCB *dcb, GWBUF *buf);
static bool null_auth_is_client_ssl_capable(DCB *dcb);
static int null_auth_authenticate(DCB *dcb, GWBUF **buffer);
static void null_auth_free_client_data(DCB *dcb);

/*
 * The "module object" for mysql client authenticator module.
 */
static GWAUTHENTICATOR MyObject =
{
    null_auth_set_protocol_data,           /* Extract data into structure   */
    null_auth_is_client_ssl_capable,       /* Check if client supports SSL  */
    null_auth_authenticate,                /* Authenticate user credentials */
    null_auth_free_client_data,            /* Free the client data held in DCB */
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
GWAUTHENTICATOR* GetModuleObject()
{
    return &MyObject;
}

/**
 * @brief Null authentication of a user.
 *
 * Always returns success
 *
 * @param dcb Request handler DCB connected to the client
 * @param buffer Pointer to pointer to buffer containing data from client
 * @return Authentication status - always 0 to denote success
 */
static int
null_auth_authenticate(DCB *dcb, GWBUF **buffer)
{
    return 0;
}

/**
 * @brief Transfer data from the authentication request to the DCB.
 *
 * Does not actually transfer any data
 *
 * @param dcb Request handler DCB connected to the client
 * @param buffer Pointer to pointer to buffer containing data from client
 * @return Authentication status - always 0 to indicate success
 */
static int
null_auth_set_protocol_data(DCB *dcb, GWBUF *buf)
{
    return 0;
}

/**
 * @brief Determine whether the client is SSL capable
 *
 * Always say that client is SSL capable.  The null authenticator cannot be
 * used in a context where the client is not SSL capable.
 *
 * @param dcb Request handler DCB connected to the client
 * @return Boolean indicating whether client is SSL capable - always true
 */
static bool
null_auth_is_client_ssl_capable(DCB *dcb)
{
    return true;
}

/**
 * @brief Free the client data pointed to by the passed DCB.
 *
 * The null authenticator does not allocate any data, so nothing to do.
 *
 * @param dcb Request handler DCB connected to the client
 */
static void
null_auth_free_client_data(DCB *dcb) {}
