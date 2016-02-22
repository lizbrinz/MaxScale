/*
 * This file is distributed as part of MaxScale from MariaDB Corporation.  It is free
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
 * @file mysql_utils.c  - Binary MySQL data processing utilities
 *
 * This file contains functions that are used when processing binary format
 * information. The MySQL protocol uses the binary format in result sets and
 * row based replication.
 */

#include <mysql_utils.h>
#include <string.h>
#include <stdbool.h>
#include <log_manager.h>
#include <skygw_debug.h>

/**
 * @brief Calculate the length of a length-encoded integer in bytes
 *
 * @param ptr Start of the length encoded value
 * @return Number of bytes before the actual value
 */
size_t leint_bytes(uint8_t* ptr)
{
    uint8_t val = *ptr;
    if (val < 0xfb)
    {
        return 1;
    }
    else if (val == 0xfc)
    {
        return 3;
    }
    else if (val == 0xfd)
    {
        return 4;
    }
    else
    {
        return 9;
    }
}

/**
 * @brief Converts a length-encoded integer to @c uint64_t
 *
 * @see https://dev.mysql.com/doc/internals/en/integer.html
 * @param c Pointer to the first byte of a length-encoded integer
 * @return The value converted to a standard unsigned integer
 */
uint64_t leint_value(uint8_t* c)
{
    uint64_t sz = 0;

    if (*c < 0xfb)
    {
        sz = *c;
    }
    else if (*c == 0xfc)
    {
        memcpy(&sz, c + 1, 2);
    }
    else if (*c == 0xfd)
    {
        memcpy(&sz, c + 1, 3);
    }
    else if (*c == 0xfe)
    {
        memcpy(&sz, c + 1, 8);
    }
    else
    {
        ss_dassert(*c == 0xff);
        MXS_ERROR("Unexpected length encoding '%x' encountered when reading "
                  "length-encoded integer.", *c);
    }

    return sz;
}

/**
 * Converts a length-encoded integer into a standard unsigned integer
 * and advances the pointer to the next unrelated byte.
 *
 * @param c Pointer to the first byte of a length-encoded integer
 */
uint64_t leint_consume(uint8_t ** c)
{
    uint64_t rval = leint_value(*c);
    *c += leint_bytes(*c);
    return rval;
}

/**
 * @brief Consume and duplicate a length-encoded string
 *
 * Converts a length-encoded string to a C string and advances the pointer to
 * the first byte after the string. The caller is responsible for freeing
 * the returned string.
 * @param c Pointer to the first byte of a valid packet.
 * @return The newly allocated string or NULL if memory allocation failed
 */
char* lestr_consume_dup(uint8_t** c)
{
    uint64_t slen = leint_consume(c);
    char *str = malloc((slen + 1) * sizeof(char));

    if (str)
    {
        memcpy(str, *c, slen);
        str[slen] = '\0';
        *c += slen;
    }

    return str;
}

/**
 * @brief Consume a length-encoded string
 *
 * Converts length-encoded strings to character strings and advanced
 * the pointer to the next unrelated byte.
 * @param c Pointer to the start of the length-encoded string
 * @param size Pointer to a variable where the size of the string is stored
 * @return Pointer to the start of the string
 */
char* lestr_consume(uint8_t** c, size_t *size)
{
    uint64_t slen = leint_consume(c);
    *size = slen;
    char* start = (char*) *c;
    *c += slen;
    return start;
}
