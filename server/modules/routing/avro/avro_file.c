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
 * Date		Who			Description
 * 25/02/2016	Markus Mäkelä   Initial implementation
 *
 * @endverbatim
 */

#include <mxs_avro.h>
#include <limits.h>
#include <log_manager.h>
#include <fcntl.h>

/**
 * Prepare an existing binlog file to be appened to.
 *
 * @param router	The router instance
 * @param file		The binlog file name
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
    fsync(fd);
    close(fd);
}
