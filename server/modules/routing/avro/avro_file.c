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

/**
 * Prepare an existing binlog file to be appened to.
 *
 * @param router	The router instance
 * @param file		The binlog file name
 */
bool avr_file_open(int *int, char *file)
{
    char path[PATH_MAX + 1] = "";
    int fd;

    strcpy(path, router->binlogdir);
    strcat(path, "/");
    strcat(path, file);

    if ((fd = open(path, O_RDWR | O_APPEND, 0666)) == -1)
    {
        MXS_ERROR("Failed to open binlog file %s for append.",
                  path);
        return;
    }
    fsync(fd);
    if (router->binlog_fd != -1)
    {
        close(router->binlog_fd);
    }
    spinlock_acquire(&router->binlog_lock);
    memmove(router->binlog_name, file, BINLOG_FNAMELEN);
    router->current_pos = lseek(fd, 0L, SEEK_END);
    if (router->current_pos < 4)
    {
        if (router->current_pos == 0)
        {
            if (blr_file_add_magic(fd))
            {
                router->current_pos = BINLOG_MAGIC_SIZE;
                router->binlog_position = BINLOG_MAGIC_SIZE;
                router->current_safe_event = BINLOG_MAGIC_SIZE;
                router->last_written = BINLOG_MAGIC_SIZE;
                router->last_event_pos = 0;
            }
            else
            {
                MXS_ERROR("%s: Could not write magic to binlog file.", router->service->name);
            }
        }
        else
        {
            /* If for any reason the file's length is between 1 and 3 bytes
             * then report an error. */
            MXS_ERROR("%s: binlog file %s has an invalid length %lu.",
                      router->service->name, path, router->current_pos);
            close(fd);
            spinlock_release(&router->binlog_lock);
            return;
        }
    }
    router->binlog_fd = fd;
    spinlock_release(&router->binlog_lock);
}

