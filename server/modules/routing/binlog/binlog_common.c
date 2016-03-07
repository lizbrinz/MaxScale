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

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdio.h>
#include <unistd.h>
#include <binlog_common.h>
#include <blr_constants.h>
#include <log_manager.h>

/**
 * @file binlog_common.c - Common binary log code shared between multiple modules
 *
 * This file contains functions that are common to multiple modules that all
 * handle MySQL/MariaDB binlog files.
 *
 * @verbatim
 * Revision History
 *
 * Date     Who              Description
 * 7/3/16   Markus Makela    Initial implementation
 *
 * @endverbatim
 */

/**
 * Get the next binlog file name.
 *
 * @param router	The router instance
 * @return 		0 on error, >0 as sequence number
 */
int
blr_file_get_next_binlogname(const char *binlog_name)
{
    char *sptr;
    int filenum;

    if ((sptr = strrchr(binlog_name, '.')) == NULL)
        return 0;
    filenum = atoi(sptr + 1);
    if (filenum)
        filenum++;

    return filenum;
}

/**
 * 
 * @param binlogdir
 * @param binlog
 * @return 
 */
bool blr_next_binlog_exists(const char* binlogdir, const char* binlog)
{
    bool rval = false;
    int filenum = blr_file_get_next_binlogname(binlog);

    if (filenum)
    {
        char *sptr = strrchr(binlog, '.');

        if (sptr)
        {
            char buf[BLRM_BINLOG_NAME_STR_LEN +1];
            char filename[PATH_MAX +1];
            char next_file[BLRM_BINLOG_NAME_STR_LEN + 1];
            int offset = sptr - binlog;
            strncpy(buf, binlog, offset);
            buf[offset] ='\0';
            sprintf(next_file, BINLOG_NAMEFMT, buf, filenum);
            snprintf(filename, PATH_MAX, "%s/%s", binlogdir, next_file);
            filename[PATH_MAX] = '\0';

            /* Next file in sequence doesn't exist */
            if (access(filename, R_OK) == -1)
            {
                MXS_DEBUG("This file is still being written.");
            }
            else
            {
                MXS_NOTICE("Warning: the next binlog file %s exists: "
                    "the current binlog file is missing Rotate or Stop event. "
                    "Client should read next one", next_file);
                rval = true;
            }
        }
    }

    return rval;
}
