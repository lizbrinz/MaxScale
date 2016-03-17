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
 * Copyright MariaDB Corporation Ab 2016
 */

#include <maxavro.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>

const char *testfile = "test.db";
const char *testschema = "";

void write_file()
{
    FILE *file = fopen(testfile, "wb");
}

int main(int argc, char** argv)
{
    MAXAVRO_FILE *file = maxavro_file_open(testfile);

    if(!file)
    {
        return 1;
    }

    uint64_t blocks = 0;

    while (maxavro_next_block(file))
    {
        blocks++;
    }

    uint64_t blocksread = file->blocks_read;
    maxavro_file_close(file);
    return blocks != blocksread;
}
