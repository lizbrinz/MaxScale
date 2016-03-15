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
#include <limits.h>

int check_file(const char* filename)
{
    bool verbose = false;
    avro_file_t *file = avro_file_open(filename);

    if (!file)
    {
        return 1;
    }

    int rval = 0;
    printf("File sync marker: ");
    for (int i = 0; i < sizeof(file->sync); i++)
    {
        printf("%hhx", file->sync[i]);
    }
    printf("\n");

    uint64_t total_records = 0, total_bytes = 0, data_blocks = 0;

    /** After the header come the data blocks. Each data block has the number of records
     * in this block and the size of the compressed block encoded as Avro long values
     * followed by the actual data. Each data block ends with an identical, 16 byte sync marker
     * which can be checked to make sure the file is not corrupted. */
    do
    {
        uint64_t records, data_size;
        if (avro_read_datablock_start(file, &records, &data_size))
        {
            /** Skip data block */
            fseek(file->file, data_size, SEEK_CUR);
            data_blocks++;
            total_records += records;
            total_bytes += data_size;

            if (verbose)
            {
                printf("Block %lu: %lu records, %lu bytes\n", data_blocks, records, data_size);
            }
        }
        else
        {
            break;
        }
    }
    while (avro_verify_block(file));

    if (!avro_file_is_eof(file))
    {
        printf("Failed to read next data block after data block %lu. "
               "Read %lu records and %lu bytes before failure.\n",
               data_blocks, total_records, total_bytes);
        rval = 1;
    }
    else
    {
        printf("%s: %lu blocks, %lu records and %lu bytes\n", filename, data_blocks, total_records, total_bytes);
    }


    avro_file_close(file);
    return rval;
}

int main(int argc, char** argv)
{

    if (argc < 2)
    {
        printf("Usage: %s FILE\n", argv[0]);
        return 1;
    }

    int rval = 0;
    char pathbuf[PATH_MAX + 1];
    for (int i = 1; i < argc; i++)
    {
        if (check_file(realpath(argv[i], pathbuf)))
        {
            rval = 1;
        }
    }
    return rval;
}
