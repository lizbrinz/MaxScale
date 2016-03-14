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

int main(int argc, char** argv)
{

    if (argc < 2)
    {
        printf("Usage: %s FILE\n", argv[0]);
        return 1;
    }

    bool verbose = false;
    avro_file_t *file = avro_open_file(argv[1]);

    if(!file)
    {
        return 1;
    }

    printf("File sync marker: ");
    for (int i = 0; i < sizeof(file->sync); i++)
    {
            printf("%hhx", file->sync[i]);
    }
    printf("\n");

    uint64_t total_objects = 0, total_bytes = 0, data_blocks = 0;

    /** After the header come the data blocks. Each data block has the number of objects
     * in this block and the size of the compressed block encoded as Avro long values
     * followed by the actual data. Each data block ends with an identical, 16 byte sync marker
     * which can be checked to make sure the file is not corrupted. */
    do
    {
        uint64_t objects, data_size;
        if(avro_read_datablock_start(file, &objects, &data_size))
        {
            /** Skip data block */
            fseek(file->file, data_size, SEEK_CUR);
            data_blocks++;
            total_objects += objects;
            total_bytes += data_size;

            if (verbose)
            {
                printf("Block %lu: %lu objects, %lu bytes\n", data_blocks, objects, data_size);
            }
        }
        else
        {
            avro_close_file(file);
            return 1;
        }
    }
    while (avro_verify_block(file));
    

    printf("%s: %lu blocks, %lu objects and %lu bytes\n", argv[1], data_blocks, total_objects, total_bytes);

    avro_close_file(file);
    return 0;
}
