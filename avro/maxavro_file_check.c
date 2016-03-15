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
#include <getopt.h>
#include <skygw_debug.h>

static int verbose = 0;
static uint64_t seekto = 0;
static int64_t num_rows = -1;
static bool dump = false;

int check_file(const char* filename)
{
    maxavro_file_t *file = avro_file_open(filename);

    if (!file)
    {
        return 1;
    }

    int rval = 0;

    if (!dump)
    {
        printf("File sync marker: ");
        for (int i = 0; i < sizeof(file->sync); i++)
        {
            printf("%hhx", file->sync[i]);
        }
        printf("\n");
    }

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
            if (seekto > 0)
            {
                avro_record_seek(file, seekto);
                seekto = 0;
            }
            total_records += records;
            total_bytes += data_size;
            data_blocks++;

            if (verbose > 1 || dump)
            {
                json_t* row;
                while (num_rows != 0 && (row = avro_record_read(file)))
                {
                    char *json = json_dumps(row, JSON_PRESERVE_ORDER);
                    printf("%s\n", json);
                    json_decref(row);
                    if (num_rows > 0)
                    {
                        num_rows--;
                    }
                }
            }
            else
            {
                /** Skip the data */
                fseek(file->file, data_size, SEEK_CUR);
            }

            if (verbose && !dump)
            {
                printf("Block %lu: %lu records, %lu bytes\n", data_blocks, records, data_size);
            }
        }
        else
        {
            break;
        }
    }
    while (num_rows != 0 && avro_verify_block(file));

    if (!avro_file_is_eof(file) && num_rows != 0)
    {
        printf("Failed to read next data block after data block %lu. "
               "Read %lu records and %lu bytes before failure.\n",
               data_blocks, total_records, total_bytes);
        rval = 1;
    }
    else if (!dump)
    {
        printf("%s: %lu blocks, %lu records and %lu bytes\n", filename, data_blocks, total_records, total_bytes);
    }


    avro_file_close(file);
    return rval;
}

static struct option long_options[] =
{
    {"verbose",   no_argument, 0, 'v'},
    {"dump",  no_argument, 0, 'd'},
    {"from",  no_argument, 0, 'f'},
    {"count", no_argument, 0, 'c'},
    {0, 0, 0, 0}
};

int main(int argc, char** argv)
{

    if (argc < 2)
    {
        printf("Usage: %s FILE\n", argv[0]);
        return 1;
    }

    char c;
    int option_index;

    while ((c = getopt_long(argc, argv, "vdf:c:", long_options, &option_index)) >= 0)
    {
        switch (c)
        {
            case 'v':
                verbose++;
                break;
            case 'd':
                dump = true;
                break;
            case 'f':
                seekto = strtol(optarg, NULL, 10);
                break;
            case 'c':
                num_rows = strtol(optarg, NULL, 10);
                break;
        }
    }

    int rval = 0;
    char pathbuf[PATH_MAX + 1];
    for (int i = optind; i < argc; i++)
    {
        if (check_file(realpath(argv[i], pathbuf)))
        {
            rval = 1;
        }
    }
    return rval;
}
