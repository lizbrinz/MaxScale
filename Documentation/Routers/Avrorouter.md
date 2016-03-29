# Avrorouter

The avrorouter is a MariaDB 10.0 binary log to Avro file converter. It consumes
binary logs from a local directory and transforms them into a set of Avro files.
These files can then be queried by clients for various purposes.

This router is intended to be used in tandem with the [Binlogrouter](Binlogrouter.md).
The binlogrouter can connect to a master server and request binlog records. These
records can then consumed by the avrorouter directly from the binlog cache of
the binlogrouter. This allows MaxScale to automatically transform binlog events
on the master to local Avro format files.

The converted Avro files can be requested with the CDC protocol. This protocol
should be used to communicate with the avrorouter and currently it is the only
supported protocol. The clients can request either Avro or JSON format data
streams from a database table.

# Configuration

For information about common service parameters, refer to the
[Configuration Guide](../Getting-Started/Configuration-Guide.md).

## Mandatory Router Parameters

The `router_options` is the only mandatory parameter for avrorouter. It is
the main way the avrorouter is configured and it will be covered in detail
in the next section.

## Router Options

The avrorouter is configured with a comma-separated list of key-value pairs.
Currently the router has two mandatory parameters, `binlogdir` and `avrodir`.
The following options should be given as a value to the `router_options` parameter.

### `binlogdir`

The location of the binary log files. This is the first mandatory parameter
and it defines where the module will read binlog files from. 
If used in conjunction with the binlogrouter, the value of this option should
be the same for both the binlogrouter and the avrorouter.

### `avrodir`

The location where the Avro files are stored. This is the second mandatory
parameter and it governs where the converted files are stored. This directory
will be used to store the Avro files, plain-text Avro schemas and other files
needed by the avrorouter.

### `filestem`

The base name of the binlog files. The default value is "mysql-bin". The binlog
files are assumed to follow the naming schema _<filestem>.<N>_ where _<N>_ is
the binlog number and _<filestem>_ is the value of this router option.

### `start_index`

The starting index number of the binlog file. The default value is 1.
For the binlog _mysql-bin.000001_ the index would be 1, for _mysql-bin.000005_
the index would be 5. If you need to start from a binlog file other than 1,
you need to set the value of this option to the correct index.

### Avro file options

These options control how large the Avro file data blocks can get.
Increasing or lowering the block size could have a positive effect
depending on your use case. For more information about the Avro file
format and how it organizes data, refer to the [Avro documentation](https://avro.apache.org/docs/current/).

The avrorouter will flush a block and start a new one when either `group_trx`
transactions or `group_rows` row events have been processed. Changing these
options will also allow more frequent updates to stored data but this
will cause a small increase in file size and search times.

#### `group_trx`

Controls the number of transactions that are grouped into a single Avro
data block. The default value is 50 transactions.


#### `group_rows`

Controls the number of row events that are grouped into a single Avro
data block. The default value is 1000 row events.


## Examples

The avrorouter does not have any examples yet.
