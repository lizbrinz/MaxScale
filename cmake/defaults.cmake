# Default configuration values
#
# You can change these through CMake by adding -D<variable>=<value> when
# configuring the build.

# Use C99
set(USE_C99 TRUE CACHE BOOL "Use C99 standard")

# Install the template maxscale.cnf file
set(WITH_MAXSCALE_CNF TRUE CACHE BOOL "Install the template maxscale.cnf file")

# hostname or IP address of MaxScale's host
set(TEST_HOST "127.0.0.1" CACHE STRING "hostname or IP address of MaxScale's host")

# port of read connection router module
set(TEST_PORT "4008" CACHE STRING "port of read connection router module")

# port of read/write split router module
set(TEST_PORT_RW "4006" CACHE STRING "port of read/write split router module")

# port of schemarouter router module
set(TEST_PORT_DB "4010" CACHE STRING "port of schemarouter router module")

# port of read/write split router module with hints
set(TEST_PORT_RW_HINT "4009" CACHE STRING "port of read/write split router module with hints")

# master test server server_id
set(TEST_MASTER_ID "3000" CACHE STRING "master test server server_id")

# master test server port
set(MASTER_PORT "3000" CACHE STRING "master test server port")

# username of MaxScale user
set(TEST_USER "maxuser" CACHE STRING "username of MaxScale user")

# password of MaxScale user
set(TEST_PASSWORD "maxpwd" CACHE STRING "password of MaxScale user")

# Use static version of libmysqld
set(STATIC_EMBEDDED TRUE CACHE BOOL "Use static version of libmysqld")

# Build RabbitMQ components
set(BUILD_RABBITMQ TRUE CACHE BOOL "Build RabbitMQ components")

# Build the binlog router
set(BUILD_BINLOG TRUE CACHE BOOL "Build binlog router")

# Build the multimaster monitor
set(BUILD_MMMON TRUE CACHE BOOL "Build multimaster monitor")

# Build Luafilter
set(BUILD_LUAFILTER FALSE CACHE BOOL "Build Luafilter")

# Use gcov build flags
set(GCOV FALSE CACHE BOOL "Use gcov build flags")

# Install init.d scripts and ldconf configuration files
set(WITH_SCRIPTS TRUE CACHE BOOL "Install init.d scripts and ldconf configuration files")

# Use tcmalloc as the memory allocator
set(WITH_TCMALLOC FALSE CACHE BOOL "Use tcmalloc as the memory allocator")

# Use jemalloc as the memory allocator
set(WITH_JEMALLOC FALSE CACHE BOOL "Use jemalloc as the memory allocator")

# Build tests
set(BUILD_TESTS FALSE CACHE BOOL "Build tests")

# Build packages
set(PACKAGE FALSE CACHE BOOL "Enable package building (this disables local installation of system files)")

# Build extra tools
set(BUILD_TOOLS FALSE CACHE BOOL "Build extra utility tools")

# Profiling
set(PROFILE FALSE CACHE BOOL "Profiling (gprof)")
