/**
 * MaxScale AVRO router
 *
 */
#ifndef _MXS_AVRO_H
#define _MXS_AVRO_H
#include <stdbool.h>
#include <stdint.h>
#include <blr_constants.h>
#include <dcb.h>
#include <service.h>
#include <spinlock.h>
#include <mysql_binlog.h>
#include <dbusers.h>
#include <avro.h>
#include <cdc.h>

/**
 * How often to call the router status function (seconds)
 */
#define AVRO_STATS_FREQ          60
#define AVRO_NSTATS_MINUTES      30

static char *avro_client_states[] = { "Unregistered", "Registered",
        "Processing", "Errored" };

/** How a binlog file is closed */
typedef enum avro_binlog_end
{
    AVRO_OK = 0, /*< A newer binlog file exists with a rotate event to that file */
    AVRO_LAST_FILE, /* Last binlog which is closed */
    AVRO_OPEN_TRANSACTION, /*< The binlog ends with an open transaction */
    AVRO_BINLOG_ERROR /*< An error occurred while processing the binlog file */
}avro_binlog_end_t;

/**
 * The statistics for this AVRO router instance
 */
typedef struct {
        int             n_clients;      /*< Number slave sessions created     */
        int             n_reads;        /*< Number of record reads */
        uint64_t        n_binlogs;      /*< Number of binlog records from master */
        uint64_t        n_rotates;      /*< Number of binlog rotate events */
        int             n_masterstarts; /*< Number of times connection restarted */
        time_t          lastReply;
        uint64_t        events[MAX_EVENT_TYPE_END + 1]; /*< Per event counters */
        uint64_t        lastsample;
        int             minno;
        int             minavgs[AVRO_NSTATS_MINUTES];
} AVRO_ROUTER_STATS;

/**
 * Client statistics
 */
typedef struct {
        int             n_events;       /*< Number of events sent */
        unsigned long   n_bytes;        /*< Number of bytes sent */
        int             n_requests;     /*< Number of requests received */
        int             n_queries;      /*< Number of queries */
        int             n_failed_read;
        uint64_t        lastsample;
        int             minno;
        int             minavgs[AVRO_NSTATS_MINUTES];
} AVRO_CLIENT_STATS;

typedef struct avro_table_t
{
    char* filename; /*< Absolute filename */
    char* json_schema; /*< JSON representation of the schema */
    avro_file_writer_t avro_file; /*< Current Avro data file */
    avro_value_iface_t *avro_writer_iface; /*< Avro C API writer interface */
    avro_schema_t avro_schema; /*< Native Avro schema of the table */
} AVRO_TABLE;

/**
 * The client structure used within this router.
 * This represents the clients that are requesting AVRO files from MaxScale.
 */
typedef struct avro_client {
#if defined(SS_DEBUG)
        skygw_chk_t     rses_chk_top;
#endif
        DCB             *dcb;           /*< The client DCB */
        int             state;          /*< The state of this client */
        char            *gtid;          /*< GTID the client requests */
        char            *schemaid;      /*< SchemaID the client requests */
        char            avrofile[BINLOG_FNAMELEN+1];
                                        /*< Current avro file for this client */
        char            *uuid;          /*< Client UUID */
        char            *user;          /*< Username if given */
        char            *passwd;        /*< Password if given */
        uint32_t        lastEventTimestamp;/*< Last event timestamp sent */
        SPINLOCK        catch_lock;     /*< Event catchup lock */
        SPINLOCK        rses_lock;      /*< Protects rses_deleted */
        pthread_t       pthread;
        struct avro_instance *router;   /*< Pointer to the owning router */
        struct avro_client *next;
        AVRO_CLIENT_STATS  stats;       /*< Slave statistics */
        time_t          connect_time;   /*< Connect time of slave */
        char            *warning_msg;   /*< Warning message */
        uint8_t         lastEventReceived; /*< Last event received */
#if defined(SS_DEBUG)
        skygw_chk_t     rses_chk_tail;
#endif
} AVRO_CLIENT;

/**
 *  * The per instance data for the AVRO router.
 *   */
typedef struct avro_instance {
    SERVICE                 *service;       /*< Pointer to the service using this router */
    AVRO_CLIENT             *clients;       /*< Link list of all the CDC client connections  */
    SPINLOCK                lock;           /*< Spinlock for the instance data */
    int                     initbinlog;     /*< Initial binlog file number */
    char                    *fileroot;      /*< Root of binlog filename */
    unsigned int            state;          /*< State of the AVRO router */
    uint8_t                 lastEventReceived; /*< Last even received */
    uint32_t                lastEventTimestamp; /*< Timestamp from last event */
    char                    *binlogdir;     /*< The directory where the binlog files are stored */
    char                    *avrodir;       /*< The directory with the AVRO files */
    char                    binlog_name[BINLOG_FNAMELEN+1];
                                            /*< Name of the current binlog file */
    uint64_t                binlog_position;
                                            /*< last committed transaction position */
    uint64_t                current_pos;
                                            /*< Current binlog position */
    int                     binlog_fd;      /*< File descriptor of the binlog file being read */
    uint8_t event_types;
    uint8_t event_type_hdr_lens[MAX_EVENT_TYPE_END];
    char    current_gtid[GTID_MAX_LEN];
    HASHTABLE     *table_maps;
    HASHTABLE     *open_tables;
    HASHTABLE     *created_tables;
    char              prevbinlog[BINLOG_FNAMELEN+1];
    int               rotating;     /*< Rotation in progress flag */
    SPINLOCK          fileslock;    /*< Lock for the files queue above */
    AVRO_ROUTER_STATS      stats;        /*< Statistics for this router */
    int task_delay; /*< Delay in seconds until the next conversion takes place */
    struct avro_instance  *next;
} AVRO_INSTANCE;

extern int avro_client_handle_request(AVRO_INSTANCE *, AVRO_CLIENT *, GWBUF *);
extern void avro_client_rotate(AVRO_INSTANCE *router, AVRO_CLIENT *client, uint8_t *ptr);
extern bool avro_open_binlog(const char *binlogdir, const char *file, int *fd);
extern void avro_close_binlog(int fd);
avro_binlog_end_t avro_read_all_events(AVRO_INSTANCE *router);
AVRO_TABLE* avro_table_alloc(const char* filepath, const char* json_schema);
void* avro_table_free(AVRO_TABLE *table);
void avro_flush_all_tables(AVRO_INSTANCE *router);

#define AVRO_CLIENT_UNREGISTERED 0x0000
#define AVRO_CLIENT_REGISTERED   0x0001
#define AVRO_CLIENT_REQUEST_DATA 0x0002
#define AVRO_CLIENT_ERRORED      0x0003
#define AVRO_CLIENT_MAXSTATE     0x0003

#define AVRO_MAX_FILENAME_LEN         255

#endif
