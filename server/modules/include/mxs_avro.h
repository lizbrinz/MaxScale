/**
 * MaxScale AVRO router
 *
 */

/**
 * How often to call the router status function (seconds)
 */
#define AVRO_STATS_FREQ          60
#define AVRO_NSTATS_MINUTES      30

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
#ifdef BLFILE_IN_SLAVE
        BLFILE          *file;          /*< Currently open avro file */
#endif
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
    HASHTABLE     *schemas;
    char              prevbinlog[BINLOG_FNAMELEN+1];
    int               rotating;     /*< Rotation in progress flag */
    SPINLOCK          fileslock;    /*< Lock for the files queue above */
    AVRO_ROUTER_STATS      stats;        /*< Statistics for this router */
    struct avro_instance  *next;
} AVRO_INSTANCE;

extern int avro_client_request(AVRO_INSTANCE *, AVRO_CLIENT *, GWBUF *);
extern void avro_client_rotate(AVRO_INSTANCE *router, AVRO_CLIENT *client, uint8_t *ptr);
