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
 * @file avro.c - Avro router, allows MaxScale to act as an intermediatory for
 * MySQL replication binlog files and AVRO binary files
 *
 * @verbatim
 * Revision History
 *
 * Date		Who			Description
 * 25/02/2016	Massimiliano Pinto	Initial implementation
 *
 * @endverbatim
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <service.h>
#include <server.h>
#include <router.h>
#include <atomic.h>
#include <spinlock.h>
#include <blr.h>
#include <dcb.h>
#include <spinlock.h>
#include <housekeeper.h>
#include <time.h>

#include <skygw_types.h>
#include <skygw_utils.h>
#include <log_manager.h>

#include <mysql_client_server_protocol.h>
#include <ini.h>
#include <sys/stat.h>

#include <mxs_avro.h>
#include <random_jkiss.h>

#ifndef BINLOG_NAMEFMT
#define BINLOG_NAMEFMT		"%s.%06d"
#endif

static char *version_str = "V1.0.0";

/* The router entry points */
static	ROUTER	*createInstance(SERVICE *service, char **options);
static	void	*newSession(ROUTER *instance, SESSION *session);
static	void 	closeSession(ROUTER *instance, void *router_session);
static	void 	freeSession(ROUTER *instance, void *router_session);
static	int	routeQuery(ROUTER *instance, void *router_session, GWBUF *queue);
static	void	diagnostics(ROUTER *instance, DCB *dcb);
static  void    clientReply(
        ROUTER  *instance,
        void    *router_session,
        GWBUF   *queue,
        DCB     *backend_dcb);
static  void    errorReply(
        ROUTER  *instance,
        void    *router_session,
        GWBUF   *message,
        DCB     *backend_dcb,
        error_action_t     action,
	bool	*succp);

static  int getCapabilities ();
static int avro_set_service_CDC_user(SERVICE *service);
int blr_load_dbusers(AVRO_INSTANCE *router);
int blr_save_dbusers(AVRO_INSTANCE *router);
extern char *decryptPassword(char *crypt);
extern char *create_hex_sha1_sha1_passwd(char *passwd);
extern int MaxScaleUptime();
void converter_func(void* data);
bool blr_next_binlog_exists(const char* binlogdir, const char* binlog);
int blr_file_get_next_binlogname(const char *router);
int blr_read_events_all_events(ROUTER_INSTANCE *router, int fix, int debug);

/** The module object definition */
static ROUTER_OBJECT MyObject = {
    createInstance,
    newSession,
    closeSession,
    freeSession,
    routeQuery,
    diagnostics,
    clientReply,
    errorReply,
    getCapabilities
};

static void	stats_func(void *);

static bool rses_begin_locked_router_action(AVRO_CLIENT *);
static void rses_end_locked_router_action(AVRO_CLIENT *);
int table_id_hash(void *data);
int table_id_cmp(void *a, void *b);
extern void* i64dup(void *data);
extern void* i64free(void *data);

static SPINLOCK	instlock;
static AVRO_INSTANCE *instances;

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 */
char *
version()
{
	return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void
ModuleInit()
{
        MXS_NOTICE("Initialise binlog router module %s.\n", version_str);
        spinlock_init(&instlock);
	instances = NULL;
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
ROUTER_OBJECT *
GetModuleObject()
{
	return &MyObject;
}

/**
 * Create an instance of the router for a particular service
 * within MaxScale.
 *
 * The process of creating the instance causes the router to register
 * with the master server and begin replication of the binlogs from
 * the master server to MaxScale.
 *
 * @param service	The service this router is being create for
 * @param options	An array of options for this query router
 *
 * @return The instance data for this new instance
 */
static	ROUTER	*
createInstance(SERVICE *service, char **options)
{
AVRO_INSTANCE	*inst;
char		*value;
int		i;
unsigned char	*defuuid;
char		path[PATH_MAX+1] = "";
char		filename[PATH_MAX+1] = "";
int		rc = 0;
char		task_name[BLRM_TASK_NAME_LEN+1] = "";

	if(service->credentials.name == NULL ||
	   service->credentials.authdata == NULL)
	{
	    MXS_ERROR("%s: Error: Service is missing user credentials."
                      " Add the missing username or passwd parameter to the service.",
                      service->name);
	    return NULL;
	}

	if(options == NULL || options[0] == NULL)
	{
	    MXS_ERROR("%s: Error: No router options supplied for binlogrouter",
                      service->name);
	    return NULL;
	}

	/* Check for listeners associated to this service */
	if (service->ports == NULL)
	{
	    MXS_ERROR("%s: Error: No listener configured for binlogrouter. Add a listener section in config file.",
                      service->name);
	    return NULL;
	}

	/*
	 * We only support one server behind this router, since the server is
	 * the master from which we replicate binlog records. Therefore check
	 * that only one server has been defined.
	 */
	if (service->dbref != NULL)
	{
		MXS_WARNING("%s: backend database server is provided by master.ini file "
			    "for use with the binlog router."
			    " Server section is no longer required.",
			    service->name);

		server_free(service->dbref->server);
		free(service->dbref);
		service->dbref = NULL;
	}

	if ((inst = calloc(1, sizeof(AVRO_INSTANCE))) == NULL) {
		MXS_ERROR("%s: Error: failed to allocate memory for router instance.",
                          service->name);

		return NULL;
	}

	memset(&inst->stats, 0, sizeof(AVRO_ROUTER_STATS));

	inst->service = service;
	spinlock_init(&inst->lock);
	spinlock_init(&inst->fileslock);

	inst->binlog_fd = -1;

	inst->binlogdir = NULL;
	inst->avrodir = NULL;

	/*
	 * Process the options.
	 * We have an array of attribute values passed to us that we must
	 * examine. Supported attributes are:
	 *	uuid=
	 *	server-id=
	 *	user=
	 *	password=
	 *	master-id=
	 *	filestem=
	 *	lowwater=
	 *	highwater=
	 */
	if (options)
	{
		for (i = 0; options[i]; i++)
		{
			if ((value = strchr(options[i], '=')) == NULL)
			{
                            MXS_WARNING("Unsupported router "
					"option %s for binlog router.",
					options[i]);
			}
			else
			{
				*value = 0;
				value++;

                                if (strcmp(options[i], "binlogdir") == 0)
                                {
                                    inst->binlogdir = strdup(value);
                                    MXS_INFO("reading MySQL binlog files from %s",
                                             inst->binlogdir);
                                }
                                else if (strcmp(options[i], "avrodir") == 0)
                                {
                                    inst->avrodir = strdup(value);
                                    MXS_INFO("AVRO files are in %s",
                                             inst->avrodir);
                                }
				else
				{
					MXS_WARNING("Unsupported router "
                                                    "option %s for AVRO router.",
                                                    options[i]);
				}
			}
		}
	}
	else
	{
		MXS_ERROR("%s: Error: No router options supplied for AVRO router",
                          service->name);
	}

	if (inst->fileroot == NULL)
		inst->fileroot = strdup(BINLOG_NAME_ROOT);

	inst->clients = NULL;
	inst->next = NULL;
	inst->lastEventTimestamp = 0;

	inst->binlog_position = 0;

	sprintf(inst->binlog_name, BINLOG_NAMEFMT, inst->fileroot, 1);
	strcpy(inst->prevbinlog, "");

    /* Hashtable alloc */
    /* Run time errors with i64dup, i64free */
    /*
    if ((inst->table_maps = hashtable_alloc(1000, table_id_hash, table_id_cmp)) &&
        (inst->schemas = hashtable_alloc(1000, table_id_hash, table_id_cmp)))
    {
        hashtable_memory_fns(inst->table_maps, i64dup, NULL, i64free, NULL);
        hashtable_memory_fns(inst->schemas, i64dup, NULL, i64free, NULL);
    }
    else
    {
        hashtable_free(inst->table_maps);
        hashtable_free(inst->schemas);
        MXS_ERROR("Hashtable allocation failed. This is most likely caused "
                  "by a lack of available memory.");
        free(inst);
        return NULL;
    }
    */

    /* Check binlogdir and avrodir */
    // avro_check_dirs(inst);

    /* Allocate dbusers for this router here instead of serviceStartPort() */
    if (service->users == NULL) {
        //service->users = (void *)avro_users_alloc();
        /*
        if (service->users == NULL) {
            MXS_ERROR("%s: Error allocating AVRO users in createInstance",
                      inst->service->name);

            free(inst);
            return NULL;
        }
        */
    }

    /* Set service user or load db users */
    //avro_set_service_CDC_user(inst->service);

    /**
     * Initialise the AVRO router
     */

    /* Find latest binlog file or create a new one (000001) */
    /*
    if (blr_file_init(inst) == 0)
    {
        MXS_ERROR("%s: Service not started due to lack of binlog directory %s",
                  service->name,
                  inst->binlogdir);

        if (service->users) {
            users_free(service->users);
            service->users = NULL;
        }

        free(inst);

        return NULL;
    }
    */

    /**
     * We have completed the creation of the instance data, so now
     * insert this router instance into the linked list of routers
     * that have been created with this module.
     */
    spinlock_acquire(&instlock);
    inst->next = instances;
    instances = inst;
    spinlock_release(&instlock);

    /*
     * Add tasks for statistic computation
     */
    snprintf(task_name, BLRM_TASK_NAME_LEN, "%s stats", service->name);
    hktask_add(task_name, stats_func, inst, AVRO_STATS_FREQ);


    /* Start the scan, read, convert task */
    //hktask_add("binlog_to_avro", converter_func, inst, 5);

    MXS_INFO("AVRO: current MySQL binlog file is %s, pos is %lu\n",
              inst->binlog_name, inst->current_pos);

    return (ROUTER *)inst;
}

/**
 * Associate a new session with this instance of the router.
 *
 * In the case of the binlog router a new session equates to a new slave
 * connecting to MaxScale and requesting binlog records. We need to go
 * through the slave registration process for this new slave.
 *
 * @param instance	The router instance data
 * @param session	The session itself
 * @return Session specific data for this session
 */
static	void	*
newSession(ROUTER *instance, SESSION *session)
{
AVRO_INSTANCE		*inst = (AVRO_INSTANCE *)instance;
AVRO_CLIENT		*client;

        MXS_DEBUG("binlog router: %lu [newSession] new router session with "
                  "session %p, and inst %p.",
                  pthread_self(),
                  session,
                  inst);


	if ((client = (AVRO_CLIENT *)calloc(1, sizeof(AVRO_CLIENT))) == NULL)
	{
		MXS_ERROR("Insufficient memory to create new client session for AVRO router");
                return NULL;
	}

#if defined(SS_DEBUG)
        client->rses_chk_top = CHK_NUM_ROUTER_SES;
        client->rses_chk_tail = CHK_NUM_ROUTER_SES;
#endif

	memset(&client->stats, 0, sizeof(AVRO_CLIENT_STATS));
	atomic_add(&inst->stats.n_clients, 1);
	client->state = BLRS_CREATED;		/* Set initial state of the slave */
	client->uuid = NULL;
        spinlock_init(&client->catch_lock);
	client->dcb = session->client;
	client->router = inst;
#ifdef BLFILE_IN_SLAVE
	client->file = NULL;
#endif
	strcpy(client->avrofile, "unassigned");
	client->connect_time = time(0);
	client->lastEventTimestamp = 0;
	client->lastEventReceived = 0;

	/**
         * Add this session to the list of active sessions.
         */
	spinlock_acquire(&inst->lock);
	client->next = inst->clients;
	inst->clients = client;
	spinlock_release(&inst->lock);

        CHK_CLIENT_RSES(client);

	return (void *)client;
}

/**
 * The session is no longer required. Shutdown all operation and free memory
 * associated with this session. In this case a single session is associated
 * to a slave of MaxScale. Therefore this is called when that slave is no
 * longer active and should remove of reference to that slave, free memory
 * and prevent any further forwarding of binlog records to that slave.
 *
 * Parameters:
 * @param router_instance	The instance of the router
 * @param router_cli_ses 	The particular session to free
 *
 */
static void freeSession(
        ROUTER* router_instance,
        void*   router_client_ses)
{
AVRO_INSTANCE 	*router = (AVRO_INSTANCE *)router_instance;
AVRO_CLIENT	*client = (AVRO_CLIENT *)router_client_ses;
int			prev_val;

        prev_val = atomic_add(&router->stats.n_clients, -1);
        ss_dassert(prev_val > 0);
        (void)prev_val;
	/*
	 * Remove the slave session form the list of slaves that are using the
	 * router currently.
	 */
	spinlock_acquire(&router->lock);
	if (router->clients == client) {
		router->clients = client->next;
        } else {
		AVRO_CLIENT *ptr = router->clients;

		while (ptr != NULL && ptr->next != client) {
			ptr = ptr->next;
                }

		if (ptr != NULL) {
			ptr->next = client->next;
                }
	}
	spinlock_release(&router->lock);

        free(client);
}

/**
 * Close a session with the router, this is the mechanism
 * by which a router may cleanup data structure etc.
 *
 * @param instance		The router instance data
 * @param router_session	The session being closed
 */
static	void
closeSession(ROUTER *instance, void *router_session)
{
AVRO_INSTANCE	 *router = (AVRO_INSTANCE *)instance;
AVRO_CLIENT	 *client = (AVRO_CLIENT *)router_session;

        CHK_CLIENT_RSES(client);

        /**
         * Lock router client session for secure read and update.
         */
        if (rses_begin_locked_router_action(client))
        {
		/* decrease server registered slaves counter */
		atomic_add(&router->stats.n_clients, -1);

		/*
		 * Mark the slave as unregistered to prevent the forwarding
		 * of any more binlog records to this slave.
		 */
		client->state = BLRS_UNREGISTERED;

#if BLFILE_IN_SLAVE
                // TODO: Is it really certain the file can be closed here? If other
                // TODO: threads are using the slave instance, bag things will happen. [JWi].
		if (client->file)
			//blr_close_binlog(router, slave->file);
#endif

                /* Unlock */
                rses_end_locked_router_action(client);
        }
}

/**
 * We have data from the client, this is likely to be packets related to
 * the registration of the slave to receive binlog records. Unlike most
 * MaxScale routers there is no forwarding to the backend database, merely
 * the return of either predefined server responses that have been cached
 * or binlog records.
 *
 * @param instance		The router instance
 * @param router_session	The router session returned from the newSession call
 * @param queue			The queue of data buffers to route
 * @return The number of bytes sent
 */
static	int
routeQuery(ROUTER *instance, void *router_session, GWBUF *queue)
{
AVRO_INSTANCE	*router = (AVRO_INSTANCE *)instance;
AVRO_CLIENT	 *client = (AVRO_CLIENT *)router_session;

	return avro_client_request(router, client, queue);
}

static char *event_names[] = {
	"Invalid", "Start Event V3", "Query Event", "Stop Event", "Rotate Event",
	"Integer Session Variable", "Load Event", "Slave Event", "Create File Event",
	"Append Block Event", "Exec Load Event", "Delete File Event",
	"New Load Event", "Rand Event", "User Variable Event", "Format Description Event",
	"Transaction ID Event (2 Phase Commit)", "Begin Load Query Event",
	"Execute Load Query Event", "Table Map Event", "Write Rows Event (v0)",
	"Update Rows Event (v0)", "Delete Rows Event (v0)", "Write Rows Event (v1)",
	"Update Rows Event (v1)", "Delete Rows Event (v1)", "Incident Event",
	"Heartbeat Event", "Ignorable Event", "Rows Query Event", "Write Rows Event (v2)",
	"Update Rows Event (v2)", "Delete Rows Event (v2)", "GTID Event",
	"Anonymous GTID Event", "Previous GTIDS Event"
};

/* New MariaDB event numbers starts from 0xa0 */
static char *event_names_mariadb10[] = {
	"Annotate Rows Event",
	/* New MariaDB 10.x event numbers */
	"Binlog Checkpoint Event",
	"GTID Event",
	"GTID List Event"
};

/**
 * Display an entry from the spinlock statistics data
 *
 * @param	dcb	The DCB to print to
 * @param	desc	Description of the statistic
 * @param	value	The statistic value
 */
static void
spin_reporter(void *dcb, char *desc, int value)
{
	dcb_printf((DCB *)dcb, "\t\t%-35s	%d\n", desc, value);
}

/**
 * Display router diagnostics
 *
 * @param instance	Instance of the router
 * @param dcb		DCB to send diagnostics to
 */
static	void
diagnostics(ROUTER *router, DCB *dcb)
{
AVRO_INSTANCE	*router_inst = (AVRO_INSTANCE *)router;
AVRO_CLIENT	*session;
int		i = 0, j;
int		minno = 0;
double		min5, min10, min15, min30;
char		buf[40];
struct tm	tm;

	spinlock_acquire(&router_inst->lock);
	session = router_inst->clients;
	while (session)
	{
		i++;
		session = session->next;
	}
	spinlock_release(&router_inst->lock);

	minno = router_inst->stats.minno;
	min30 = 0.0;
	min15 = 0.0;
	min10 = 0.0;
	min5 = 0.0;
	for (j = 0; j < 30; j++)
	{
		minno--;
		if (minno < 0)
			minno += 30;
		min30 += router_inst->stats.minavgs[minno];
		if (j < 15)
			min15 += router_inst->stats.minavgs[minno];
		if (j < 10)
			min10 += router_inst->stats.minavgs[minno];
		if (j < 5)
			min5 += router_inst->stats.minavgs[minno];
	}
	min30 /= 30.0;
	min15 /= 15.0;
	min10 /= 10.0;
	min5 /= 5.0;

	dcb_printf(dcb, "\tMaster connection state:			%s\n",
			blrm_states[router_inst->state]);

	localtime_r(&router_inst->stats.lastReply, &tm);
	asctime_r(&tm, buf);

	dcb_printf(dcb, "\tBinlog directory:				%s\n",
		   router_inst->binlogdir);
	dcb_printf(dcb, "\tAVRO files directory:				%s\n",
		   router_inst->avrodir);
	dcb_printf(dcb, "\tCurrent binlog file:		  		%s\n",
                   router_inst->binlog_name);
	dcb_printf(dcb, "\tCurrent binlog position:	  		%lu\n",
                   router_inst->current_pos);
	dcb_printf(dcb, "\tNumber of slave servers:	   		%u\n",
                   router_inst->stats.n_clients);
	dcb_printf(dcb, "\tTotal no. of binlog events received:        	%u\n",
                   router_inst->stats.n_binlogs);
	minno = router_inst->stats.minno - 1;
	if (minno == -1)
		minno = 30;
	dcb_printf(dcb, "\tNumber of binlog events per minute\n");
	dcb_printf(dcb, "\tCurrent        5        10       15       30 Min Avg\n");
	dcb_printf(dcb, "\t %6d  %8.1f %8.1f %8.1f %8.1f\n",
		   router_inst->stats.minavgs[minno], min5, min10, min15, min30);
	dcb_printf(dcb, "\tNumber of binlog rotate events:  		%u\n",
                   router_inst->stats.n_rotates);
	dcb_printf(dcb, "\tNumber of packets received:			%u\n",
		   router_inst->stats.n_reads);
	dcb_printf(dcb, "\tAverage events per packet:			%.1f\n",
		   router_inst->stats.n_reads != 0 ? ((double)router_inst->stats.n_binlogs / router_inst->stats.n_reads) : 0);

	spinlock_acquire(&router_inst->lock);
	if (router_inst->stats.lastReply) {
		if (buf[strlen(buf)-1] == '\n') {
			buf[strlen(buf)-1] = '\0';
		}
		dcb_printf(dcb, "\tLast event from master at:  			%s (%d seconds ago)\n",
			buf, time(0) - router_inst->stats.lastReply);

		if (router_inst->lastEventTimestamp) {
			time_t	last_event = (time_t)router_inst->lastEventTimestamp;
			localtime_r(&last_event, &tm);
			asctime_r(&tm, buf);
			if (buf[strlen(buf)-1] == '\n') {
				buf[strlen(buf)-1] = '\0';
			}
			dcb_printf(dcb, "\tLast binlog event timestamp:  			%ld (%s)\n",
				router_inst->lastEventTimestamp, buf);
		}
	} else {
		dcb_printf(dcb, "\tNo events received from master yet\n");
	}
	spinlock_release(&router_inst->lock);

	dcb_printf(dcb, "\tEvents received:\n");
	for (i = 0; i <= MAX_EVENT_TYPE; i++)
	{
		dcb_printf(dcb, "\t\t%-38s   %u\n", event_names[i], router_inst->stats.events[i]);
	}

#if SPINLOCK_PROFILE
	dcb_printf(dcb, "\tSpinlock statistics (instlock):\n");
	spinlock_stats(&instlock, spin_reporter, dcb);
	dcb_printf(dcb, "\tSpinlock statistics (instance lock):\n");
	spinlock_stats(&router_inst->lock, spin_reporter, dcb);
	dcb_printf(dcb, "\tSpinlock statistics (binlog position lock):\n");
	spinlock_stats(&router_inst->binlog_lock, spin_reporter, dcb);
#endif

	if (router_inst->clients)
	{
		dcb_printf(dcb, "\tSlaves:\n");
		spinlock_acquire(&router_inst->lock);
		session = router_inst->clients;
		while (session)
		{

			minno = session->stats.minno;
			min30 = 0.0;
			min15 = 0.0;
			min10 = 0.0;
			min5 = 0.0;
			for (j = 0; j < 30; j++)
			{
				minno--;
				if (minno < 0)
					minno += 30;
				min30 += session->stats.minavgs[minno];
				if (j < 15)
					min15 += session->stats.minavgs[minno];
				if (j < 10)
					min10 += session->stats.minavgs[minno];
				if (j < 5)
					min5 += session->stats.minavgs[minno];
			}
			min30 /= 30.0;
			min15 /= 15.0;
			min10 /= 10.0;
			min5 /= 5.0;

				dcb_printf(dcb, "\t\tSlave UUID:					%s\n", session->uuid);
			dcb_printf(dcb,
				"\t\tSlave_host_port:				%s:%d\n",
						 session->dcb->remote, ntohs((session->dcb->ipv4).sin_port));
			dcb_printf(dcb,
				"\t\tUsername:					%s\n",
						 session->dcb->user);
			dcb_printf(dcb,
				"\t\tSlave DCB:					%p\n",
						 session->dcb);
			dcb_printf(dcb,
				"\t\tState:    					%s\n",
						 blrs_states[session->state]);
			dcb_printf(dcb,
				"\t\tBinlog file:					%s\n",
						session->avrofile);
			dcb_printf(dcb,
				"\t\tNo. requests:   				%u\n",
						session->stats.n_requests);
			dcb_printf(dcb,
					"\t\tNo. events sent:				%u\n",
						session->stats.n_events);
			dcb_printf(dcb,
					"\t\tNo. bytes sent:					%u\n",
						session->stats.n_bytes);

			minno = session->stats.minno - 1;
			if (minno == -1)
				minno = 30;
			dcb_printf(dcb, "\t\tNumber of binlog events per minute\n");
			dcb_printf(dcb, "\t\tCurrent        5        10       15       30 Min Avg\n");
			dcb_printf(dcb, "\t\t %6d  %8.1f %8.1f %8.1f %8.1f\n",
		   		session->stats.minavgs[minno], min5, min10,
						min15, min30);

			dcb_printf(dcb, "\t\tNo. of failed reads				%u\n", session->stats.n_failed_read);

#if DETAILED_DIAG
			dcb_printf(dcb, "\t\tNo. of nested distribute events			%u\n", session->stats.n_overrun);
			dcb_printf(dcb, "\t\tNo. of distribute action 1			%u\n", session->stats.n_actions[0]);
			dcb_printf(dcb, "\t\tNo. of distribute action 2			%u\n", session->stats.n_actions[1]);
			dcb_printf(dcb, "\t\tNo. of distribute action 3			%u\n", session->stats.n_actions[2]);
#endif
			if (session->lastEventTimestamp
					&& router_inst->lastEventTimestamp && session->lastEventReceived != HEARTBEAT_EVENT)
			{
				unsigned long seconds_behind;
				time_t	session_last_event = (time_t)session->lastEventTimestamp;

				if (router_inst->lastEventTimestamp > session->lastEventTimestamp)
					seconds_behind  = router_inst->lastEventTimestamp - session->lastEventTimestamp;
				else
					seconds_behind = 0;

				localtime_r(&session_last_event, &tm);
				asctime_r(&tm, buf);
				dcb_printf(dcb, "\t\tLast binlog event timestamp			%u, %s", session->lastEventTimestamp, buf);
				dcb_printf(dcb, "\t\tSeconds behind master				%lu\n", seconds_behind);
			}

			if (session->state == 0)
			{
				dcb_printf(dcb, "\t\tSlave_mode:					connected\n");
			}
			else
			{
				dcb_printf(dcb, "\t\tSlave_mode:					follow\n");
			}
#if SPINLOCK_PROFILE
			dcb_printf(dcb, "\tSpinlock statistics (catch_lock):\n");
			spinlock_stats(&session->catch_lock, spin_reporter, dcb);
			dcb_printf(dcb, "\tSpinlock statistics (rses_lock):\n");
			spinlock_stats(&session->rses_lock, spin_reporter, dcb);
#endif
			dcb_printf(dcb, "\t\t--------------------\n\n");
			session = session->next;
		}
		spinlock_release(&router_inst->lock);
	}
}

/**
 * Client Reply routine - in this case this is a message from the
 * master server, It should be sent to the state machine that manages
 * master packets as it may be binlog records or part of the registration
 * handshake that takes part during connection establishment.
 *
 *
 * @param       instance        The router instance
 * @param       router_session  The router session
 * @param       master_dcb      The DCB for the connection to the master
 * @param       queue           The GWBUF with reply data
 */
static  void
clientReply(ROUTER *instance, void *router_session, GWBUF *queue, DCB *backend_dcb)
{
AVRO_INSTANCE	*router = (AVRO_INSTANCE *)instance;
    ;
}

static char *
extract_message(GWBUF *errpkt)
{
char	*rval;
int	len;

	len = EXTRACT24(errpkt->start);
	if ((rval = (char *)malloc(len)) == NULL)
		return NULL;
	memcpy(rval, (char *)(errpkt->start) + 7, 6);
	rval[6] = ' ';
	/* message size is len - (1 byte field count + 2 bytes errno + 6 bytes status) */
	memcpy(&rval[7], (char *)(errpkt->start) + 13, len - 9);
	rval[len-2] = 0;
	return rval;
}

/**
 * Error Reply routine
 *
 * The routine will reply to client errors and/or closing the session
 * or try to open a new backend connection.
 *
 * @param       instance        The router instance
 * @param       router_session  The router session
 * @param       message         The error message to reply
 * @param       backend_dcb     The backend DCB
 * @param       action     	The action: ERRACT_NEW_CONNECTION or ERRACT_REPLY_CLIENT
 * @param	succp		Result of action: true iff router can continue
 *
 */
static  void
errorReply(ROUTER *instance, void *router_session, GWBUF *message, DCB *backend_dcb, error_action_t action, bool *succp)
{
AVRO_INSTANCE	*router = (AVRO_INSTANCE *)instance;
int		error;
char		msg[STRERROR_BUFLEN + 1 + 5] = "";
char 		*errmsg;
unsigned long	mysql_errno;

	/** Don't handle same error twice on same DCB */
        if (backend_dcb->dcb_errhandle_called)
	{
		/** we optimistically assume that previous call succeed */
		*succp = true;
		return;
	}
	else
	{
		backend_dcb->dcb_errhandle_called = true;
	}

}

/** to be inline'd */
/**
 * @node Acquires lock to router client session if it is not closed.
 *
 * Parameters:
 * @param rses - in, use
 *
 *
 * @return true if router session was not closed. If return value is true
 * it means that router is locked, and must be unlocked later. False, if
 * router was closed before lock was acquired.
 *
 *
 * @details (write detailed description here)
 *
 */
static bool rses_begin_locked_router_action(AVRO_CLIENT *rses)
{
        bool succp = false;

        CHK_CLIENT_RSES(rses);

        spinlock_acquire(&rses->rses_lock);
        succp = true;

        return succp;
}

/** to be inline'd */
/**
 * @node Releases router client session lock.
 *
 * Parameters:
 * @param rses - <usage>
 *          <description>
 *
 * @return void
 *
 *
 * @details (write detailed description here)
 *
 */
static void rses_end_locked_router_action(AVRO_CLIENT *rses)
{
        CHK_CLIENT_RSES(rses);
        spinlock_release(&rses->rses_lock);
}


static int getCapabilities()
{
        return RCAP_TYPE_NO_RSESSION;
}

/**
 * The stats gathering function called from the housekeeper so that we
 * can get timed averages of binlog records shippped
 *
 * @param inst	The router instance
 */
static void
stats_func(void *inst)
{
AVRO_INSTANCE *router = (AVRO_INSTANCE *)inst;
AVRO_CLIENT    *client;

	router->stats.minavgs[router->stats.minno++]
			 = router->stats.n_binlogs - router->stats.lastsample;
	router->stats.lastsample = router->stats.n_binlogs;
	if (router->stats.minno == AVRO_NSTATS_MINUTES)
		router->stats.minno = 0;

	spinlock_acquire(&router->lock);
	client = router->clients;
	while (client)
	{
		client->stats.minavgs[client->stats.minno++]
			 = client->stats.n_events - client->stats.lastsample;
		client->stats.lastsample = client->stats.n_events;
		if (client->stats.minno == AVRO_NSTATS_MINUTES)
			client->stats.minno = 0;
		client = client->next;
	}
	spinlock_release(&router->lock);
}

/**
 * Add the service user to CDC dbusers (service->users)
 * via cdc_users_alloc
 *
 * @param service	The current service
 * @return		0 on success, 1 on failure
 */
static int
avro_set_service_CDC_user(SERVICE *service) {
char	*dpwd = NULL;
char	*newpasswd = NULL;
char	*service_user = NULL;
char	*service_passwd = NULL;

	if (serviceGetUser(service, &service_user, &service_passwd) == 0) {
		MXS_ERROR("failed to get service user details for service %s",
                          service->name);

		return 1;
	}

	dpwd = decryptPassword(service->credentials.authdata);

	if (!dpwd) {
		MXS_ERROR("decrypt password failed for service user %s, service %s",
                          service_user,
                          service->name);

		return 1;
        }

	newpasswd = create_hex_sha1_sha1_passwd(dpwd);

	if (!newpasswd) {
		MXS_ERROR("create hex_sha1_sha1_password failed for service user %s",
                          service_user);

		free(dpwd);
		return 1;
	}

	/* add service user for % and localhost */
/*
	(void)add_mysql_users_with_host_ipv4(service->users, service->credentials.name, "%", newpasswd, "Y", "");
	(void)add_mysql_users_with_host_ipv4(service->users, service->credentials.name, "localhost", newpasswd, "Y", "");
*/

	free(newpasswd);
	free(dpwd);

	return 0;
}

/**
 * Load mysql dbusers into (service->users)
 *
 * @param router	The router instance
 * @return              -1 on failure, 0 for no users found, > 0 for found users
 */
int
blr_load_dbusers(AVRO_INSTANCE *router)
{
int loaded = -1;
char	path[PATH_MAX+1] = "";
SERVICE *service;
	service = router->service;

	/* File path for router cached authentication data */
	strncpy(path, router->binlogdir, PATH_MAX);
	strncat(path, "/cache", PATH_MAX);
	strncat(path, "/dbusers", PATH_MAX);

	/* Try loading dbusers from configured backends */
	loaded = load_mysql_users(service);

	if (loaded < 0)
	{
		MXS_ERROR("Unable to load users for service %s",
                          service->name);

		/* Try loading authentication data from file cache */

		loaded = dbusers_load(router->service->users, path);

		if (loaded != -1)
		{
			MXS_ERROR("Service %s, Using cached credential information file %s.",
                                  service->name,
                                  path);
		} else {
			MXS_ERROR("Service %s, Unable to read cache credential information from %s."
                                  " No database user added to service users table.",
                                  service->name,
                                  path);
		}
	} else {
		/* don't update cache if no user was loaded */
		if (loaded == 0)
		{
			MXS_ERROR("Service %s: failed to load any user "
                                  "information. Authentication will "
                                  "probably fail as a result.",
                                  service->name);
		} else {
			/* update cached data */
			blr_save_dbusers(router);
		}
	}

	/* At service start last update is set to USERS_REFRESH_TIME seconds earlier.
	 * This way MaxScale could try reloading users' just after startup
	 */
	service->rate_limit.last=time(NULL) - USERS_REFRESH_TIME;
	service->rate_limit.nloads=1;

	return loaded;
}

/**
 * Save dbusers to cache file
 *
 * @param router	The router instance
 * @return              -1 on failure, >= 0 on success
 */
int
blr_save_dbusers(AVRO_INSTANCE *router)
{
SERVICE *service;
char	path[PATH_MAX+1] = "";
int	mkdir_rval = 0;

        service = router->service;

        /* File path for router cached authentication data */
        strncpy(path, router->binlogdir, PATH_MAX);
        strncat(path, "/cache", PATH_MAX);

	/* check and create dir */
	if (access(path, R_OK) == -1)
	{
		mkdir_rval = mkdir(path, 0700);
	}

	if (mkdir_rval == -1)
	{
		char err_msg[STRERROR_BUFLEN];
		MXS_ERROR("Service %s, Failed to create directory '%s': [%d] %s",
                          service->name,
                          path,
                          errno,
                          strerror_r(errno, err_msg, sizeof(err_msg)));

		return -1;
	}

	/* set cache file name */
	strncat(path, "/dbusers", PATH_MAX);

	return dbusers_save(service->users, path);
}

/**
 * Extract a numeric field from a packet of the specified number of bits
 *
 * @param src	The raw packet source
 * @param birs	The number of bits to extract (multiple of 8)
 */
uint32_t
extract_field(uint8_t *src, int bits)
{
uint32_t	rval = 0, shift = 0;

	while (bits > 0)
	{
		rval |= (*src++) << shift;
		shift += 8;
		bits -= 8;
	}
	return rval;
}

/**
 * Conversion task: MySQL binlogs to AVRO files
 */
void converter_func(void* data)
{
    AVRO_INSTANCE* router = (AVRO_INSTANCE*)data;

    while (blr_next_binlog_exists(router->binlogdir, router->binlog_name))
    {
        if(avro_open_binlog(router->binlogdir, router->binlog_name, &router->binlog_fd))
        {
            lseek(router->binlog_fd, 0L, SEEK_SET);
            //blr_read_events_all_events(router, 0, 0);
            avro_close_binlog(router->binlog_fd);
            sprintf(router->binlog_name, BINLOG_NAMEFMT, router->fileroot,
                    blr_file_get_next_binlogname(router->binlog_name));
        }
    }
}
