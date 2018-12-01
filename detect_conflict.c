/*-------------------------------------------------------------------------
 *
 * detect_conflict.c
 *		detect logoical replication conflict
 *		
 *
 * Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		detect_conflict/detect_conflict.c
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include <sys/time.h>

#include "postgres.h"
#include "libpq/libpq.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "access/transam.h"
#include "lib/stringinfo.h"
#include "postmaster/syslogger.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/json.h"

/* Allow load of this module in shared libs */
PG_MODULE_MAGIC;

char* constraint_name;
char* action_script;

void _PG_init(void);
void _PG_fini(void);

/* Hold previous logging hook */
static emit_log_hook_type prev_log_hook = NULL;

/*
 * Track if redirection to syslogger can happen. This uses the same method
 * as postmaster.c and syslogger.c, this flag being updated by the postmaster
 * once server parameters are loaded.
 */
extern bool redirection_done;


static void detect_conflict(ErrorData *edata);

/*
 * detect_conflict
 * detect conflict and execute script
 */
static void
detect_conflict(ErrorData *edata)
{

fprintf(stderr, "detect_conflict start, sqlstate=%s\n", unpack_sql_state(edata->sqlerrcode));
fprintf(stderr, "detect_conflict, constraint_name=%s\n", edata->constraint_name);

    if ( !strcmp(unpack_sql_state(edata->sqlerrcode) , "23505") ) {
      /* duplicate key error. */
      fprintf(stderr, "conflict error. constraint_name=%s\n", edata->constraint_name);

      if ( !strcmp( constraint_name, edata->constraint_name)) {
        if (action_script != NULL) {
          fprintf(stderr, "launch action_script, script = %s \n", action_script);
          system( action_script );
        }
      } else {
        fprintf(stderr, "conflict error. constraint_name nomatch.\n");
      }
    }

	/* Continue chain to previous hook */
	if (prev_log_hook)
		(*prev_log_hook) (edata);
}

/*
 * _PG_init
 * Entry point loading hooks
 */
void
_PG_init(void)
{
    /* Must be loaded with shared_preload_libaries */
    if (IsUnderPostmaster)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("detect_confilict must be loaded via shared_preload_libraries")));

    ereport(LOG, (errmsg(
          "load start detect_confilict"
           )));

    DefineCustomStringVariable(
        "detect_conflict.constraint_name",
        "constraint_name.",
        "constraint_name.",
        &constraint_name,
        "yamanote_t_name_key",
        PGC_SUSET,
        GUC_NOT_IN_SAMPLE,
        NULL, NULL, NULL);

    DefineCustomStringVariable(
        "detect_conflict.action_script",
        "action_script.",
        "action_script.",
        &action_script,
        NULL,
        PGC_SUSET,
        GUC_NOT_IN_SAMPLE,
        NULL, NULL, NULL);

	prev_log_hook = emit_log_hook;
	emit_log_hook = detect_conflict;
    ereport(LOG, (errmsg(
          "detect_conflict:loaded, detect_confilict.constraint_name=%s, action_script=%s",
           constraint_name,
           action_script
           )));
}

/*
 * _PG_fini
 * Exit point unloading hooks
 */
void
_PG_fini(void)
{
	emit_log_hook = prev_log_hook;
}
