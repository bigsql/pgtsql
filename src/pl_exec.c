/*---------------------------------------------------------------------------------
 *
 * pl_exec.c	Executor for the PL/TSQL procedural language
 *
 * Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl_exec.c
 *
 *---------------------------------------------------------------------------------
 */

#include "pltsql.h"

#include <ctype.h>

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/tupconvert.h"
#include "access/tuptoaster.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/spi_priv.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/scansup.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"


static const char *const raise_skip_msg = "RAISE";

typedef struct
{
	int			nargs;			/* number of arguments */
	Oid		   *types;			/* types of arguments */
	Datum	   *values;			/* evaluated argument values */
	char	   *nulls;			/* null markers (' '/'n' style) */
	bool	   *freevals;		/* which arguments are pfree-able */
} PreparedParamsData;

/*
 * All pltsql function executions within a single transaction share the same
 * executor EState for evaluating "simple" expressions.  Each function call
 * creates its own "eval_econtext" ExprContext within this estate for
 * per-evaluation workspace.  eval_econtext is freed at normal function exit,
 * and the EState is freed at transaction end (in case of error, we assume
 * that the abort mechanisms clean it all up).	Furthermore, any exception
 * block within a function has to have its own eval_econtext separate from
 * the containing function's, so that we can clean up ExprContext callbacks
 * properly at subtransaction exit.  We maintain a stack that tracks the
 * individual econtexts so that we can clean up correctly at subxact exit.
 *
 * This arrangement is a bit tedious to maintain, but it's worth the trouble
 * so that we don't have to re-prepare simple expressions on each trip through
 * a function.	(We assume the case to optimize is many repetitions of a
 * function within a transaction.)
 */
typedef struct SimpleEcontextStackEntry
{
	ExprContext *stack_econtext;	/* a stacked econtext */
	SubTransactionId xact_subxid;		/* ID for current subxact */
	struct SimpleEcontextStackEntry *next;		/* next stack entry up */
} SimpleEcontextStackEntry;

static EState *shared_simple_eval_estate = NULL;
static SimpleEcontextStackEntry *simple_econtext_stack = NULL;

/*
 * Memory management within a plpgsql function generally works with three
 * contexts:
 *
 * 1. Function-call-lifespan data, such as variable values, is kept in the
 * "main" context, a/k/a the "SPI Proc" context established by SPI_connect().
 * This is usually the CurrentMemoryContext while running code in this module
 * (which is not good, because careless coding can easily cause
 * function-lifespan memory leaks, but we live with it for now).
 *
 * 2. Some statement-execution routines need statement-lifespan workspace.
 * A suitable context is created on-demand by get_stmt_mcontext(), and must
 * be reset at the end of the requesting routine.  Error recovery will clean
 * it up automatically.  Nested statements requiring statement-lifespan
 * workspace will result in a stack of such contexts, see push_stmt_mcontext().
 *
 * 3. We use the eval_econtext's per-tuple memory context for expression
 * evaluation, and as a general-purpose workspace for short-lived allocations.
 * Such allocations usually aren't explicitly freed, but are left to be
 * cleaned up by a context reset, typically done by exec_eval_cleanup().
 *
 * These macros are for use in making short-lived allocations:
 */
#define get_eval_mcontext(estate) \
	((estate)->eval_econtext->ecxt_per_tuple_memory)
#define eval_mcontext_alloc(estate, sz) \
	MemoryContextAlloc(get_eval_mcontext(estate), sz)
#define eval_mcontext_alloc0(estate, sz) \
	MemoryContextAllocZero(get_eval_mcontext(estate), sz)

/*
 * We use a session-wide hash table for caching cast information.
 *
 * Once built, the compiled expression trees (cast_expr fields) survive for
 * the life of the session.  At some point it might be worth invalidating
 * those after pg_cast changes, but for the moment we don't bother.
 *
 * The evaluation state trees (cast_exprstate) are managed in the same way as
 * simple expressions (i.e., we assume cast expressions are always simple).
 *
 * As with simple expressions, DO blocks don't use the shared hash table but
 * must have their own.  This isn't ideal, but we don't want to deal with
 * multiple simple_eval_estates within a DO block.
 */
typedef struct					/* lookup key for cast info */
{
	/* NB: we assume this struct contains no padding bytes */
	Oid			srctype;		/* source type for cast */
	Oid			dsttype;		/* destination type for cast */
	int32		srctypmod;		/* source typmod for cast */
	int32		dsttypmod;		/* destination typmod for cast */
} pltsql_CastHashKey;

typedef struct					/* cast_hash table entry */
{
	pltsql_CastHashKey key;	/* hash key --- MUST BE FIRST */
	Expr	   *cast_expr;		/* cast expression, or NULL if no-op cast */
	/* ExprState is valid only when cast_lxid matches current LXID */
	ExprState  *cast_exprstate; /* expression's eval tree */
	bool		cast_in_use;	/* true while we're executing eval tree */
	LocalTransactionId cast_lxid;
} pltsql_CastHashEntry;

static MemoryContext shared_cast_context = NULL;
static HTAB *shared_cast_hash = NULL;

/************************************************************
 * Local function forward declarations
 ************************************************************/
static void pltsql_exec_error_callback(void *arg);
static PLTSQL_datum *copy_pltsql_datum(PLTSQL_datum *datum);
static MemoryContext get_stmt_mcontext(PLTSQL_execstate *estate);

static int exec_stmt_block(PLTSQL_execstate *estate,
				PLTSQL_stmt_block *block);
static int exec_stmts(PLTSQL_execstate *estate,
		   List *stmts);
static int exec_stmt(PLTSQL_execstate *estate,
		  PLTSQL_stmt *stmt);
static int exec_stmt_assign(PLTSQL_execstate *estate,
				 PLTSQL_stmt_assign *stmt);
static int exec_stmt_perform(PLTSQL_execstate *estate,
				  PLTSQL_stmt_perform *stmt);
static int exec_stmt_getdiag(PLTSQL_execstate *estate,
				  PLTSQL_stmt_getdiag *stmt);
static int exec_stmt_if(PLTSQL_execstate *estate,
			 PLTSQL_stmt_if *stmt);
static int exec_stmt_case(PLTSQL_execstate *estate,
			   PLTSQL_stmt_case *stmt);
static int exec_stmt_loop(PLTSQL_execstate *estate,
			   PLTSQL_stmt_loop *stmt);
static int exec_stmt_while(PLTSQL_execstate *estate,
				PLTSQL_stmt_while *stmt);
static int exec_stmt_fori(PLTSQL_execstate *estate,
			   PLTSQL_stmt_fori *stmt);
static int exec_stmt_fors(PLTSQL_execstate *estate,
			   PLTSQL_stmt_fors *stmt);
static int exec_stmt_forc(PLTSQL_execstate *estate,
			   PLTSQL_stmt_forc *stmt);
static int exec_stmt_foreach_a(PLTSQL_execstate *estate,
					PLTSQL_stmt_foreach_a *stmt);
static int exec_stmt_open(PLTSQL_execstate *estate,
			   PLTSQL_stmt_open *stmt);
static int exec_stmt_fetch(PLTSQL_execstate *estate,
				PLTSQL_stmt_fetch *stmt);
static int exec_stmt_close(PLTSQL_execstate *estate,
				PLTSQL_stmt_close *stmt);
static int exec_stmt_exit(PLTSQL_execstate *estate,
			   PLTSQL_stmt_exit *stmt);
static int exec_stmt_return(PLTSQL_execstate *estate,
				 PLTSQL_stmt_return *stmt);
static int exec_stmt_return_next(PLTSQL_execstate *estate,
					  PLTSQL_stmt_return_next *stmt);
static int exec_stmt_return_query(PLTSQL_execstate *estate,
					   PLTSQL_stmt_return_query *stmt);
static int exec_stmt_raise(PLTSQL_execstate *estate,
				PLTSQL_stmt_raise *stmt);
static int exec_stmt_execsql(PLTSQL_execstate *estate,
				  PLTSQL_stmt_execsql *stmt);
static int exec_stmt_dynexecute(PLTSQL_execstate *estate,
					 PLTSQL_stmt_dynexecute *stmt);
static int exec_stmt_dynfors(PLTSQL_execstate *estate,
				  PLTSQL_stmt_dynfors *stmt);

static void pltsql_estate_setup(PLTSQL_execstate *estate,
					 PLTSQL_function *func,
					 ReturnSetInfo *rsi,
					 EState *simple_eval_estate);
static void exec_eval_cleanup(PLTSQL_execstate *estate);
static void exec_prepare_plan(PLTSQL_execstate *estate,
				  PLTSQL_expr *expr, int cursorOptions,
				  bool keepplan);
static void exec_simple_check_plan(PLTSQL_execstate *estate, PLTSQL_expr *expr);
static void exec_save_simple_expr(PLTSQL_expr *expr, CachedPlan *cplan);
static void exec_check_rw_parameter(PLTSQL_expr *expr, int target_dno);
static bool contains_target_param(Node *node, int *target_dno);
static bool exec_eval_simple_expr(PLTSQL_execstate *estate,
					  PLTSQL_expr *expr,
					  Datum *result,
					  bool *isNull,
					  Oid *rettype,
					  int32 *rettypmod);

static void exec_assign_expr(PLTSQL_execstate *estate,
				 PLTSQL_datum *target,
				 PLTSQL_expr *expr);
static void exec_assign_c_string(PLTSQL_execstate *estate,
					 PLTSQL_datum *target,
					 const char *str);
static void exec_assign_value(PLTSQL_execstate *estate,
				  PLTSQL_datum *target,
				  Datum value, bool isNull,
				  Oid valtype, int32 valtypmod);
static void exec_eval_datum(PLTSQL_execstate *estate,
				PLTSQL_datum *datum,
				Oid *typeid,
				int32 *typetypmod,
				Datum *value,
				bool *isnull);
static int exec_eval_integer(PLTSQL_execstate *estate,
				  PLTSQL_expr *expr,
				  bool *isNull);
static bool exec_eval_boolean(PLTSQL_execstate *estate,
				  PLTSQL_expr *expr,
				  bool *isNull);
static Datum exec_eval_expr(PLTSQL_execstate *estate,
							PLTSQL_expr *expr,
							bool *isNull,
							Oid *rettype,
							int32 *rettypmod);
static int exec_run_select(PLTSQL_execstate *estate,
				PLTSQL_expr *expr, long maxtuples, Portal *portalP);
static int exec_for_query(PLTSQL_execstate *estate, PLTSQL_stmt_forq *stmt,
			   Portal portal, bool prefetch_ok);
static ParamListInfo setup_param_list(PLTSQL_execstate *estate,
				 PLTSQL_expr *expr);
static void exec_move_row(PLTSQL_execstate *estate,
			  PLTSQL_variable *target,
			  HeapTuple tup, TupleDesc tupdesc);
static ExpandedRecordHeader *make_expanded_record_for_rec(PLTSQL_execstate *estate,
							 PLTSQL_rec *rec,
							 TupleDesc srctupdesc,
							 ExpandedRecordHeader *srcerh);
static void exec_move_row_from_fields(PLTSQL_execstate *estate,
						  PLTSQL_variable *target,
						  ExpandedRecordHeader *newerh,
						  Datum *values, bool *nulls,
						  TupleDesc tupdesc);
static bool compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc);
static HeapTuple make_tuple_from_row(PLTSQL_execstate *estate,
					PLTSQL_row *row,
					TupleDesc tupdesc);
static TupleDesc deconstruct_composite_datum(Datum value,
							HeapTupleData *tmptup);
static void exec_move_row_from_datum(PLTSQL_execstate *estate,
						 PLTSQL_variable *target,
						 Datum value);
static void instantiate_empty_record_variable(PLTSQL_execstate *estate,
								  PLTSQL_rec *rec);
static char *convert_value_to_string(PLTSQL_execstate *estate,
						Datum value, Oid valtype);
static Datum exec_cast_value(PLTSQL_execstate *estate,
				Datum value, bool *isnull,
				Oid valtype, int32 valtypmod,
				Oid reqtype, int32 reqtypmod);
static pltsql_CastHashEntry *get_cast_hashentry(PLTSQL_execstate *estate,
				   Oid srctype, int32 srctypmod,
				   Oid dsttype, int32 dsttypmod);
static Datum exec_cast_value(PLTSQL_execstate *estate,
				Datum value, bool *isnull,
				Oid valtype, int32 valtypmod,
				Oid reqtype, int32 reqtypmod);
static void exec_init_tuple_store(PLTSQL_execstate *estate);
static void exec_set_found(PLTSQL_execstate *estate, bool state);
static void pltsql_create_econtext(PLTSQL_execstate *estate);
static void pltsql_destroy_econtext(PLTSQL_execstate *estate);
static void free_var(PLTSQL_var *var);
static void assign_simple_var(PLTSQL_execstate *estate, PLTSQL_var *var,
				  Datum newvalue, bool isnull, bool freeable);
static void assign_text_var(PLTSQL_var *var, const char *str);
static void assign_record_var(PLTSQL_execstate *estate, PLTSQL_rec *rec,
				  ExpandedRecordHeader *erh);
static PreparedParamsData *exec_eval_using_params(PLTSQL_execstate *estate,
					   List *params);
static void free_params_data(PreparedParamsData *ppd);
static Portal exec_dynquery_with_params(PLTSQL_execstate *estate,
						  PLTSQL_expr *dynquery, List *params,
						  const char *portalname, int cursorOptions);
static char * transform_tsql_temp_tables(char * dynstmt);
static bool is_char_identstart(char c);
static bool is_char_identpart(char c);
static char * next_word(char *dyntext);
static bool is_next_temptbl(char *dyntext);

/* ----------
 * pltsql_exec_function	Called by the call handler for
 *				function execution.
 * ----------
 */
Datum
pltsql_exec_function(PLTSQL_function *func, FunctionCallInfo fcinfo,
					 EState *simple_eval_estate)
{
	PLTSQL_execstate estate;
	ErrorContextCallback plerrcontext;
	int			i;
	int			rc;

	/*
	 * Setup the execution state
	 */
	pltsql_estate_setup(&estate, func, (ReturnSetInfo *) fcinfo->resultinfo,
						simple_eval_estate);

	/*
	 * Setup error traceback support for ereport()
	 */
	plerrcontext.callback = pltsql_exec_error_callback;
	plerrcontext.arg = &estate;
	plerrcontext.previous = error_context_stack;
	error_context_stack = &plerrcontext;

	/*
	 * Make local execution copies of all the datums
	 */
	estate.err_text = gettext_noop("during initialization of execution state");
	for (i = 0; i < estate.ndatums; i++)
		estate.datums[i] = copy_pltsql_datum(func->datums[i]);

	/*
	 * Store the actual call argument values into the appropriate variables
	 */
	estate.err_text = gettext_noop("while storing call arguments into local variables");
	for (i = 0; i < func->fn_nargs; i++)
	{
		int			n = func->fn_argvarnos[i];

		switch (estate.datums[n]->dtype)
		{
			case PLTSQL_DTYPE_VAR:
				{
					PLTSQL_var *var = (PLTSQL_var *) estate.datums[n];

					var->value = fcinfo->arg[i];
					var->isnull = fcinfo->argnull[i];
					var->freeval = false;
				}
				break;

			case PLTSQL_DTYPE_ROW:
				{
					PLTSQL_row *row = (PLTSQL_row *) estate.datums[n];

					if (!fcinfo->argnull[i])
					{
						HeapTupleHeader td;
						Oid			tupType;
						int32		tupTypmod;
						TupleDesc	tupdesc;
						HeapTupleData tmptup;

						td = DatumGetHeapTupleHeader(fcinfo->arg[i]);
						/* Extract rowtype info and find a tupdesc */
						tupType = HeapTupleHeaderGetTypeId(td);
						tupTypmod = HeapTupleHeaderGetTypMod(td);
						tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
						/* Build a temporary HeapTuple control structure */
						tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
						ItemPointerSetInvalid(&(tmptup.t_self));
						tmptup.t_tableOid = InvalidOid;
						tmptup.t_data = td;
						// exec_move_row(&estate, NULL, row, &tmptup, tupdesc);
						exec_move_row(&estate, (PLTSQL_variable *) row, &tmptup, tupdesc);
						ReleaseTupleDesc(tupdesc);
					}
					else
					{
						/* If arg is null, treat it as an empty row */
						exec_move_row(&estate, (PLTSQL_variable *) row, NULL, NULL);
					}
					/* clean up after exec_move_row() */
					exec_eval_cleanup(&estate);
				}
				break;

			default:
				elog(ERROR, "unrecognized dtype: %d", func->datums[i]->dtype);
		}
	}

	estate.err_text = gettext_noop("during function entry");

	/*
	 * Set the magic variable FOUND to false
	 */
	exec_set_found(&estate, false);

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plugin_ptr && (*plugin_ptr)->func_beg)
		((*plugin_ptr)->func_beg) (&estate, func);

	/*
	 * Now call the toplevel block of statements
	 */
	estate.err_text = NULL;
	estate.err_stmt = (PLTSQL_stmt *) (func->action);
	rc = exec_stmt_block(&estate, func->action);
	if (rc != PLTSQL_RC_RETURN)
	{
		estate.err_stmt = NULL;
		estate.err_text = NULL;

		/*
		 * Provide a more helpful message if a CONTINUE or RAISE has been used
		 * outside the context it can work in.
		 */
		if (rc == PLTSQL_RC_CONTINUE)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("CONTINUE cannot be used outside a loop")));
		else
			ereport(ERROR,
			   (errcode(ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
				errmsg("control reached end of function without RETURN")));
	}

	/*
	 * We got a return value - process it
	 */
	estate.err_stmt = NULL;
	estate.err_text = gettext_noop("while casting return value to function's return type");

	fcinfo->isnull = estate.retisnull;

	if (estate.retisset)
	{
		ReturnSetInfo *rsi = estate.rsi;

		/* Check caller can handle a set result */
		if (!rsi || !IsA(rsi, ReturnSetInfo) ||
			(rsi->allowedModes & SFRM_Materialize) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("set-valued function called in context that cannot accept a set")));
		rsi->returnMode = SFRM_Materialize;

		/* If we produced any tuples, send back the result */
		if (estate.tuple_store)
		{
			rsi->setResult = estate.tuple_store;
			if (estate.rettupdesc)
			{
				MemoryContext oldcxt;

				oldcxt = MemoryContextSwitchTo(estate.tuple_store_cxt);
				rsi->setDesc = CreateTupleDescCopy(estate.rettupdesc);
				MemoryContextSwitchTo(oldcxt);
			}
		}
		estate.retval = (Datum) 0;
		fcinfo->isnull = true;
	}
	else if (!estate.retisnull)
	{
		if (estate.retistuple)
		{
			/*
			 * We have to check that the returned tuple actually matches the
			 * expected result type.  XXX would be better to cache the tupdesc
			 * instead of repeating get_call_result_type()
			 */
			HeapTuple	rettup = (HeapTuple) DatumGetPointer(estate.retval);
			TupleDesc	tupdesc;
			TupleConversionMap *tupmap;

			switch (get_call_result_type(fcinfo, NULL, &tupdesc))
			{
				case TYPEFUNC_COMPOSITE:
					/* got the expected result rowtype, now check it */
					tupmap = convert_tuples_by_position(estate.rettupdesc,
														tupdesc,
														gettext_noop("returned record type does not match expected record type"));
					/* it might need conversion */
					if (tupmap)
						rettup = do_convert_tuple(rettup, tupmap);
					/* no need to free map, we're about to return anyway */
					break;
				case TYPEFUNC_RECORD:

					/*
					 * Failed to determine actual type of RECORD.  We could
					 * raise an error here, but what this means in practice is
					 * that the caller is expecting any old generic rowtype,
					 * so we don't really need to be restrictive. Pass back
					 * the generated result type, instead.
					 */
					tupdesc = estate.rettupdesc;
					if (tupdesc == NULL)		/* shouldn't happen */
						elog(ERROR, "return type must be a row type");
					break;
				default:
					/* shouldn't get here if retistuple is true ... */
					elog(ERROR, "return type must be a row type");
					break;
			}

			/*
			 * Copy tuple to upper executor memory, as a tuple Datum. Make
			 * sure it is labeled with the caller-supplied tuple type.
			 */
			estate.retval = PointerGetDatum(SPI_returntuple(rettup, tupdesc));
		}
		else
		{
			/* Scalar case: use exec_cast_value */
			estate.retval = exec_cast_value(&estate,
											estate.retval,
											&fcinfo->isnull,
											estate.rettype,
											-1,
											func->fn_rettype,
											-1);
			/*
			 * If the function's return type isn't by value, copy the value
			 * into upper executor memory context.
			 */
			if (!fcinfo->isnull && !func->fn_retbyval)
			{
				Size		len;
				void	   *tmp;

				len = datumGetSize(estate.retval, false, func->fn_rettyplen);
				tmp = SPI_palloc(len);
				memcpy(tmp, DatumGetPointer(estate.retval), len);
				estate.retval = PointerGetDatum(tmp);
			}
		}
	}

	estate.err_text = gettext_noop("during function exit");

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plugin_ptr && (*plugin_ptr)->func_end)
		((*plugin_ptr)->func_end) (&estate, func);

	/* Clean up any leftover temporary memory */
	pltsql_destroy_econtext(&estate);
	exec_eval_cleanup(&estate);

	/*
	 * Pop the error context stack
	 */
	error_context_stack = plerrcontext.previous;

	/*
	 * Return the function's result
	 */
	return estate.retval;
}


/* ----------
 * pltsql_exec_trigger		Called by the call handler for
 *				trigger execution.
 * ----------
 */
HeapTuple
pltsql_exec_trigger(PLTSQL_function *func,
					 TriggerData *trigdata)
{
	PLTSQL_execstate estate;
	ErrorContextCallback plerrcontext;
	int			i;
	int			rc;
	PLTSQL_var *var;
	PLTSQL_rec *rec_new,
			   *rec_old;
	HeapTuple	rettup;

	/*
	 * Setup the execution state
	 */
	pltsql_estate_setup(&estate, func, NULL, NULL);

	/*
	 * Setup error traceback support for ereport()
	 */
	plerrcontext.callback = pltsql_exec_error_callback;
	plerrcontext.arg = &estate;
	plerrcontext.previous = error_context_stack;
	error_context_stack = &plerrcontext;

	/*
	 * Make local execution copies of all the datums
	 */
	estate.err_text = gettext_noop("during initialization of execution state");
	for (i = 0; i < estate.ndatums; i++)
		estate.datums[i] = copy_pltsql_datum(func->datums[i]);

	/*
	 * Put the OLD and NEW tuples into record variables
	 *
	 * We make the tupdescs available in both records even though only one may
	 * have a value.  This allows parsing of record references to succeed in
	 * functions that are used for multiple trigger types.	For example, we
	 * might have a test like "if (TG_OP = 'INSERT' and NEW.foo = 'xyz')",
	 * which should parse regardless of the current trigger type.
	 */
	rec_new = (PLTSQL_rec *) (estate.datums[func->new_varno]);
	rec_new->freetup = false;
	rec_new->tupdesc = trigdata->tg_relation->rd_att;
	rec_new->freetupdesc = false;
	rec_old = (PLTSQL_rec *) (estate.datums[func->old_varno]);
	rec_old->freetup = false;
	rec_old->tupdesc = trigdata->tg_relation->rd_att;
	rec_old->freetupdesc = false;

	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
	{
		/*
		 * Per-statement triggers don't use OLD/NEW variables
		 */
		rec_new->tup = NULL;
		rec_old->tup = NULL;
	}
	else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		rec_new->tup = trigdata->tg_trigtuple;
		rec_old->tup = NULL;
	}
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		rec_new->tup = trigdata->tg_newtuple;
		rec_old->tup = trigdata->tg_trigtuple;
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		rec_new->tup = NULL;
		rec_old->tup = trigdata->tg_trigtuple;
	}
	else
		elog(ERROR, "unrecognized trigger action: not INSERT, DELETE, or UPDATE");

	/*
	 * Assign the special tg_ variables
	 */

	var = (PLTSQL_var *) (estate.datums[func->tg_op_varno]);
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		var->value = CStringGetTextDatum("INSERT");
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		var->value = CStringGetTextDatum("UPDATE");
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		var->value = CStringGetTextDatum("DELETE");
	else if (TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
		var->value = CStringGetTextDatum("TRUNCATE");
	else
		elog(ERROR, "unrecognized trigger action: not INSERT, DELETE, UPDATE, or TRUNCATE");
	var->isnull = false;
	var->freeval = true;

	var = (PLTSQL_var *) (estate.datums[func->tg_name_varno]);
	var->value = DirectFunctionCall1(namein,
							  CStringGetDatum(trigdata->tg_trigger->tgname));
	var->isnull = false;
	var->freeval = true;

	var = (PLTSQL_var *) (estate.datums[func->tg_when_varno]);
	if (TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		var->value = CStringGetTextDatum("BEFORE");
	else if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
		var->value = CStringGetTextDatum("AFTER");
	else if (TRIGGER_FIRED_INSTEAD(trigdata->tg_event))
		var->value = CStringGetTextDatum("INSTEAD OF");
	else
		elog(ERROR, "unrecognized trigger execution time: not BEFORE, AFTER, or INSTEAD OF");
	var->isnull = false;
	var->freeval = true;

	var = (PLTSQL_var *) (estate.datums[func->tg_level_varno]);
	if (TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		var->value = CStringGetTextDatum("ROW");
	else if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
		var->value = CStringGetTextDatum("STATEMENT");
	else
		elog(ERROR, "unrecognized trigger event type: not ROW or STATEMENT");
	var->isnull = false;
	var->freeval = true;

	var = (PLTSQL_var *) (estate.datums[func->tg_relid_varno]);
	var->value = ObjectIdGetDatum(trigdata->tg_relation->rd_id);
	var->isnull = false;
	var->freeval = false;

	var = (PLTSQL_var *) (estate.datums[func->tg_relname_varno]);
	var->value = DirectFunctionCall1(namein,
			CStringGetDatum(RelationGetRelationName(trigdata->tg_relation)));
	var->isnull = false;
	var->freeval = true;

	var = (PLTSQL_var *) (estate.datums[func->tg_table_name_varno]);
	var->value = DirectFunctionCall1(namein,
			CStringGetDatum(RelationGetRelationName(trigdata->tg_relation)));
	var->isnull = false;
	var->freeval = true;

	var = (PLTSQL_var *) (estate.datums[func->tg_table_schema_varno]);
	var->value = DirectFunctionCall1(namein,
									 CStringGetDatum(
													 get_namespace_name(
														RelationGetNamespace(
												   trigdata->tg_relation))));
	var->isnull = false;
	var->freeval = true;

	var = (PLTSQL_var *) (estate.datums[func->tg_nargs_varno]);
	var->value = Int16GetDatum(trigdata->tg_trigger->tgnargs);
	var->isnull = false;
	var->freeval = false;

	var = (PLTSQL_var *) (estate.datums[func->tg_argv_varno]);
	if (trigdata->tg_trigger->tgnargs > 0)
	{
		/*
		 * For historical reasons, tg_argv[] subscripts start at zero not one.
		 * So we can't use construct_array().
		 */
		int			nelems = trigdata->tg_trigger->tgnargs;
		Datum	   *elems;
		int			dims[1];
		int			lbs[1];

		elems = palloc(sizeof(Datum) * nelems);
		for (i = 0; i < nelems; i++)
			elems[i] = CStringGetTextDatum(trigdata->tg_trigger->tgargs[i]);
		dims[0] = nelems;
		lbs[0] = 0;

		var->value = PointerGetDatum(construct_md_array(elems, NULL,
														1, dims, lbs,
														TEXTOID,
														-1, false, 'i'));
		var->isnull = false;
		var->freeval = true;
	}
	else
	{
		var->value = (Datum) 0;
		var->isnull = true;
		var->freeval = false;
	}

	estate.err_text = gettext_noop("during function entry");

	/*
	 * Set the magic variable FOUND to false
	 */
	exec_set_found(&estate, false);

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plugin_ptr && (*plugin_ptr)->func_beg)
		((*plugin_ptr)->func_beg) (&estate, func);

	/*
	 * Now call the toplevel block of statements
	 */
	estate.err_text = NULL;
	estate.err_stmt = (PLTSQL_stmt *) (func->action);
	rc = exec_stmt_block(&estate, func->action);
	if (rc != PLTSQL_RC_RETURN)
	{
		estate.err_stmt = NULL;
		estate.err_text = NULL;

		/*
		 * Provide a more helpful message if a CONTINUE or RAISE has been used
		 * outside the context it can work in.
		 */
		if (rc == PLTSQL_RC_CONTINUE)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("CONTINUE cannot be used outside a loop")));
		else
			ereport(ERROR,
			   (errcode(ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
				errmsg("control reached end of trigger procedure without RETURN")));
	}

	estate.err_stmt = NULL;
	estate.err_text = gettext_noop("during function exit");

	if (estate.retisset)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("trigger procedure cannot return a set")));

	/*
	 * Check that the returned tuple structure has the same attributes, the
	 * relation that fired the trigger has. A per-statement trigger always
	 * needs to return NULL, so we ignore any return value the function itself
	 * produces (XXX: is this a good idea?)
	 *
	 * XXX This way it is possible, that the trigger returns a tuple where
	 * attributes don't have the correct atttypmod's length. It's up to the
	 * trigger's programmer to ensure that this doesn't happen. Jan
	 */
	if (estate.retisnull || !TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		rettup = NULL;
	else
	{
		TupleConversionMap *tupmap;

		rettup = (HeapTuple) DatumGetPointer(estate.retval);
		/* check rowtype compatibility */
		tupmap = convert_tuples_by_position(estate.rettupdesc,
											trigdata->tg_relation->rd_att,
											gettext_noop("returned row structure does not match the structure of the triggering table"));
		/* it might need conversion */
		if (tupmap)
			rettup = do_convert_tuple(rettup, tupmap);
		/* no need to free map, we're about to return anyway */

		/* Copy tuple to upper executor memory */
		rettup = SPI_copytuple(rettup);
	}

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plugin_ptr && (*plugin_ptr)->func_end)
		((*plugin_ptr)->func_end) (&estate, func);

	/* Clean up any leftover temporary memory */
	pltsql_destroy_econtext(&estate);
	exec_eval_cleanup(&estate);

	/*
	 * Pop the error context stack
	 */
	error_context_stack = plerrcontext.previous;

	/*
	 * Return the trigger's result
	 */
	return rettup;
}


/*
 * error context callback to let us supply a call-stack traceback
 */
static void
pltsql_exec_error_callback(void *arg)
{
	PLTSQL_execstate *estate = (PLTSQL_execstate *) arg;

	/* if we are doing RAISE, don't report its location */
	if (estate->err_text == raise_skip_msg)
		return;

	if (estate->err_text != NULL)
	{
		/*
		 * We don't expend the cycles to run gettext() on err_text unless we
		 * actually need it.  Therefore, places that set up err_text should
		 * use gettext_noop() to ensure the strings get recorded in the
		 * message dictionary.
		 *
		 * If both err_text and err_stmt are set, use the err_text as
		 * description, but report the err_stmt's line number.  When err_stmt
		 * is not set, we're in function entry/exit, or some such place not
		 * attached to a specific line number.
		 */
		if (estate->err_stmt != NULL)
		{
			/*
			 * translator: last %s is a phrase such as "during statement block
			 * local variable initialization"
			 */
			errcontext("PL/TSQL function %s line %d %s",
					   estate->func->fn_signature,
					   estate->err_stmt->lineno,
					   _(estate->err_text));
		}
		else
		{
			/*
			 * translator: last %s is a phrase such as "while storing call
			 * arguments into local variables"
			 */
			errcontext("PL/TSQL function %s %s",
					   estate->func->fn_signature,
					   _(estate->err_text));
		}
	}
	else if (estate->err_stmt != NULL)
	{
		/* translator: last %s is a pltsql statement type name */
		errcontext("PL/TSQL function %s line %d at %s",
				   estate->func->fn_signature,
				   estate->err_stmt->lineno,
				   pltsql_stmt_typename(estate->err_stmt));
	}
	else
		errcontext("PL/TSQL function %s",
				   estate->func->fn_signature);
}


/* ----------
 * Support function for initializing local execution variables
 * ----------
 */
static PLTSQL_datum *
copy_pltsql_datum(PLTSQL_datum *datum)
{
	PLTSQL_datum *result;

	switch (datum->dtype)
	{
		case PLTSQL_DTYPE_VAR:
			{
				PLTSQL_var *new = palloc(sizeof(PLTSQL_var));

				memcpy(new, datum, sizeof(PLTSQL_var));
				/* Ensure the value is null (possibly not needed?) */
				new->value = 0;
				new->isnull = true;
				new->freeval = false;

				result = (PLTSQL_datum *) new;
			}
			break;

		case PLTSQL_DTYPE_REC:
			{
				PLTSQL_rec *new = palloc(sizeof(PLTSQL_rec));

				memcpy(new, datum, sizeof(PLTSQL_rec));
				/* Ensure the value is null (possibly not needed?) */
				new->tup = NULL;
				new->tupdesc = NULL;
				new->freetup = false;
				new->freetupdesc = false;

				result = (PLTSQL_datum *) new;
			}
			break;

		case PLTSQL_DTYPE_ROW:
		case PLTSQL_DTYPE_RECFIELD:
		case PLTSQL_DTYPE_ARRAYELEM:

			/*
			 * These datum records are read-only at runtime, so no need to
			 * copy them (well, ARRAYELEM contains some cached type data,
			 * but we'd just as soon centralize the caching anyway)
			 */
			result = datum;
			break;

		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			result = NULL;		/* keep compiler quiet */
			break;
	}

	return result;
}

/*
 * Create a memory context for statement-lifespan variables, if we don't
 * have one already.  It will be a child of stmt_mcontext_parent, which is
 * either the function's main context or a pushed-down outer stmt_mcontext.
 */
static MemoryContext
get_stmt_mcontext(PLTSQL_execstate *estate)
{
	if (estate->stmt_mcontext == NULL)
	{
		estate->stmt_mcontext =
			AllocSetContextCreate(estate->stmt_mcontext_parent,
								  "PLpgSQL per-statement data",
								  ALLOCSET_DEFAULT_SIZES);
	}
	return estate->stmt_mcontext;
}

static bool
exception_matches_conditions(ErrorData *edata, PLTSQL_condition *cond)
{
	for (; cond != NULL; cond = cond->next)
	{
		int			sqlerrstate = cond->sqlerrstate;

		/*
		 * OTHERS matches everything *except* query-canceled; if you're
		 * foolish enough, you can match that explicitly.
		 */
		if (sqlerrstate == 0)
		{
			if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
				return true;
		}
		/* Exact match? */
		else if (edata->sqlerrcode == sqlerrstate)
			return true;
		/* Category match? */
		else if (ERRCODE_IS_CATEGORY(sqlerrstate) &&
				 ERRCODE_TO_CATEGORY(edata->sqlerrcode) == sqlerrstate)
			return true;
	}
	return false;
}


/* ----------
 * exec_stmt_block			Execute a block of statements
 * ----------
 */
static int
exec_stmt_block(PLTSQL_execstate *estate, PLTSQL_stmt_block *block)
{
	volatile int rc = -1;
	int			i;
	int			n;

	/*
	 * First initialize all variables declared in this block
	 */
	estate->err_text = gettext_noop("during statement block local variable initialization");

	for (i = 0; i < block->n_initvars; i++)
	{
		n = block->initvarnos[i];

		switch (estate->datums[n]->dtype)
		{
			case PLTSQL_DTYPE_VAR:
				{
					PLTSQL_var *var = (PLTSQL_var *) (estate->datums[n]);

					/* free any old value, in case re-entering block */
					free_var(var);

					/* Initially it contains a NULL */
					var->value = (Datum) 0;
					var->isnull = true;

					if (var->default_val == NULL)
					{
						/*
						 * If needed, give the datatype a chance to reject
						 * NULLs, by assigning a NULL to the variable. We
						 * claim the value is of type UNKNOWN, not the var's
						 * datatype, else coercion will be skipped. (Do this
						 * before the notnull check to be consistent with
						 * exec_assign_value.)
						 */
						if (!var->datatype->typinput.fn_strict)
							exec_assign_value(estate,
											  (PLTSQL_datum *) var,
											  (Datum) 0,
											  true,
											  UNKNOWNOID,
											  -1);
						if (var->notnull)
							ereport(ERROR,
									(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
									 errmsg("variable \"%s\" declared NOT NULL cannot default to NULL",
											var->refname)));
					}
					else
					{
						exec_assign_expr(estate, (PLTSQL_datum *) var,
										 var->default_val);
					}
				}
				break;

			case PLTSQL_DTYPE_REC:
				{
					PLTSQL_rec *rec = (PLTSQL_rec *) (estate->datums[n]);

					if (rec->freetup)
					{
						heap_freetuple(rec->tup);
						rec->freetup = false;
					}
					if (rec->freetupdesc)
					{
						FreeTupleDesc(rec->tupdesc);
						rec->freetupdesc = false;
					}
					rec->tup = NULL;
					rec->tupdesc = NULL;
				}
				break;

			case PLTSQL_DTYPE_RECFIELD:
			case PLTSQL_DTYPE_ARRAYELEM:
				break;

			default:
				elog(ERROR, "unrecognized dtype: %d",
					 estate->datums[n]->dtype);
		}
	}

	if (block->exceptions)
	{
		/*
		 * Execute the statements in the block's body inside a sub-transaction
		 */
		MemoryContext oldcontext = CurrentMemoryContext;
		ResourceOwner oldowner = CurrentResourceOwner;
		ExprContext *old_eval_econtext = estate->eval_econtext;
		ErrorData  *save_cur_error = estate->cur_error;

		estate->err_text = gettext_noop("during statement block entry");

		BeginInternalSubTransaction(NULL);
		/* Want to run statements inside function's memory context */
		MemoryContextSwitchTo(oldcontext);

		PG_TRY();
		{
			/*
			 * We need to run the block's statements with a new eval_econtext
			 * that belongs to the current subtransaction; if we try to use
			 * the outer econtext then ExprContext shutdown callbacks will be
			 * called at the wrong times.
			 */
			pltsql_create_econtext(estate);

			estate->err_text = NULL;

			/* Run the block's statements */
			rc = exec_stmts(estate, block->body);

			estate->err_text = gettext_noop("during statement block exit");

			/*
			 * If the block ended with RETURN, we may need to copy the return
			 * value out of the subtransaction eval_context.  This is
			 * currently only needed for scalar result types --- rowtype
			 * values will always exist in the function's own memory context.
			 */
			if (rc == PLTSQL_RC_RETURN &&
				!estate->retisset &&
				!estate->retisnull &&
				estate->rettupdesc == NULL)
			{
				int16		resTypLen;
				bool		resTypByVal;

				get_typlenbyval(estate->rettype, &resTypLen, &resTypByVal);
				estate->retval = datumCopy(estate->retval,
										   resTypByVal, resTypLen);
			}

			/* Commit the inner transaction, return to outer xact context */
			ReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = oldowner;

			/*
			 * Revert to outer eval_econtext.  (The inner one was
			 * automatically cleaned up during subxact exit.)
			 */
			estate->eval_econtext = old_eval_econtext;

			/*
			 * AtEOSubXact_SPI() should not have popped any SPI context, but
			 * just in case it did, make sure we remain connected.
			 */
			SPI_restore_connection();
		}
		PG_CATCH();
		{
			ErrorData  *edata;
			ListCell   *e;

			estate->err_text = gettext_noop("during exception cleanup");

			/* Save error info */
			MemoryContextSwitchTo(oldcontext);
			edata = CopyErrorData();
			FlushErrorState();

			/* Abort the inner transaction */
			RollbackAndReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = oldowner;

			/* Revert to outer eval_econtext */
			estate->eval_econtext = old_eval_econtext;

			/*
			 * If AtEOSubXact_SPI() popped any SPI context of the subxact, it
			 * will have left us in a disconnected state.  We need this hack
			 * to return to connected state.
			 */
			SPI_restore_connection();

			/* Must clean up the econtext too */
			exec_eval_cleanup(estate);

			/* Look for a matching exception handler */
			foreach(e, block->exceptions->exc_list)
			{
				PLTSQL_exception *exception = (PLTSQL_exception *) lfirst(e);

				if (exception_matches_conditions(edata, exception->conditions))
				{
					/*
					 * Initialize the magic SQLSTATE and SQLERRM variables for
					 * the exception block. We needn't do this until we have
					 * found a matching exception.
					 */
					PLTSQL_var *state_var;
					PLTSQL_var *errm_var;

					state_var = (PLTSQL_var *)
						estate->datums[block->exceptions->sqlstate_varno];
					errm_var = (PLTSQL_var *)
						estate->datums[block->exceptions->sqlerrm_varno];

					assign_text_var(state_var,
									unpack_sql_state(edata->sqlerrcode));
					assign_text_var(errm_var, edata->message);

					/*
					 * Also set up cur_error so the error data is accessible
					 * inside the handler.
					 */
					estate->cur_error = edata;

					estate->err_text = NULL;

					rc = exec_stmts(estate, exception->action);

					free_var(state_var);
					state_var->value = (Datum) 0;
					state_var->isnull = true;
					free_var(errm_var);
					errm_var->value = (Datum) 0;
					errm_var->isnull = true;

					break;
				}
			}

			/*
			 * Restore previous state of cur_error, whether or not we executed
			 * a handler.  This is needed in case an error got thrown from
			 * some inner block's exception handler.
			 */
			estate->cur_error = save_cur_error;

			/* If no match found, re-throw the error */
			if (e == NULL)
				ReThrowError(edata);
			else
				FreeErrorData(edata);
		}
		PG_END_TRY();

		Assert(save_cur_error == estate->cur_error);
	}
	else
	{
		/*
		 * Just execute the statements in the block's body
		 */
		estate->err_text = NULL;

		rc = exec_stmts(estate, block->body);
	}

	estate->err_text = NULL;

	/*
	 * Handle the return code.
	 */
	switch (rc)
	{
		case PLTSQL_RC_OK:
		case PLTSQL_RC_RETURN:
		case PLTSQL_RC_CONTINUE:
			return rc;

		case PLTSQL_RC_EXIT:

			/*
			 * This is intentionally different from the handling of RC_EXIT
			 * for loops: to match a block, we require a match by label.
			 */
			if (estate->exitlabel == NULL)
				return PLTSQL_RC_EXIT;
			if (block->label == NULL)
				return PLTSQL_RC_EXIT;
			if (strcmp(block->label, estate->exitlabel) != 0)
				return PLTSQL_RC_EXIT;
			estate->exitlabel = NULL;
			return PLTSQL_RC_OK;

		default:
			elog(ERROR, "unrecognized rc: %d", rc);
	}

	return PLTSQL_RC_OK;
}


/* ----------
 * exec_stmts			Iterate over a list of statements
 *				as long as their return code is OK
 * ----------
 */
static int
exec_stmts(PLTSQL_execstate *estate, List *stmts)
{
	ListCell   *s;

	if (stmts == NIL)
	{
		/*
		 * Ensure we do a CHECK_FOR_INTERRUPTS() even though there is no
		 * statement.  This prevents hangup in a tight loop if, for instance,
		 * there is a LOOP construct with an empty body.
		 */
		CHECK_FOR_INTERRUPTS();
		return PLTSQL_RC_OK;
	}

	foreach(s, stmts)
	{
		PLTSQL_stmt *stmt = (PLTSQL_stmt *) lfirst(s);
		int			rc = exec_stmt(estate, stmt);

		if (rc != PLTSQL_RC_OK)
			return rc;
	}

	return PLTSQL_RC_OK;
}


/* ----------
 * exec_stmt			Distribute one statement to the statements
 *				type specific execution function.
 * ----------
 */
static int
exec_stmt(PLTSQL_execstate *estate, PLTSQL_stmt *stmt)
{
	PLTSQL_stmt *save_estmt;
	int			rc = -1;

	save_estmt = estate->err_stmt;
	estate->err_stmt = stmt;

	/* Let the plugin know that we are about to execute this statement */
	if (*plugin_ptr && (*plugin_ptr)->stmt_beg)
		((*plugin_ptr)->stmt_beg) (estate, stmt);

	CHECK_FOR_INTERRUPTS();

	switch ((enum PLTSQL_stmt_types) stmt->cmd_type)
	{
		case PLTSQL_STMT_BLOCK:
			rc = exec_stmt_block(estate, (PLTSQL_stmt_block *) stmt);
			break;

		case PLTSQL_STMT_ASSIGN:
			rc = exec_stmt_assign(estate, (PLTSQL_stmt_assign *) stmt);
			break;

		case PLTSQL_STMT_PERFORM:
			rc = exec_stmt_perform(estate, (PLTSQL_stmt_perform *) stmt);
			break;

		case PLTSQL_STMT_GETDIAG:
			rc = exec_stmt_getdiag(estate, (PLTSQL_stmt_getdiag *) stmt);
			break;

		case PLTSQL_STMT_IF:
			rc = exec_stmt_if(estate, (PLTSQL_stmt_if *) stmt);
			break;

		case PLTSQL_STMT_CASE:
			rc = exec_stmt_case(estate, (PLTSQL_stmt_case *) stmt);
			break;

		case PLTSQL_STMT_LOOP:
			rc = exec_stmt_loop(estate, (PLTSQL_stmt_loop *) stmt);
			break;

		case PLTSQL_STMT_WHILE:
			rc = exec_stmt_while(estate, (PLTSQL_stmt_while *) stmt);
			break;

		case PLTSQL_STMT_FORI:
			rc = exec_stmt_fori(estate, (PLTSQL_stmt_fori *) stmt);
			break;

		case PLTSQL_STMT_FORS:
			rc = exec_stmt_fors(estate, (PLTSQL_stmt_fors *) stmt);
			break;

		case PLTSQL_STMT_FORC:
			rc = exec_stmt_forc(estate, (PLTSQL_stmt_forc *) stmt);
			break;

		case PLTSQL_STMT_FOREACH_A:
			rc = exec_stmt_foreach_a(estate, (PLTSQL_stmt_foreach_a *) stmt);
			break;

		case PLTSQL_STMT_EXIT:
			rc = exec_stmt_exit(estate, (PLTSQL_stmt_exit *) stmt);
			break;

		case PLTSQL_STMT_RETURN:
			rc = exec_stmt_return(estate, (PLTSQL_stmt_return *) stmt);
			break;

		case PLTSQL_STMT_RETURN_NEXT:
			rc = exec_stmt_return_next(estate, (PLTSQL_stmt_return_next *) stmt);
			break;

		case PLTSQL_STMT_RETURN_QUERY:
			rc = exec_stmt_return_query(estate, (PLTSQL_stmt_return_query *) stmt);
			break;

		case PLTSQL_STMT_RAISE:
			rc = exec_stmt_raise(estate, (PLTSQL_stmt_raise *) stmt);
			break;

		case PLTSQL_STMT_EXECSQL:
			rc = exec_stmt_execsql(estate, (PLTSQL_stmt_execsql *) stmt);
			break;

		case PLTSQL_STMT_DYNEXECUTE:
			rc = exec_stmt_dynexecute(estate, (PLTSQL_stmt_dynexecute *) stmt);
			break;

		case PLTSQL_STMT_DYNFORS:
			rc = exec_stmt_dynfors(estate, (PLTSQL_stmt_dynfors *) stmt);
			break;

		case PLTSQL_STMT_OPEN:
			rc = exec_stmt_open(estate, (PLTSQL_stmt_open *) stmt);
			break;

		case PLTSQL_STMT_FETCH:
			rc = exec_stmt_fetch(estate, (PLTSQL_stmt_fetch *) stmt);
			break;

		case PLTSQL_STMT_CLOSE:
			rc = exec_stmt_close(estate, (PLTSQL_stmt_close *) stmt);
			break;

		default:
			estate->err_stmt = save_estmt;
			elog(ERROR, "unrecognized cmdtype: %d", stmt->cmd_type);
	}

	/* Let the plugin know that we have finished executing this statement */
	if (*plugin_ptr && (*plugin_ptr)->stmt_end)
		((*plugin_ptr)->stmt_end) (estate, stmt);

	estate->err_stmt = save_estmt;

	return rc;
}


/* ----------
 * exec_stmt_assign			Evaluate an expression and
 *					put the result into a variable.
 * ----------
 */
static int
exec_stmt_assign(PLTSQL_execstate *estate, PLTSQL_stmt_assign *stmt)
{
	Assert(stmt->varno >= 0);

	exec_assign_expr(estate, estate->datums[stmt->varno], stmt->expr);

	return PLTSQL_RC_OK;
}

/* ----------
 * exec_stmt_perform		Evaluate query and discard result (but set
 *							FOUND depending on whether at least one row
 *							was returned).
 * ----------
 */
static int
exec_stmt_perform(PLTSQL_execstate *estate, PLTSQL_stmt_perform *stmt)
{
	PLTSQL_expr *expr = stmt->expr;

	(void) exec_run_select(estate, expr, 0, NULL);
	exec_set_found(estate, (estate->eval_processed != 0));
	exec_eval_cleanup(estate);

	return PLTSQL_RC_OK;
}

/* ----------
 * exec_stmt_getdiag					Put internal PG information into
 *										specified variables.
 * ----------
 */
static int
exec_stmt_getdiag(PLTSQL_execstate *estate, PLTSQL_stmt_getdiag *stmt)
{
	ListCell   *lc;

	/*
	 * GET STACKED DIAGNOSTICS is only valid inside an exception handler.
	 *
	 * Note: we trust the grammar to have disallowed the relevant item kinds
	 * if not is_stacked, otherwise we'd dump core below.
	 */
	if (stmt->is_stacked && estate->cur_error == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER),
				 errmsg("GET STACKED DIAGNOSTICS cannot be used outside an exception handler")));

	foreach(lc, stmt->diag_items)
	{
		PLTSQL_diag_item *diag_item = (PLTSQL_diag_item *) lfirst(lc);
		PLTSQL_datum *var = estate->datums[diag_item->target];

		switch (diag_item->kind)
		{
			case PLTSQL_GETDIAG_ROW_COUNT:
				exec_assign_value(estate, var,
								  UInt32GetDatum(estate->eval_processed),
								  INT4OID, false, -1);
				break;

			case PLTSQL_GETDIAG_RESULT_OID:
				exec_assign_value(estate, var,
								  ObjectIdGetDatum(estate->eval_lastoid),
								  OIDOID, false, -1);
				break;

			case PLTSQL_GETDIAG_ERROR_CONTEXT:
				exec_assign_c_string(estate, var,
									 estate->cur_error->context);
				break;

			case PLTSQL_GETDIAG_ERROR_DETAIL:
				exec_assign_c_string(estate, var,
									 estate->cur_error->detail);
				break;

			case PLTSQL_GETDIAG_ERROR_HINT:
				exec_assign_c_string(estate, var,
									 estate->cur_error->hint);
				break;

			case PLTSQL_GETDIAG_RETURNED_SQLSTATE:
				exec_assign_c_string(estate, var,
									 unpack_sql_state(estate->cur_error->sqlerrcode));
				break;

			case PLTSQL_GETDIAG_MESSAGE_TEXT:
				exec_assign_c_string(estate, var,
									 estate->cur_error->message);
				break;

			default:
				elog(ERROR, "unrecognized diagnostic item kind: %d",
					 diag_item->kind);
		}
	}

	return PLTSQL_RC_OK;
}

/* ----------
 * exec_stmt_if				Evaluate a bool expression and
 *					execute the true or false body
 *					conditionally.
 * ----------
 */
static int
exec_stmt_if(PLTSQL_execstate *estate, PLTSQL_stmt_if *stmt)
{
	bool		value;
	bool		isnull;
	ListCell   *lc;

	value = exec_eval_boolean(estate, stmt->cond, &isnull);
	exec_eval_cleanup(estate);
	if (!isnull && value)
		return exec_stmts(estate, stmt->then_body);

	foreach(lc, stmt->elsif_list)
	{
		PLTSQL_if_elsif *elif = (PLTSQL_if_elsif *) lfirst(lc);

		value = exec_eval_boolean(estate, elif->cond, &isnull);
		exec_eval_cleanup(estate);
		if (!isnull && value)
			return exec_stmts(estate, elif->stmts);
	}

	return exec_stmts(estate, stmt->else_body);
}


/*-----------
 * exec_stmt_case
 *-----------
 */
static int
exec_stmt_case(PLTSQL_execstate *estate, PLTSQL_stmt_case *stmt)
{
	PLTSQL_var *t_var = NULL;
	bool		isnull;
	ListCell   *l;

	if (stmt->t_expr != NULL)
	{
		/* simple case */
		Datum		t_val;
		Oid			t_oid;
		int32		t_typmod;

		t_val = exec_eval_expr(estate, stmt->t_expr, &isnull, &t_oid, &t_typmod);

		t_var = (PLTSQL_var *) estate->datums[stmt->t_varno];

		/*
		 * When expected datatype is different from real, change it. Note that
		 * what we're modifying here is an execution copy of the datum, so
		 * this doesn't affect the originally stored function parse tree.
		 */
		if (t_var->datatype->typoid != t_oid ||	
			t_var->datatype->atttypmod != t_typmod)
			t_var->datatype = pltsql_build_datatype(t_oid,
													t_typmod,
													estate->func->fn_input_collation);

		/* now we can assign to the variable */
		exec_assign_value(estate,
						  (PLTSQL_datum *) t_var,
						  t_val,
						  isnull,
						  t_oid,
						  t_typmod);

		exec_eval_cleanup(estate);
	}

	/* Now search for a successful WHEN clause */
	foreach(l, stmt->case_when_list)
	{
		PLTSQL_case_when *cwt = (PLTSQL_case_when *) lfirst(l);
		bool		value;

		value = exec_eval_boolean(estate, cwt->expr, &isnull);
		exec_eval_cleanup(estate);
		if (!isnull && value)
		{
			/* Found it */

			/* We can now discard any value we had for the temp variable */
			if (t_var != NULL)
			{
				free_var(t_var);
				t_var->value = (Datum) 0;
				t_var->isnull = true;
			}

			/* Evaluate the statement(s), and we're done */
			return exec_stmts(estate, cwt->stmts);
		}
	}

	/* We can now discard any value we had for the temp variable */
	if (t_var != NULL)
	{
		free_var(t_var);
		t_var->value = (Datum) 0;
		t_var->isnull = true;
	}

	/* SQL2003 mandates this error if there was no ELSE clause */
	if (!stmt->have_else)
		ereport(ERROR,
				(errcode(ERRCODE_CASE_NOT_FOUND),
				 errmsg("case not found"),
				 errhint("CASE statement is missing ELSE part.")));

	/* Evaluate the ELSE statements, and we're done */
	return exec_stmts(estate, stmt->else_stmts);
}


/* ----------
 * exec_stmt_loop			Loop over statements until
 *					an exit occurs.
 * ----------
 */
static int
exec_stmt_loop(PLTSQL_execstate *estate, PLTSQL_stmt_loop *stmt)
{
	for (;;)
	{
		int			rc = exec_stmts(estate, stmt->body);

		switch (rc)
		{
			case PLTSQL_RC_OK:
				break;

			case PLTSQL_RC_EXIT:
				if (estate->exitlabel == NULL)
					return PLTSQL_RC_OK;
				if (stmt->label == NULL)
					return PLTSQL_RC_EXIT;
				if (strcmp(stmt->label, estate->exitlabel) != 0)
					return PLTSQL_RC_EXIT;
				estate->exitlabel = NULL;
				return PLTSQL_RC_OK;

			case PLTSQL_RC_CONTINUE:
				if (estate->exitlabel == NULL)
					/* anonymous continue, so re-run the loop */
					break;
				else if (stmt->label != NULL &&
						 strcmp(stmt->label, estate->exitlabel) == 0)
					/* label matches named continue, so re-run loop */
					estate->exitlabel = NULL;
				else
					/* label doesn't match named continue, so propagate upward */
					return PLTSQL_RC_CONTINUE;
				break;

			case PLTSQL_RC_RETURN:
				return rc;

			default:
				elog(ERROR, "unrecognized rc: %d", rc);
		}
	}

	return PLTSQL_RC_OK;
}


/* ----------
 * exec_stmt_while			Loop over statements as long
 *					as an expression evaluates to
 *					true or an exit occurs.
 * ----------
 */
static int
exec_stmt_while(PLTSQL_execstate *estate, PLTSQL_stmt_while *stmt)
{
	for (;;)
	{
		int			rc;
		bool		value;
		bool		isnull;

		value = exec_eval_boolean(estate, stmt->cond, &isnull);
		exec_eval_cleanup(estate);

		if (isnull || !value)
			break;

		rc = exec_stmts(estate, stmt->body);

		switch (rc)
		{
			case PLTSQL_RC_OK:
				break;

			case PLTSQL_RC_EXIT:
				if (estate->exitlabel == NULL)
					return PLTSQL_RC_OK;
				if (stmt->label == NULL)
					return PLTSQL_RC_EXIT;
				if (strcmp(stmt->label, estate->exitlabel) != 0)
					return PLTSQL_RC_EXIT;
				estate->exitlabel = NULL;
				return PLTSQL_RC_OK;

			case PLTSQL_RC_CONTINUE:
				if (estate->exitlabel == NULL)
					/* anonymous continue, so re-run loop */
					break;
				else if (stmt->label != NULL &&
						 strcmp(stmt->label, estate->exitlabel) == 0)
					/* label matches named continue, so re-run loop */
					estate->exitlabel = NULL;
				else
					/* label doesn't match named continue, propagate upward */
					return PLTSQL_RC_CONTINUE;
				break;

			case PLTSQL_RC_RETURN:
				return rc;

			default:
				elog(ERROR, "unrecognized rc: %d", rc);
		}
	}

	return PLTSQL_RC_OK;
}


/* ----------
 * exec_stmt_fori			Iterate an integer variable
 *					from a lower to an upper value
 *					incrementing or decrementing by the BY value
 * ----------
 */
static int
exec_stmt_fori(PLTSQL_execstate *estate, PLTSQL_stmt_fori *stmt)
{
	PLTSQL_var *var;
	Datum		value;
	bool		isnull;
	Oid			valtype;
	int32		valtypmod;
	int32		loop_value;
	int32		end_value;
	int32		step_value;
	bool		found = false;
	int			rc = PLTSQL_RC_OK;

	var = (PLTSQL_var *) (estate->datums[stmt->var->dno]);

	/*
	 * Get the value of the lower bound
	 */
	value = exec_eval_expr(estate, stmt->lower, &isnull, &valtype, &valtypmod);
	value = exec_cast_value(estate, value, &isnull,
							valtype, valtypmod,
							var->datatype->typoid,
							var->datatype->atttypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("lower bound of FOR loop cannot be null")));
	loop_value = DatumGetInt32(value);
	exec_eval_cleanup(estate);

	/*
	 * Get the value of the upper bound
	 */
	value = exec_eval_expr(estate, stmt->upper, &isnull, &valtype, &valtypmod);
	value = exec_cast_value(estate, value, &isnull,
							valtype, valtypmod,
							var->datatype->typoid,
							var->datatype->atttypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("upper bound of FOR loop cannot be null")));
	end_value = DatumGetInt32(value);
	exec_eval_cleanup(estate);

	/*
	 * Get the step value
	 */
	if (stmt->step)
	{
		value = exec_eval_expr(estate, stmt->step, &isnull, &valtype, &valtypmod);
		value = exec_cast_value(estate, value, &isnull,
								valtype, valtypmod,
								var->datatype->typoid,
								var->datatype->atttypmod);
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("BY value of FOR loop cannot be null")));
		step_value = DatumGetInt32(value);
		exec_eval_cleanup(estate);
		if (step_value <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				  errmsg("BY value of FOR loop must be greater than zero")));
	}
	else
		step_value = 1;

	/*
	 * Now do the loop
	 */
	for (;;)
	{
		/*
		 * Check against upper bound
		 */
		if (stmt->reverse)
		{
			if (loop_value < end_value)
				break;
		}
		else
		{
			if (loop_value > end_value)
				break;
		}

		found = true;			/* looped at least once */

		/*
		 * Assign current value to loop var
		 */
		var->value = Int32GetDatum(loop_value);
		var->isnull = false;

		/*
		 * Execute the statements
		 */
		rc = exec_stmts(estate, stmt->body);

		if (rc == PLTSQL_RC_RETURN)
			break;				/* break out of the loop */
		else if (rc == PLTSQL_RC_EXIT)
		{
			if (estate->exitlabel == NULL)
				/* unlabelled exit, finish the current loop */
				rc = PLTSQL_RC_OK;
			else if (stmt->label != NULL &&
					 strcmp(stmt->label, estate->exitlabel) == 0)
			{
				/* labelled exit, matches the current stmt's label */
				estate->exitlabel = NULL;
				rc = PLTSQL_RC_OK;
			}

			/*
			 * otherwise, this is a labelled exit that does not match the
			 * current statement's label, if any: return RC_EXIT so that the
			 * EXIT continues to propagate up the stack.
			 */
			break;
		}
		else if (rc == PLTSQL_RC_CONTINUE)
		{
			if (estate->exitlabel == NULL)
				/* unlabelled continue, so re-run the current loop */
				rc = PLTSQL_RC_OK;
			else if (stmt->label != NULL &&
					 strcmp(stmt->label, estate->exitlabel) == 0)
			{
				/* label matches named continue, so re-run loop */
				estate->exitlabel = NULL;
				rc = PLTSQL_RC_OK;
			}
			else
			{
				/*
				 * otherwise, this is a named continue that does not match the
				 * current statement's label, if any: return RC_CONTINUE so
				 * that the CONTINUE will propagate up the stack.
				 */
				break;
			}
		}

		/*
		 * Increase/decrease loop value, unless it would overflow, in which
		 * case exit the loop.
		 */
		if (stmt->reverse)
		{
			if ((int32) (loop_value - step_value) > loop_value)
				break;
			loop_value -= step_value;
		}
		else
		{
			if ((int32) (loop_value + step_value) < loop_value)
				break;
			loop_value += step_value;
		}
	}

	/*
	 * Set the FOUND variable to indicate the result of executing the loop
	 * (namely, whether we looped one or more times). This must be set here so
	 * that it does not interfere with the value of the FOUND variable inside
	 * the loop processing itself.
	 */
	exec_set_found(estate, found);

	return rc;
}


/* ----------
 * exec_stmt_fors			Execute a query, assign each
 *					tuple to a record or row and
 *					execute a group of statements
 *					for it.
 * ----------
 */
static int
exec_stmt_fors(PLTSQL_execstate *estate, PLTSQL_stmt_fors *stmt)
{
	Portal		portal;
	int			rc;

	/*
	 * Open the implicit cursor for the statement using exec_run_select
	 */
	exec_run_select(estate, stmt->query, 0, &portal);

	/*
	 * Execute the loop
	 */
	rc = exec_for_query(estate, (PLTSQL_stmt_forq *) stmt, portal, true);

	/*
	 * Close the implicit cursor
	 */
	SPI_cursor_close(portal);

	return rc;
}


/* ----------
 * exec_stmt_forc			Execute a loop for each row from a cursor.
 * ----------
 */
static int
exec_stmt_forc(PLTSQL_execstate *estate, PLTSQL_stmt_forc *stmt)
{
	PLTSQL_var *curvar;
	char	   *curname = NULL;
	PLTSQL_expr *query;
	ParamListInfo paramLI;
	Portal		portal;
	int			rc;

	/* ----------
	 * Get the cursor variable and if it has an assigned name, check
	 * that it's not in use currently.
	 * ----------
	 */
	curvar = (PLTSQL_var *) (estate->datums[stmt->curvar]);
	if (!curvar->isnull)
	{
		curname = TextDatumGetCString(curvar->value);
		if (SPI_cursor_find(curname) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_CURSOR),
					 errmsg("cursor \"%s\" already in use", curname)));
	}

	/* ----------
	 * Open the cursor just like an OPEN command
	 *
	 * Note: parser should already have checked that statement supplies
	 * args iff cursor needs them, but we check again to be safe.
	 * ----------
	 */
	if (stmt->argquery != NULL)
	{
		/* ----------
		 * OPEN CURSOR with args.  We fake a SELECT ... INTO ...
		 * statement to evaluate the args and put 'em into the
		 * internal row.
		 * ----------
		 */
		PLTSQL_stmt_execsql set_args;

		if (curvar->cursor_explicit_argrow < 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("arguments given for cursor without arguments")));

		memset(&set_args, 0, sizeof(set_args));
		set_args.cmd_type = PLTSQL_STMT_EXECSQL;
		set_args.lineno = stmt->lineno;
		set_args.sqlstmt = stmt->argquery;
		set_args.into = true;
		/* XXX historically this has not been STRICT */
		set_args.target = (PLTSQL_variable *)
			(estate->datums[curvar->cursor_explicit_argrow]);

		if (exec_stmt_execsql(estate, &set_args) != PLTSQL_RC_OK)
			elog(ERROR, "open cursor failed during argument processing");
	}
	else
	{
		if (curvar->cursor_explicit_argrow >= 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("arguments required for cursor")));
	}

	query = curvar->cursor_explicit_expr;
	Assert(query);

	if (query->plan == NULL)
		exec_prepare_plan(estate, query, curvar->cursor_options, true);

	/*
	 * Set up ParamListInfo (hook function and possibly data values)
	 */
	paramLI = setup_param_list(estate, query);

	/*
	 * Open the cursor (the paramlist will get copied into the portal)
	 */
	portal = SPI_cursor_open_with_paramlist(curname, query->plan,
											paramLI,
											estate->readonly_func);
	if (portal == NULL)
		elog(ERROR, "could not open cursor: %s",
			 SPI_result_code_string(SPI_result));

	/* don't need paramlist any more */
	if (paramLI)
		pfree(paramLI);

	/*
	 * If cursor variable was NULL, store the generated portal name in it
	 */
	if (curname == NULL)
		assign_text_var(curvar, portal->name);

	/*
	 * Execute the loop.  We can't prefetch because the cursor is accessible
	 * to the user, for instance via UPDATE WHERE CURRENT OF within the loop.
	 */
	rc = exec_for_query(estate, (PLTSQL_stmt_forq *) stmt, portal, false);

	/* ----------
	 * Close portal, and restore cursor variable if it was initially NULL.
	 * ----------
	 */
	SPI_cursor_close(portal);

	if (curname == NULL)
	{
		free_var(curvar);
		curvar->value = (Datum) 0;
		curvar->isnull = true;
	}

	if (curname)
		pfree(curname);

	return rc;
}


/* ----------
 * exec_stmt_foreach_a			Loop over elements or slices of an array
 *
 * When looping over elements, the loop variable is the same type that the
 * array stores (eg: integer), when looping through slices, the loop variable
 * is an array of size and dimensions to match the size of the slice.
 * ----------
 */
static int
exec_stmt_foreach_a(PLTSQL_execstate *estate, PLTSQL_stmt_foreach_a *stmt)
{
	ArrayType  *arr;
	Oid			arrtype;
	int32		arrtypmod;
	PLTSQL_datum *loop_var;
	Oid			loop_var_elem_type;
	bool		found = false;
	int			rc = PLTSQL_RC_OK;
	ArrayIterator array_iterator;
	Oid			iterator_result_type;
	int32		iterator_result_typmod;
	Datum		value;
	bool		isnull;

	/* get the value of the array expression */
	value = exec_eval_expr(estate, stmt->expr, &isnull, &arrtype, &arrtypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("FOREACH expression must not be null")));

	/* check the type of the expression - must be an array */
	if (!OidIsValid(get_element_type(arrtype)))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("FOREACH expression must yield an array, not type %s",
						format_type_be(arrtype))));

	/*
	 * We must copy the array, else it will disappear in exec_eval_cleanup.
	 * This is annoying, but cleanup will certainly happen while running the
	 * loop body, so we have little choice.
	 */
	arr = DatumGetArrayTypePCopy(value);

	/* Clean up any leftover temporary memory */
	exec_eval_cleanup(estate);

	/* Slice dimension must be less than or equal to array dimension */
	if (stmt->slice < 0 || stmt->slice > ARR_NDIM(arr))
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
			   errmsg("slice dimension (%d) is out of the valid range 0..%d",
					  stmt->slice, ARR_NDIM(arr))));

	/* Set up the loop variable and see if it is of an array type */
	loop_var = estate->datums[stmt->varno];
	if (loop_var->dtype == PLTSQL_DTYPE_REC ||
		loop_var->dtype == PLTSQL_DTYPE_ROW)
	{
		/*
		 * Record/row variable is certainly not of array type, and might not
		 * be initialized at all yet, so don't try to get its type
		 */
		loop_var_elem_type = InvalidOid;
	}
	else
		loop_var_elem_type = get_element_type(exec_get_datum_type(estate,
																  loop_var));

	/*
	 * Sanity-check the loop variable type.  We don't try very hard here, and
	 * should not be too picky since it's possible that exec_assign_value can
	 * coerce values of different types.  But it seems worthwhile to complain
	 * if the array-ness of the loop variable is not right.
	 */
	if (stmt->slice > 0 && loop_var_elem_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
		errmsg("FOREACH ... SLICE loop variable must be of an array type")));
	if (stmt->slice == 0 && loop_var_elem_type != InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
			  errmsg("FOREACH loop variable must not be of an array type")));

	/* Create an iterator to step through the array */
	array_iterator = array_create_iterator(arr, stmt->slice, NULL);

	/* Identify iterator result type */
	if (stmt->slice > 0)
	{
		/* When slicing, nominal type of result is same as array type */
		iterator_result_type = arrtype;
		iterator_result_typmod = arrtypmod;
	}
	else
	{
		/* Without slicing, results are individual array elements */
		iterator_result_type = ARR_ELEMTYPE(arr);
		iterator_result_typmod = arrtypmod;
	}

	/* Iterate over the array elements or slices */
	while (array_iterate(array_iterator, &value, &isnull))
	{
		found = true;			/* looped at least once */

		/* Assign current element/slice to the loop variable */
		exec_assign_value(estate, loop_var, value, isnull, 
						  iterator_result_type, iterator_result_typmod);

		/* In slice case, value is temporary; must free it to avoid leakage */
		if (stmt->slice > 0)
			pfree(DatumGetPointer(value));

		/*
		 * Execute the statements
		 */
		rc = exec_stmts(estate, stmt->body);

		/* Handle the return code */
		if (rc == PLTSQL_RC_RETURN)
			break;				/* break out of the loop */
		else if (rc == PLTSQL_RC_EXIT)
		{
			if (estate->exitlabel == NULL)
				/* unlabelled exit, finish the current loop */
				rc = PLTSQL_RC_OK;
			else if (stmt->label != NULL &&
					 strcmp(stmt->label, estate->exitlabel) == 0)
			{
				/* labelled exit, matches the current stmt's label */
				estate->exitlabel = NULL;
				rc = PLTSQL_RC_OK;
			}

			/*
			 * otherwise, this is a labelled exit that does not match the
			 * current statement's label, if any: return RC_EXIT so that the
			 * EXIT continues to propagate up the stack.
			 */
			break;
		}
		else if (rc == PLTSQL_RC_CONTINUE)
		{
			if (estate->exitlabel == NULL)
				/* unlabelled continue, so re-run the current loop */
				rc = PLTSQL_RC_OK;
			else if (stmt->label != NULL &&
					 strcmp(stmt->label, estate->exitlabel) == 0)
			{
				/* label matches named continue, so re-run loop */
				estate->exitlabel = NULL;
				rc = PLTSQL_RC_OK;
			}
			else
			{
				/*
				 * otherwise, this is a named continue that does not match the
				 * current statement's label, if any: return RC_CONTINUE so
				 * that the CONTINUE will propagate up the stack.
				 */
				break;
			}
		}
	}

	/* Release temporary memory, including the array value */
	array_free_iterator(array_iterator);
	pfree(arr);

	/*
	 * Set the FOUND variable to indicate the result of executing the loop
	 * (namely, whether we looped one or more times). This must be set here so
	 * that it does not interfere with the value of the FOUND variable inside
	 * the loop processing itself.
	 */
	exec_set_found(estate, found);

	return rc;
}


/* ----------
 * exec_stmt_exit			Implements EXIT and CONTINUE
 *
 * This begins the process of exiting / restarting a loop.
 * ----------
 */
static int
exec_stmt_exit(PLTSQL_execstate *estate, PLTSQL_stmt_exit *stmt)
{
	/*
	 * If the exit / continue has a condition, evaluate it
	 */
	if (stmt->cond != NULL)
	{
		bool		value;
		bool		isnull;

		value = exec_eval_boolean(estate, stmt->cond, &isnull);
		exec_eval_cleanup(estate);
		if (isnull || value == false)
			return PLTSQL_RC_OK;
	}

	estate->exitlabel = stmt->label;
	if (stmt->is_exit)
		return PLTSQL_RC_EXIT;
	else
		return PLTSQL_RC_CONTINUE;
}


/* ----------
 * exec_stmt_return			Evaluate an expression and start
 *					returning from the function.
 * ----------
 */
static int
exec_stmt_return(PLTSQL_execstate *estate, PLTSQL_stmt_return *stmt)
{
	/*
	 * If processing a set-returning PL/TSQL function, the final RETURN
	 * indicates that the function is finished producing tuples.  The rest of
	 * the work will be done at the top level.
	 */
	if (estate->retisset)
		return PLTSQL_RC_RETURN;

	/* initialize for null result (possibly a tuple) */
	estate->retval = (Datum) 0;
	estate->rettupdesc = NULL;
	estate->retisnull = true;

	if (stmt->retvarno >= 0)
	{
		PLTSQL_datum *retvar = estate->datums[stmt->retvarno];

		switch (retvar->dtype)
		{
			case PLTSQL_DTYPE_VAR:
				{
					PLTSQL_var *var = (PLTSQL_var *) retvar;

					estate->retval = var->value;
					estate->retisnull = var->isnull;
					estate->rettype = var->datatype->typoid;
				}
				break;

			case PLTSQL_DTYPE_REC:
				{
					PLTSQL_rec *rec = (PLTSQL_rec *) retvar;

					if (HeapTupleIsValid(rec->tup))
					{
						estate->retval = PointerGetDatum(rec->tup);
						estate->rettupdesc = rec->tupdesc;
						estate->retisnull = false;
					}
				}
				break;

			case PLTSQL_DTYPE_ROW:
				{
					PLTSQL_row *row = (PLTSQL_row *) retvar;

					Assert(row->rowtupdesc);
					estate->retval =
						PointerGetDatum(make_tuple_from_row(estate, row,
															row->rowtupdesc));
					if (DatumGetPointer(estate->retval) == NULL)		/* should not happen */
						elog(ERROR, "row not compatible with its own tupdesc");
					estate->rettupdesc = row->rowtupdesc;
					estate->retisnull = false;
				}
				break;

			default:
				elog(ERROR, "unrecognized dtype: %d", retvar->dtype);
		}

		return PLTSQL_RC_RETURN;
	}

	if (stmt->expr != NULL)
	{
		if (estate->retistuple)
		{
			exec_run_select(estate, stmt->expr, 1, NULL);
			if (estate->eval_processed > 0)
			{
				estate->retval = PointerGetDatum(estate->eval_tuptable->vals[0]);
				estate->rettupdesc = estate->eval_tuptable->tupdesc;
				estate->retisnull = false;
			}
		}
		else
		{
			int32 rettypmod;

			/* Normal case for scalar results */
			estate->retval = exec_eval_expr(estate, stmt->expr,
											&(estate->retisnull),
											&(estate->rettype),
											&rettypmod);
		}

		return PLTSQL_RC_RETURN;
	}

	/*
	 * Special hack for function returning VOID: instead of NULL, return a
	 * non-null VOID value.  This is of dubious importance but is kept for
	 * backwards compatibility.  Note that the only other way to get here is
	 * to have written "RETURN NULL" in a function returning tuple.
	 */
	if (estate->fn_rettype == VOIDOID)
	{
		estate->retval = (Datum) 0;
		estate->retisnull = false;
		estate->rettype = VOIDOID;
	}

	return PLTSQL_RC_RETURN;
}

/* ----------
 * exec_stmt_return_next		Evaluate an expression and add it to the
 *								list of tuples returned by the current
 *								SRF.
 * ----------
 */
static int
exec_stmt_return_next(PLTSQL_execstate *estate,
					  PLTSQL_stmt_return_next *stmt)
{
	TupleDesc	tupdesc;
	int			natts;
	HeapTuple	tuple;
	MemoryContext oldcontext;

	if (!estate->retisset)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot use RETURN NEXT in a non-SETOF function")));

	if (estate->tuple_store == NULL)
		exec_init_tuple_store(estate);

	/* tuple_store_desc will be filled by exec_init_tuple_store */
	tupdesc = estate->tuple_store_desc;
	natts = tupdesc->natts;

	/*
	 * Special case path when the RETURN NEXT expression is a simple variable
	 * reference; in particular, this path is always taken in functions with
	 * one or more OUT parameters.
	 *
	 * Unlike exec_statement_return, there's no special win here for R/W
	 * expanded values, since they'll have to get flattened to go into the
	 * tuplestore.  Indeed, we'd better make them R/O to avoid any risk of the
	 * casting step changing them in-place.
	 */
	if (stmt->retvarno >= 0)
	{
		PLTSQL_datum *retvar = estate->datums[stmt->retvarno];

		switch (retvar->dtype)
		{
#ifdef PLTSQL_DTYPE_PROMISE
			case PLTSQL_DTYPE_PROMISE:
				/* fulfill promise if needed, then handle like regular var */
				pltsql_fulfill_promise(estate, (PLTSQL_var *) retvar);

				/* FALL THRU */
#endif
			case PLTSQL_DTYPE_VAR:
				{
					PLTSQL_var *var = (PLTSQL_var *) retvar;
					Datum		retval = var->value;
					bool		isNull = var->isnull;
					Form_pg_attribute attr = TupleDescAttr(tupdesc, 0);

					if (natts != 1)
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("wrong result type supplied in RETURN NEXT")));

					/* let's be very paranoid about the cast step */
					retval = MakeExpandedObjectReadOnly(retval,
														isNull,
														var->datatype->typlen);

					/* coerce type if needed */
					retval = exec_cast_value(estate,
											 retval,
											 &isNull,
											 var->datatype->typoid,
											 var->datatype->atttypmod,
											 attr->atttypid,
											 attr->atttypmod);

					tuplestore_putvalues(estate->tuple_store, tupdesc,
										 &retval, &isNull);
				}
				break;

			case PLTSQL_DTYPE_REC:
				{
					PLTSQL_rec *rec = (PLTSQL_rec *) retvar;
					TupleDesc	rec_tupdesc;
					TupleConversionMap *tupmap;

					/* If rec is null, try to convert it to a row of nulls */
					if (rec->erh == NULL)
						instantiate_empty_record_variable(estate, rec);
					if (ExpandedRecordIsEmpty(rec->erh))
						deconstruct_expanded_record(rec->erh);

					/* Use eval_mcontext for tuple conversion work */
					oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
					rec_tupdesc = expanded_record_get_tupdesc(rec->erh);
					tupmap = convert_tuples_by_position(rec_tupdesc,
														tupdesc,
														gettext_noop("wrong record type supplied in RETURN NEXT"));
					tuple = expanded_record_get_tuple(rec->erh);
					if (tupmap)
						tuple = do_convert_tuple(tuple, tupmap);
					tuplestore_puttuple(estate->tuple_store, tuple);
					MemoryContextSwitchTo(oldcontext);
				}
				break;

			case PLTSQL_DTYPE_ROW:
				{
					PLTSQL_row *row = (PLTSQL_row *) retvar;

					/* We get here if there are multiple OUT parameters */

					/* Use eval_mcontext for tuple conversion work */
					oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
					tuple = make_tuple_from_row(estate, row, tupdesc);
					if (tuple == NULL)	/* should not happen */
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("wrong record type supplied in RETURN NEXT")));
					tuplestore_puttuple(estate->tuple_store, tuple);
					MemoryContextSwitchTo(oldcontext);
				}
				break;

			default:
				elog(ERROR, "unrecognized dtype: %d", retvar->dtype);
				break;
		}
	}
	else if (stmt->expr)
	{
		Datum		retval;
		bool		isNull;
		Oid			rettype;
		int32		rettypmod;

		retval = exec_eval_expr(estate,
								stmt->expr,
								&isNull,
								&rettype,
								&rettypmod);

		if (estate->retistuple)
		{
			/* Expression should be of RECORD or composite type */
			if (!isNull)
			{
				HeapTupleData tmptup;
				TupleDesc	retvaldesc;
				TupleConversionMap *tupmap;

				if (!type_is_rowtype(rettype))
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("cannot return non-composite value from function returning composite type")));

				/* Use eval_mcontext for tuple conversion work */
				oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
				retvaldesc = deconstruct_composite_datum(retval, &tmptup);
				tuple = &tmptup;
				tupmap = convert_tuples_by_position(retvaldesc, tupdesc,
													gettext_noop("returned record type does not match expected record type"));
				if (tupmap)
					tuple = do_convert_tuple(tuple, tupmap);
				tuplestore_puttuple(estate->tuple_store, tuple);
				ReleaseTupleDesc(retvaldesc);
				MemoryContextSwitchTo(oldcontext);
			}
			else
			{
				/* Composite NULL --- store a row of nulls */
				Datum	   *nulldatums;
				bool	   *nullflags;

				nulldatums = (Datum *)
					eval_mcontext_alloc0(estate, natts * sizeof(Datum));
				nullflags = (bool *)
					eval_mcontext_alloc(estate, natts * sizeof(bool));
				memset(nullflags, true, natts * sizeof(bool));
				tuplestore_putvalues(estate->tuple_store, tupdesc,
									 nulldatums, nullflags);
			}
		}
		else
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, 0);

			/* Simple scalar result */
			if (natts != 1)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("wrong result type supplied in RETURN NEXT")));

			/* coerce type if needed */
			retval = exec_cast_value(estate,
									 retval,
									 &isNull,
									 rettype,
									 rettypmod,
									 attr->atttypid,
									 attr->atttypmod);

			tuplestore_putvalues(estate->tuple_store, tupdesc,
								 &retval, &isNull);
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("RETURN NEXT must have a parameter")));
	}

	exec_eval_cleanup(estate);

	return PLTSQL_RC_OK;
}

/* ----------
 * exec_stmt_return_query		Evaluate a query and add it to the
 *								list of tuples returned by the current
 *								SRF.
 * ----------
 */
static int
exec_stmt_return_query(PLTSQL_execstate *estate,
					   PLTSQL_stmt_return_query *stmt)
{
	Portal		portal;
	uint32		processed = 0;
	TupleConversionMap *tupmap;

	if (!estate->retisset)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot use RETURN QUERY in a non-SETOF function")));

	if (estate->tuple_store == NULL)
		exec_init_tuple_store(estate);

	if (stmt->query != NULL)
	{
		/* static query */
		exec_run_select(estate, stmt->query, 0, &portal);
	}
	else
	{
		/* RETURN QUERY EXECUTE */
		Assert(stmt->dynquery != NULL);
		portal = exec_dynquery_with_params(estate, stmt->dynquery,
										   stmt->params, NULL, 0);
	}

	tupmap = convert_tuples_by_position(portal->tupDesc,
										estate->rettupdesc,
	 gettext_noop("structure of query does not match function result type"));

	while (true)
	{
		int			i;

		SPI_cursor_fetch(portal, true, 50);
		if (SPI_processed == 0)
			break;

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[i];

			if (tupmap)
				tuple = do_convert_tuple(tuple, tupmap);
			tuplestore_puttuple(estate->tuple_store, tuple);
			if (tupmap)
				heap_freetuple(tuple);
			processed++;
		}

		SPI_freetuptable(SPI_tuptable);
	}

	if (tupmap)
		free_conversion_map(tupmap);

	SPI_freetuptable(SPI_tuptable);
	SPI_cursor_close(portal);

	estate->eval_processed = processed;
	exec_set_found(estate, processed != 0);

	return PLTSQL_RC_OK;
}

static void
exec_init_tuple_store(PLTSQL_execstate *estate)
{
	ReturnSetInfo *rsi = estate->rsi;
	MemoryContext oldcxt;
	ResourceOwner oldowner;

	/*
	 * Check caller can handle a set result in the way we want
	 */
	if (!rsi || !IsA(rsi, ReturnSetInfo) ||
		(rsi->allowedModes & SFRM_Materialize) == 0 ||
		rsi->expectedDesc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	/*
	 * Switch to the right memory context and resource owner for storing the
	 * tuplestore for return set. If we're within a subtransaction opened for
	 * an exception-block, for example, we must still create the tuplestore in
	 * the resource owner that was active when this function was entered, and
	 * not in the subtransaction resource owner.
	 */
	oldcxt = MemoryContextSwitchTo(estate->tuple_store_cxt);
	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = estate->tuple_store_owner;

	estate->tuple_store =
		tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random,
							  false, work_mem);

	CurrentResourceOwner = oldowner;
	MemoryContextSwitchTo(oldcxt);

	estate->rettupdesc = rsi->expectedDesc;
}

/* ----------
 * exec_stmt_raise			Build a message and throw it with elog()
 * ----------
 */
static int
exec_stmt_raise(PLTSQL_execstate *estate, PLTSQL_stmt_raise *stmt)
{
	int			err_code = 0;
	char	   *condname = NULL;
	char	   *err_message = NULL;
	char	   *err_detail = NULL;
	char	   *err_hint = NULL;
	ListCell   *lc;

	/* RAISE with no parameters: re-throw current exception */
	if (stmt->condname == NULL && stmt->message == NULL &&
		stmt->options == NIL)
	{
		if (estate->cur_error != NULL)
			ReThrowError(estate->cur_error);
		/* oops, we're not inside a handler */
		ereport(ERROR,
				(errcode(ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER),
				 errmsg("RAISE without parameters cannot be used outside an exception handler")));
	}

	if (stmt->condname)
	{
		err_code = pltsql_recognize_err_condition(stmt->condname, true);
		condname = pstrdup(stmt->condname);
	}

	if (stmt->message)
	{
		StringInfoData ds;
		ListCell   *current_param;
		char	   *cp;

		initStringInfo(&ds);
		current_param = list_head(stmt->params);

		for (cp = stmt->message; *cp; cp++)
		{
			/*
			 * Occurrences of a single % are replaced by the next parameter's
			 * external representation. Double %'s are converted to one %.
			 */
			if (cp[0] == '%')
			{
				Oid			paramtypeid;
				int32		paramtypmod;
				Datum		paramvalue;
				bool		paramisnull;
				char	   *extval;

				if (cp[1] == '%')
				{
					appendStringInfoChar(&ds, '%');
					cp++;
					continue;
				}

				if (current_param == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
						  errmsg("too few parameters specified for RAISE")));

				paramvalue = exec_eval_expr(estate,
									  (PLTSQL_expr *) lfirst(current_param),
											&paramisnull,
											&paramtypeid,
											&paramtypmod);

				if (paramisnull)
					extval = "<NULL>";
				else
					extval = convert_value_to_string(estate,
													 paramvalue,
													 paramtypeid);
				appendStringInfoString(&ds, extval);
				current_param = lnext(current_param);
				exec_eval_cleanup(estate);
			}
			else
				appendStringInfoChar(&ds, cp[0]);
		}

		/*
		 * If more parameters were specified than were required to process the
		 * format string, throw an error
		 */
		if (current_param != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("too many parameters specified for RAISE")));

		err_message = ds.data;
		/* No pfree(ds.data), the pfree(err_message) does it */
	}

	foreach(lc, stmt->options)
	{
		PLTSQL_raise_option *opt = (PLTSQL_raise_option *) lfirst(lc);
		Datum		optionvalue;
		bool		optionisnull;
		Oid			optiontypeid;
		int32		optiontypmod;
		char	   *extval;

		optionvalue = exec_eval_expr(estate, opt->expr,
									 &optionisnull,
									 &optiontypeid,
									 &optiontypmod);
		if (optionisnull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("RAISE statement option cannot be null")));

		extval = convert_value_to_string(estate, optionvalue, optiontypeid);

		switch (opt->opt_type)
		{
			case PLTSQL_RAISEOPTION_ERRCODE:
				if (err_code)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("RAISE option already specified: %s",
									"ERRCODE")));
				err_code = pltsql_recognize_err_condition(extval, true);
				condname = pstrdup(extval);
				break;
			case PLTSQL_RAISEOPTION_MESSAGE:
				if (err_message)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("RAISE option already specified: %s",
									"MESSAGE")));
				err_message = pstrdup(extval);
				break;
			case PLTSQL_RAISEOPTION_DETAIL:
				if (err_detail)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("RAISE option already specified: %s",
									"DETAIL")));
				err_detail = pstrdup(extval);
				break;
			case PLTSQL_RAISEOPTION_HINT:
				if (err_hint)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("RAISE option already specified: %s",
									"HINT")));
				err_hint = pstrdup(extval);
				break;
			default:
				elog(ERROR, "unrecognized raise option: %d", opt->opt_type);
		}

		exec_eval_cleanup(estate);
	}

	/* Default code if nothing specified */
	if (err_code == 0 && stmt->elog_level >= ERROR)
		err_code = ERRCODE_RAISE_EXCEPTION;

	/* Default error message if nothing specified */
	if (err_message == NULL)
	{
		if (condname)
		{
			err_message = condname;
			condname = NULL;
		}
		else
			err_message = pstrdup(unpack_sql_state(err_code));
	}

	/*
	 * Throw the error (may or may not come back)
	 */
	estate->err_text = raise_skip_msg;	/* suppress traceback of raise */

	ereport(stmt->elog_level,
			(err_code ? errcode(err_code) : 0,
			 errmsg_internal("%s", err_message),
			 (err_detail != NULL) ? errdetail_internal("%s", err_detail) : 0,
			 (err_hint != NULL) ? errhint("%s", err_hint) : 0));

	estate->err_text = NULL;	/* un-suppress... */

	if (condname != NULL)
		pfree(condname);
	if (err_message != NULL)
		pfree(err_message);
	if (err_detail != NULL)
		pfree(err_detail);
	if (err_hint != NULL)
		pfree(err_hint);

	return PLTSQL_RC_OK;
}


/* ----------
 * Initialize a mostly empty execution state
 * ----------
 */
static void
pltsql_estate_setup(PLTSQL_execstate *estate,
					 PLTSQL_function *func,
					ReturnSetInfo *rsi,
					EState *simple_eval_estate)
{
	HASHCTL		ctl;

	/* this link will be restored at exit from pltsql_call_handler */
	func->cur_estate = estate;

	estate->func = func;

	estate->retval = (Datum) 0;
	estate->retisnull = true;
	estate->rettype = InvalidOid;

	estate->fn_rettype = func->fn_rettype;
	estate->retistuple = func->fn_retistuple;
	estate->retisset = func->fn_retset;

	estate->readonly_func = func->fn_readonly;

	estate->rettupdesc = NULL;
	estate->exitlabel = NULL;
	estate->cur_error = NULL;

	estate->tuple_store = NULL;
	if (rsi)
	{
		estate->tuple_store_cxt = rsi->econtext->ecxt_per_query_memory;
		estate->tuple_store_owner = CurrentResourceOwner;
	}
	else
	{
		estate->tuple_store_cxt = NULL;
		estate->tuple_store_owner = NULL;
	}
	estate->rsi = rsi;

	estate->found_varno = func->found_varno;
	estate->ndatums = func->ndatums;
	estate->datums = palloc(sizeof(PLTSQL_datum *) * estate->ndatums);
	/* caller is expected to fill the datums array */

	/* set up for use of appropriate simple-expression EState and cast hash */
	if (simple_eval_estate)
	{
		estate->simple_eval_estate = simple_eval_estate;
		/* Private cast hash just lives in function's main context */
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(pltsql_CastHashKey);
		ctl.entrysize = sizeof(pltsql_CastHashEntry);
		ctl.hcxt = CurrentMemoryContext;
		estate->cast_hash = hash_create("PLTSQL private cast cache",
										16, /* start small and extend */
										&ctl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		estate->cast_hash_context = CurrentMemoryContext;
	}
	else
	{
		estate->simple_eval_estate = shared_simple_eval_estate;
		/* Create the session-wide cast-info hash table if we didn't already */
		if (shared_cast_hash == NULL)
		{
			shared_cast_context = AllocSetContextCreate(TopMemoryContext,
														"PLpgSQL cast info",
														ALLOCSET_DEFAULT_SIZES);
			memset(&ctl, 0, sizeof(ctl));
			ctl.keysize = sizeof(pltsql_CastHashKey);
			ctl.entrysize = sizeof(pltsql_CastHashEntry);
			ctl.hcxt = shared_cast_context;
			shared_cast_hash = hash_create("PLTSQL cast cache",
										   16,	/* start small and extend */
										   &ctl,
										   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		}
		estate->cast_hash = shared_cast_hash;
		estate->cast_hash_context = shared_cast_context;
	}

	/*
	 * We start with no stmt_mcontext; one will be created only if needed.
	 * That context will be a direct child of the function's main execution
	 * context.  Additional stmt_mcontexts might be created as children of it.
	 */
	estate->stmt_mcontext = NULL;
	estate->stmt_mcontext_parent = CurrentMemoryContext;
	estate->eval_tuptable = NULL;
	estate->eval_processed = 0;
	estate->eval_lastoid = InvalidOid;
	estate->eval_econtext = NULL;
	estate->cur_expr = NULL;

	estate->err_stmt = NULL;
	estate->err_text = NULL;

	estate->plugin_info = NULL;

	/*
	 * Create an EState and ExprContext for evaluation of simple expressions.
	 */
	pltsql_create_econtext(estate);

	/*
	 * Let the plugin see this function before we initialize any local
	 * PL/TSQL variables - note that we also give the plugin a few function
	 * pointers so it can call back into PL/TSQL for doing things like
	 * variable assignments and stack traces
	 */
	if (*plugin_ptr)
	{
		(*plugin_ptr)->error_callback = pltsql_exec_error_callback;
		(*plugin_ptr)->assign_expr = exec_assign_expr;

		if ((*plugin_ptr)->func_setup)
			((*plugin_ptr)->func_setup) (estate, func);
	}
}

/* ----------
 * Release temporary memory used by expression/subselect evaluation
 *
 * NB: the result of the evaluation is no longer valid after this is done,
 * unless it is a pass-by-value datatype.
 *
 * NB: if you change this code, see also the hacks in exec_assign_value's
 * PLTSQL_DTYPE_ARRAYELEM case.
 * ----------
 */
static void
exec_eval_cleanup(PLTSQL_execstate *estate)
{
	/* Clear result of a full SPI_execute */
	if (estate->eval_tuptable != NULL)
		SPI_freetuptable(estate->eval_tuptable);
	estate->eval_tuptable = NULL;

	/* Clear result of exec_eval_simple_expr (but keep the econtext) */
	if (estate->eval_econtext != NULL)
		ResetExprContext(estate->eval_econtext);
}


/* ----------
 * Generate a prepared plan
 * ----------
 */
static void
exec_prepare_plan(PLTSQL_execstate *estate,
				  PLTSQL_expr *expr, int cursorOptions,
				  bool keepplan)
{
	SPIPlanPtr	plan;

	/*
	 * The grammar can't conveniently set expr->func while building the parse
	 * tree, so make sure it's set before parser hooks need it.
	 */
	expr->func = estate->func;

	/*
	 * Generate and save the plan
	 */
	plan = SPI_prepare_params(expr->query,
							  (ParserSetupHook) pltsql_parser_setup,
							  (void *) expr,
							  cursorOptions);
	if (plan == NULL)
		elog(ERROR, "SPI_prepare_params failed for \"%s\": %s",
			 expr->query, SPI_result_code_string(SPI_result));
	if (keepplan)
		SPI_keepplan(plan);
	expr->plan = plan;

	/* Check to see if it's a simple expression */
	exec_simple_check_plan(estate, expr);

	/*
	 * Mark expression as not using a read-write param.  exec_assign_value has
	 * to take steps to override this if appropriate; that seems cleaner than
	 * adding parameters to all other callers.
	 */
	expr->rwparam = -1;
}

/* ----------
 * exec_stmt_execsql			Execute an SQL statement (possibly with INTO).
 * ----------
 */
static int
exec_stmt_execsql(PLTSQL_execstate *estate,
				  PLTSQL_stmt_execsql *stmt)
{
	ParamListInfo paramLI;
	long		tcount;
	int			rc;
	PLTSQL_expr *expr = stmt->sqlstmt;

	/*
	 * On the first call for this statement generate the plan, and detect
	 * whether the statement is INSERT/UPDATE/DELETE
	 */
	if (expr->plan == NULL)
	{
		ListCell   *l;

		exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK, true);
		stmt->mod_stmt = false;
		foreach(l, expr->plan->plancache_list)
		{
			CachedPlanSource *plansource = (CachedPlanSource *) lfirst(l);
			ListCell   *l2;

			foreach(l2, plansource->query_list)
			{
				Query  *q = (Query *) lfirst(l2);

				Assert(IsA(q, Query));
				if (q->canSetTag)
				{
					if (q->commandType == CMD_INSERT ||
						q->commandType == CMD_UPDATE ||
						q->commandType == CMD_DELETE)
						stmt->mod_stmt = true;
				}
			}
		}
	}

	/*
	 * Set up ParamListInfo (hook function and possibly data values)
	 */
	paramLI = setup_param_list(estate, expr);

	/*
	 * If we have INTO, then we only need one row back ... but if we have INTO
	 * STRICT, ask for two rows, so that we can verify the statement returns
	 * only one.  INSERT/UPDATE/DELETE are always treated strictly. Without
	 * INTO, just run the statement to completion (tcount = 0).
	 *
	 * We could just ask for two rows always when using INTO, but there are
	 * some cases where demanding the extra row costs significant time, eg by
	 * forcing completion of a sequential scan.  So don't do it unless we need
	 * to enforce strictness.
	 */
	if (stmt->into)
	{
		if (stmt->strict || stmt->mod_stmt)
			tcount = 2;
		else
			tcount = 1;
	}
	else
		tcount = 0;

	/*
	 * Execute the plan
	 */
	rc = SPI_execute_plan_with_paramlist(expr->plan, paramLI,
										 estate->readonly_func, tcount);

	/*
	 * Check for error, and set FOUND if appropriate (for historical reasons
	 * we set FOUND only for certain query types).	Also Assert that we
	 * identified the statement type the same as SPI did.
	 */
	switch (rc)
	{
		case SPI_OK_SELECT:
			Assert(!stmt->mod_stmt);
			exec_set_found(estate, (SPI_processed != 0));
			break;

		case SPI_OK_INSERT:
		case SPI_OK_UPDATE:
		case SPI_OK_DELETE:
		case SPI_OK_INSERT_RETURNING:
		case SPI_OK_UPDATE_RETURNING:
		case SPI_OK_DELETE_RETURNING:
			Assert(stmt->mod_stmt);
			exec_set_found(estate, (SPI_processed != 0));
			break;

		case SPI_OK_SELINTO:
		case SPI_OK_UTILITY:
			Assert(!stmt->mod_stmt);
			break;

		case SPI_OK_REWRITTEN:
			Assert(!stmt->mod_stmt);

			/*
			 * The command was rewritten into another kind of command. It's
			 * not clear what FOUND would mean in that case (and SPI doesn't
			 * return the row count either), so just set it to false.
			 */
			exec_set_found(estate, false);
			break;

			/* Some SPI errors deserve specific error messages */
		case SPI_ERROR_COPY:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot COPY to/from client in PL/TSQL")));
		case SPI_ERROR_TRANSACTION:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot begin/end transactions in PL/TSQL"),
			errhint("Use a BEGIN block with an EXCEPTION clause instead.")));

		default:
			elog(ERROR, "SPI_execute_plan_with_paramlist failed executing query \"%s\": %s",
				 expr->query, SPI_result_code_string(rc));
	}

	/* All variants should save result info for GET DIAGNOSTICS */
	estate->eval_processed = SPI_processed;
	estate->eval_lastoid = SPI_lastoid;

	/* Process INTO if present */
	if (stmt->into)
	{
		SPITupleTable *tuptab = SPI_tuptable;
		uint32		n = SPI_processed;
		PLTSQL_variable *target;

		/* If the statement did not return a tuple table, complain */
		if (tuptab == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("INTO used with a command that cannot return data")));

		/* Fetch target's datum entry */
		target = (PLTSQL_variable *) estate->datums[stmt->target->dno];

		/*
		 * If SELECT ... INTO specified STRICT, and the query didn't find
		 * exactly one row, throw an error.  If STRICT was not specified, then
		 * allow the query to find any number of rows.
		 */
		if (n == 0)
		{
			if (stmt->strict)
				ereport(ERROR,
						(errcode(ERRCODE_NO_DATA_FOUND),
						 errmsg("query returned no rows")));
			/* set the target to NULL(s) */
			exec_move_row(estate, target, NULL, tuptab->tupdesc);
		}
		else
		{
			if (n > 1 && (stmt->strict || stmt->mod_stmt))
				ereport(ERROR,
						(errcode(ERRCODE_TOO_MANY_ROWS),
						 errmsg("query returned more than one row")));
			/* Put the first result row into the target */
			exec_move_row(estate, target, tuptab->vals[0], tuptab->tupdesc);
		}

		/* Clean up */
		exec_eval_cleanup(estate);
		SPI_freetuptable(SPI_tuptable);
	}
	else
	{
		/* If the statement returned a tuple table, complain */
		if (SPI_tuptable != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("query has no destination for result data"),
					 (rc == SPI_OK_SELECT) ? errhint("If you want to discard the results of a SELECT, use PERFORM instead.") : 0));
	}

	if (paramLI)
		pfree(paramLI);

	return PLTSQL_RC_OK;
}


/* ----------
 * exec_stmt_dynexecute			Execute a dynamic SQL query
 *					(possibly with INTO).
 * ----------
 */
static int
exec_stmt_dynexecute(PLTSQL_execstate *estate,
					 PLTSQL_stmt_dynexecute *stmt)
{
	Datum		query;
	bool		isnull = false;
	Oid			restype;
	int32		restypmod;
	char	   *querystr;
	int			exec_res;

	/*
	 * First we evaluate the string expression after the EXECUTE keyword. Its
	 * result is the querystring we have to execute.
	 */
	query = exec_eval_expr(estate, stmt->query, &isnull, &restype, &restypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("query string argument of EXECUTE is null")));

	/* Get the C-String representation */
	querystr = convert_value_to_string(estate, query, restype);

	/* carry out any local temporary table transformations that may be required */
	querystr = transform_tsql_temp_tables(querystr);

	/* the transformation yielded a new querystr copy so we can clean up */
	exec_eval_cleanup(estate);

	/*
	 * Execute the query without preparing a saved plan.
	 */
	if (stmt->params)
	{
		PreparedParamsData *ppd;

		ppd = exec_eval_using_params(estate, stmt->params);
		exec_res = SPI_execute_with_args(querystr,
										 ppd->nargs, ppd->types,
										 ppd->values, ppd->nulls,
										 estate->readonly_func, 0);
		free_params_data(ppd);
	}
	else
		exec_res = SPI_execute(querystr, estate->readonly_func, 0);

	switch (exec_res)
	{
		case SPI_OK_SELECT:
		case SPI_OK_INSERT:
		case SPI_OK_UPDATE:
		case SPI_OK_DELETE:
		case SPI_OK_INSERT_RETURNING:
		case SPI_OK_UPDATE_RETURNING:
		case SPI_OK_DELETE_RETURNING:
		case SPI_OK_UTILITY:
		case SPI_OK_REWRITTEN:
			break;

		case 0:

			/*
			 * Also allow a zero return, which implies the querystring
			 * contained no commands.
			 */
			break;

		case SPI_OK_SELINTO:

			/*
			 * We want to disallow SELECT INTO for now, because its behavior
			 * is not consistent with SELECT INTO in a normal pltsql context.
			 * (We need to reimplement EXECUTE to parse the string as a
			 * plpgsql command, not just feed it to SPI_execute.)  This is not
			 * a functional limitation because CREATE TABLE AS is allowed.
			 */
			ereport(ERROR,
			        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			         errmsg("EXECUTE of SELECT ... INTO is not implemented"),
			         errhint("You might want to use EXECUTE ... INTO or EXECUTE CREATE TABLE ... AS instead.")));
			break;

			/* Some SPI errors deserve specific error messages */
		case SPI_ERROR_COPY:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot COPY to/from client in PL/TSQL")));
		case SPI_ERROR_TRANSACTION:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot begin/end transactions in PL/TSQL"),
			errhint("Use a BEGIN block with an EXCEPTION clause instead.")));

		default:
			elog(ERROR, "SPI_execute failed executing query \"%s\": %s",
				 querystr, SPI_result_code_string(exec_res));
			break;
	}

	/* Save result info for GET DIAGNOSTICS */
	estate->eval_processed = SPI_processed;
	estate->eval_lastoid = SPI_lastoid;

	/* Process INTO if present */
	if (stmt->into)
	{
		SPITupleTable *tuptab = SPI_tuptable;
		uint32		n = SPI_processed;
		PLTSQL_variable *target;

		/* If the statement did not return a tuple table, complain */
		if (tuptab == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("INTO used with a command that cannot return data")));

		/* Fetch target's datum entry */
		target = (PLTSQL_variable *) estate->datums[stmt->target->dno];

		/*
		 * If SELECT ... INTO specified STRICT, and the query didn't find
		 * exactly one row, throw an error.  If STRICT was not specified, then
		 * allow the query to find any number of rows.
		 */
		if (n == 0)
		{
			if (stmt->strict)
				ereport(ERROR,
						(errcode(ERRCODE_NO_DATA_FOUND),
						 errmsg("query returned no rows")));
			/* set the target to NULL(s) */
			exec_move_row(estate, target, NULL, tuptab->tupdesc);
		}
		else
		{
			if (n > 1 && stmt->strict)
				ereport(ERROR,
						(errcode(ERRCODE_TOO_MANY_ROWS),
						 errmsg("query returned more than one row")));
			/* Put the first result row into the target */
			exec_move_row(estate, target, tuptab->vals[0], tuptab->tupdesc);
 		}
		/* clean up after exec_move_row() */
		exec_eval_cleanup(estate);
	}
	else
	{
		/*
		 * It might be a good idea to raise an error if the query returned
		 * tuples that are being ignored, but historically we have not done
		 * that.
		 */
	}

	/* Release any result from SPI_execute, as well as the querystring */
	SPI_freetuptable(SPI_tuptable);
	pfree(querystr);

	return PLTSQL_RC_OK;
}


/* ----------
 * exec_stmt_dynfors			Execute a dynamic query, assign each
 *					tuple to a record or row and
 *					execute a group of statements
 *					for it.
 * ----------
 */
static int
exec_stmt_dynfors(PLTSQL_execstate *estate, PLTSQL_stmt_dynfors *stmt)
{
	Portal		portal;
	int			rc;

	portal = exec_dynquery_with_params(estate, stmt->query, stmt->params,
									   NULL, 0);

	/*
	 * Execute the loop
	 */
	rc = exec_for_query(estate, (PLTSQL_stmt_forq *) stmt, portal, true);

	/*
	 * Close the implicit cursor
	 */
	SPI_cursor_close(portal);

	return rc;
}


/* ----------
 * exec_stmt_open			Execute an OPEN cursor statement
 * ----------
 */
static int
exec_stmt_open(PLTSQL_execstate *estate, PLTSQL_stmt_open *stmt)
{
	PLTSQL_var *curvar;
	char	   *curname = NULL;
	PLTSQL_expr *query;
	Portal		portal;
	ParamListInfo paramLI;

	/* ----------
	 * Get the cursor variable and if it has an assigned name, check
	 * that it's not in use currently.
	 * ----------
	 */
	curvar = (PLTSQL_var *) (estate->datums[stmt->curvar]);
	if (!curvar->isnull)
	{
		curname = TextDatumGetCString(curvar->value);
		if (SPI_cursor_find(curname) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_CURSOR),
					 errmsg("cursor \"%s\" already in use", curname)));
	}

	/* ----------
	 * Process the OPEN according to it's type.
	 * ----------
	 */
	if (stmt->query != NULL)
	{
		/* ----------
		 * This is an OPEN refcursor FOR SELECT ...
		 *
		 * We just make sure the query is planned. The real work is
		 * done downstairs.
		 * ----------
		 */
		query = stmt->query;
		if (query->plan == NULL)
			exec_prepare_plan(estate, query, stmt->cursor_options, true);
	}
	else if (stmt->dynquery != NULL)
	{
		/* ----------
		 * This is an OPEN refcursor FOR EXECUTE ...
		 * ----------
		 */
		portal = exec_dynquery_with_params(estate,
										   stmt->dynquery,
										   stmt->params,
										   curname,
										   stmt->cursor_options);

		/*
		 * If cursor variable was NULL, store the generated portal name in it
		 */
		if (curname == NULL)
			assign_text_var(curvar, portal->name);

		return PLTSQL_RC_OK;
	}
	else
	{
		/* ----------
		 * This is an OPEN cursor
		 *
		 * Note: parser should already have checked that statement supplies
		 * args iff cursor needs them, but we check again to be safe.
		 * ----------
		 */
		if (stmt->argquery != NULL)
		{
			/* ----------
			 * OPEN CURSOR with args.  We fake a SELECT ... INTO ...
			 * statement to evaluate the args and put 'em into the
			 * internal row.
			 * ----------
			 */
			PLTSQL_stmt_execsql set_args;

			if (curvar->cursor_explicit_argrow < 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("arguments given for cursor without arguments")));

			memset(&set_args, 0, sizeof(set_args));
			set_args.cmd_type = PLTSQL_STMT_EXECSQL;
			set_args.lineno = stmt->lineno;
			set_args.sqlstmt = stmt->argquery;
			set_args.into = true;
			/* XXX historically this has not been STRICT */
			set_args.target = (PLTSQL_variable *)
				(estate->datums[curvar->cursor_explicit_argrow]);

			if (exec_stmt_execsql(estate, &set_args) != PLTSQL_RC_OK)
				elog(ERROR, "open cursor failed during argument processing");
		}
		else
		{
			if (curvar->cursor_explicit_argrow >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("arguments required for cursor")));
		}

		query = curvar->cursor_explicit_expr;
		if (query->plan == NULL)
			exec_prepare_plan(estate, query, curvar->cursor_options, true);
	}

	/*
	 * Set up ParamListInfo (hook function and possibly data values)
	 */
	paramLI = setup_param_list(estate, query);

	/*
	 * Open the cursor
	 */
	portal = SPI_cursor_open_with_paramlist(curname, query->plan,
											paramLI,
											estate->readonly_func);
	if (portal == NULL)
		elog(ERROR, "could not open cursor: %s",
			 SPI_result_code_string(SPI_result));

	/*
	 * If cursor variable was NULL, store the generated portal name in it
	 */
	if (curname == NULL)
		assign_text_var(curvar, portal->name);

	if (curname)
		pfree(curname);
	if (paramLI)
		pfree(paramLI);

	return PLTSQL_RC_OK;
}


/* ----------
 * exec_stmt_fetch			Fetch from a cursor into a target, or just
 *							move the current position of the cursor
 * ----------
 */
static int
exec_stmt_fetch(PLTSQL_execstate *estate, PLTSQL_stmt_fetch *stmt)
{
	PLTSQL_var *curvar = NULL;
	long		how_many = stmt->how_many;
	SPITupleTable *tuptab;
	Portal		portal;
	char	   *curname;
	uint32		n;

	/* ----------
	 * Get the portal of the cursor by name
	 * ----------
	 */
	curvar = (PLTSQL_var *) (estate->datums[stmt->curvar]);
	if (curvar->isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cursor variable \"%s\" is null", curvar->refname)));
	curname = TextDatumGetCString(curvar->value);

	portal = SPI_cursor_find(curname);
	if (portal == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("cursor \"%s\" does not exist", curname)));
	pfree(curname);

	/* Calculate position for FETCH_RELATIVE or FETCH_ABSOLUTE */
	if (stmt->expr)
	{
		bool		isnull;

		/* XXX should be doing this in LONG not INT width */
		how_many = exec_eval_integer(estate, stmt->expr, &isnull);

		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("relative or absolute cursor position is null")));

		exec_eval_cleanup(estate);
	}

	if (!stmt->is_move)
	{
		PLTSQL_variable *target;

		/* ----------
		 * Fetch 1 tuple from the cursor
		 * ----------
		 */
		SPI_scroll_cursor_fetch(portal, stmt->direction, how_many);
		tuptab = SPI_tuptable;
		n = SPI_processed;

		/* ----------
		 * Set the target appropriately.
		 * ----------
		 */
		target = (PLTSQL_variable *) estate->datums[stmt->target->dno];
		if (n == 0)
			exec_move_row(estate, target, NULL, tuptab->tupdesc);
		else
			exec_move_row(estate, target, tuptab->vals[0], tuptab->tupdesc);

		exec_eval_cleanup(estate);
		SPI_freetuptable(tuptab);
	}
	else
	{
		/* Move the cursor */
		SPI_scroll_cursor_move(portal, stmt->direction, how_many);
		n = SPI_processed;
	}

	/* Set the ROW_COUNT and the global FOUND variable appropriately. */
	estate->eval_processed = n;
	exec_set_found(estate, n != 0);

	return PLTSQL_RC_OK;
}

/* ----------
 * exec_stmt_close			Close a cursor
 * ----------
 */
static int
exec_stmt_close(PLTSQL_execstate *estate, PLTSQL_stmt_close *stmt)
{
	PLTSQL_var *curvar = NULL;
	Portal		portal;
	char	   *curname;

	/* ----------
	 * Get the portal of the cursor by name
	 * ----------
	 */
	curvar = (PLTSQL_var *) (estate->datums[stmt->curvar]);
	if (curvar->isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cursor variable \"%s\" is null", curvar->refname)));
	curname = TextDatumGetCString(curvar->value);

	portal = SPI_cursor_find(curname);
	if (portal == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("cursor \"%s\" does not exist", curname)));
	pfree(curname);

	/* ----------
	 * And close it.
	 * ----------
	 */
	SPI_cursor_close(portal);

	return PLTSQL_RC_OK;
}


/* ----------
 * exec_assign_expr			Put an expression's result into a variable.
 * ----------
 */
static void
exec_assign_expr(PLTSQL_execstate *estate, PLTSQL_datum *target,
				 PLTSQL_expr *expr)
{
	Datum		value;
	Oid			valtype;
	int32		valtypmod;
	bool		isnull = false;

	value = exec_eval_expr(estate, expr, &isnull, &valtype, &valtypmod);
	exec_assign_value(estate, target, value, isnull, valtype, valtypmod);
	exec_eval_cleanup(estate);
}


/* ----------
 * exec_assign_c_string		Put a C string into a text variable.
 *
 * We take a NULL pointer as signifying empty string, not SQL null.
 * ----------
 */
static void
exec_assign_c_string(PLTSQL_execstate *estate, PLTSQL_datum *target,
					 const char *str)
{
	text	   *value;

	if (str != NULL)
		value = cstring_to_text(str);
	else
		value = cstring_to_text("");
	exec_assign_value(estate, target, PointerGetDatum(value),
					  false, TEXTOID, -1);
	pfree(value);
}

/* ----------
 * exec_assign_value			Put a value into a target datum
 *
 * Note: in some code paths, this will leak memory in the eval_mcontext;
 * we assume that will be cleaned up later by exec_eval_cleanup.  We cannot
 * call exec_eval_cleanup here for fear of destroying the input Datum value.
 * ----------
 */
static void
exec_assign_value(PLTSQL_execstate *estate,
				  PLTSQL_datum *target,
				  Datum value, bool isNull,
				  Oid valtype, int32 valtypmod)
{
	switch (target->dtype)
	{
		case PLTSQL_DTYPE_VAR:
#ifdef PLTSQL_DTYPE_PROMISE
		case PLTSQL_DTYPE_PROMISE:
#endif
			{
				/*
				 * Target is a variable
				 */
				PLTSQL_var *var = (PLTSQL_var *) target;
				Datum		newvalue;

				newvalue = exec_cast_value(estate,
										   value,
										   &isNull,
										   valtype,
										   valtypmod,
										   var->datatype->typoid,
										   var->datatype->atttypmod);

				if (isNull && var->notnull)
					ereport(ERROR,
							(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							 errmsg("null value cannot be assigned to variable \"%s\" declared NOT NULL",
									var->refname)));

				/*
				 * If type is by-reference, copy the new value (which is
				 * probably in the eval_mcontext) into the procedure's main
				 * memory context.  But if it's a read/write reference to an
				 * expanded object, no physical copy needs to happen; at most
				 * we need to reparent the object's memory context.
				 *
				 * If it's an array, we force the value to be stored in R/W
				 * expanded form.  This wins if the function later does, say,
				 * a lot of array subscripting operations on the variable, and
				 * otherwise might lose.  We might need to use a different
				 * heuristic, but it's too soon to tell.  Also, are there
				 * cases where it'd be useful to force non-array values into
				 * expanded form?
				 */
				if (!var->datatype->typbyval && !isNull)
				{
					if (var->datatype->typisarray &&
						!VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(newvalue)))
					{
						/* array and not already R/W, so apply expand_array */
						newvalue = expand_array(newvalue,
												estate->datum_context,
												NULL);
					}
					else
					{
						/* else transfer value if R/W, else just datumCopy */
						newvalue = datumTransfer(newvalue,
												 false,
												 var->datatype->typlen);
					}
				}

				/*
				 * Now free the old value, if any, and assign the new one. But
				 * skip the assignment if old and new values are the same.
				 * Note that for expanded objects, this test is necessary and
				 * cannot reliably be made any earlier; we have to be looking
				 * at the object's standard R/W pointer to be sure pointer
				 * equality is meaningful.
				 *
				 * Also, if it's a promise variable, we should disarm the
				 * promise in any case --- otherwise, assigning null to an
				 * armed promise variable would fail to disarm the promise.
				 */
				if (var->value != newvalue || var->isnull || isNull)
					assign_simple_var(estate, var, newvalue, isNull,
									  (!var->datatype->typbyval && !isNull));
#ifdef PLTSQL_PROMISE_NONE
				else
					var->promise = PLTSQL_PROMISE_NONE;
#endif
				break;
			}

		case PLTSQL_DTYPE_ROW:
			{
				/*
				 * Target is a row variable
				 */
				PLTSQL_row *row = (PLTSQL_row *) target;

				if (isNull)
				{
					/* If source is null, just assign nulls to the row */
					exec_move_row(estate, (PLTSQL_variable *) row,
								  NULL, NULL);
				}
				else
				{
					/* Source must be of RECORD or composite type */
					if (!type_is_rowtype(valtype))
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("cannot assign non-composite value to a row variable")));
					exec_move_row_from_datum(estate, (PLTSQL_variable *) row,
											 value);
				}
				break;
			}

		case PLTSQL_DTYPE_REC:
			{
				/*
				 * Target is a record variable
				 */
				PLTSQL_rec *rec = (PLTSQL_rec *) target;

				if (isNull)
				{
					if (rec->notnull)
						ereport(ERROR,
								(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								 errmsg("null value cannot be assigned to variable \"%s\" declared NOT NULL",
										rec->refname)));

					/* Set variable to a simple NULL */
					exec_move_row(estate, (PLTSQL_variable *) rec,
								  NULL, NULL);
				}
				else
				{
					/* Source must be of RECORD or composite type */
					if (!type_is_rowtype(valtype))
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("cannot assign non-composite value to a record variable")));
					exec_move_row_from_datum(estate, (PLTSQL_variable *) rec,
											 value);
				}
				break;
			}

		case PLTSQL_DTYPE_RECFIELD:
			{
				/*
				 * Target is a field of a record
				 */
				PLTSQL_recfield *recfield = (PLTSQL_recfield *) target;
				PLTSQL_rec *rec;
				ExpandedRecordHeader *erh;

				rec = (PLTSQL_rec *) (estate->datums[recfield->recparentno]);
				erh = rec->erh;

				/*
				 * If record variable is NULL, instantiate it if it has a
				 * named composite type, else complain.  (This won't change
				 * the logical state of the record, but if we successfully
				 * assign below, the unassigned fields will all become NULLs.)
				 */
				if (erh == NULL)
				{
					instantiate_empty_record_variable(estate, rec);
					erh = rec->erh;
				}

				/*
				 * Look up the field's properties if we have not already, or
				 * if the tuple descriptor ID changed since last time.
				 */
				if (unlikely(recfield->rectupledescid != erh->er_tupdesc_id))
				{
					if (!expanded_record_lookup_field(erh,
													  recfield->fieldname,
													  &recfield->finfo))
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("record \"%s\" has no field \"%s\"",
										rec->refname, recfield->fieldname)));
					recfield->rectupledescid = erh->er_tupdesc_id;
				}

				/* We don't support assignments to system columns. */
				if (recfield->finfo.fnumber <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot assign to system column \"%s\"",
									recfield->fieldname)));

				/* Cast the new value to the right type, if needed. */
				value = exec_cast_value(estate,
										value,
										&isNull,
										valtype,
										valtypmod,
										recfield->finfo.ftypeid,
										recfield->finfo.ftypmod);

				/* And assign it. */
				expanded_record_set_field(erh, recfield->finfo.fnumber,
										  value, isNull, !estate->atomic);
				break;
			}

		case PLTSQL_DTYPE_ARRAYELEM:
			{
				/*
				 * Target is an element of an array
				 */
				PLTSQL_arrayelem *arrayelem;
				int			nsubscripts;
				int			i;
				PLTSQL_expr *subscripts[MAXDIM];
				int			subscriptvals[MAXDIM];
				Datum		oldarraydatum,
							newarraydatum,
							coerced_value;
				bool		oldarrayisnull;
				Oid			parenttypoid;
				int32		parenttypmod;
				SPITupleTable *save_eval_tuptable;
				MemoryContext oldcontext;

				/*
				 * We need to do subscript evaluation, which might require
				 * evaluating general expressions; and the caller might have
				 * done that too in order to prepare the input Datum.  We have
				 * to save and restore the caller's SPI_execute result, if
				 * any.
				 */
				save_eval_tuptable = estate->eval_tuptable;
				estate->eval_tuptable = NULL;

				/*
				 * To handle constructs like x[1][2] := something, we have to
				 * be prepared to deal with a chain of arrayelem datums. Chase
				 * back to find the base array datum, and save the subscript
				 * expressions as we go.  (We are scanning right to left here,
				 * but want to evaluate the subscripts left-to-right to
				 * minimize surprises.)  Note that arrayelem is left pointing
				 * to the leftmost arrayelem datum, where we will cache the
				 * array element type data.
				 */
				nsubscripts = 0;
				do
				{
					arrayelem = (PLTSQL_arrayelem *) target;
					if (nsubscripts >= MAXDIM)
						ereport(ERROR,
								(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
								 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
										nsubscripts + 1, MAXDIM)));
					subscripts[nsubscripts++] = arrayelem->subscript;
					target = estate->datums[arrayelem->arrayparentno];
				} while (target->dtype == PLTSQL_DTYPE_ARRAYELEM);

				/* Fetch current value of array datum */
				exec_eval_datum(estate, target,
								&parenttypoid, &parenttypmod,
								&oldarraydatum, &oldarrayisnull);

				/* Update cached type data if necessary */
				if (arrayelem->parenttypoid != parenttypoid ||
					arrayelem->parenttypmod != parenttypmod)
				{
					Oid			arraytypoid;
					int32		arraytypmod = parenttypmod;
					int16		arraytyplen;
					Oid			elemtypoid;
					int16		elemtyplen;
					bool		elemtypbyval;
					char		elemtypalign;

					/* If target is domain over array, reduce to base type */
					arraytypoid = getBaseTypeAndTypmod(parenttypoid,
													   &arraytypmod);

					/* ... and identify the element type */
					elemtypoid = get_element_type(arraytypoid);
					if (!OidIsValid(elemtypoid))
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("subscripted object is not an array")));

					/* Collect needed data about the types */
					arraytyplen = get_typlen(arraytypoid);

					get_typlenbyvalalign(elemtypoid,
										 &elemtyplen,
										 &elemtypbyval,
										 &elemtypalign);

					/* Now safe to update the cached data */
					arrayelem->parenttypoid = parenttypoid;
					arrayelem->parenttypmod = parenttypmod;
					arrayelem->arraytypoid = arraytypoid;
					arrayelem->arraytypmod = arraytypmod;
					arrayelem->arraytyplen = arraytyplen;
					arrayelem->elemtypoid = elemtypoid;
					arrayelem->elemtyplen = elemtyplen;
					arrayelem->elemtypbyval = elemtypbyval;
					arrayelem->elemtypalign = elemtypalign;
				}

				/*
				 * Evaluate the subscripts, switch into left-to-right order.
				 * Like the expression built by ExecInitArrayRef(), complain
				 * if any subscript is null.
				 */
				for (i = 0; i < nsubscripts; i++)
				{
					bool		subisnull;

					subscriptvals[i] =
						exec_eval_integer(estate,
										  subscripts[nsubscripts - 1 - i],
										  &subisnull);
					if (subisnull)
						ereport(ERROR,
								(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								 errmsg("array subscript in assignment must not be null")));

					/*
					 * Clean up in case the subscript expression wasn't
					 * simple. We can't do exec_eval_cleanup, but we can do
					 * this much (which is safe because the integer subscript
					 * value is surely pass-by-value), and we must do it in
					 * case the next subscript expression isn't simple either.
					 */
					if (estate->eval_tuptable != NULL)
						SPI_freetuptable(estate->eval_tuptable);
					estate->eval_tuptable = NULL;
				}

				/* Now we can restore caller's SPI_execute result if any. */
				Assert(estate->eval_tuptable == NULL);
				estate->eval_tuptable = save_eval_tuptable;

				/* Coerce source value to match array element type. */
				coerced_value = exec_cast_value(estate,
												value,
												&isNull,
												valtype,
												valtypmod,
												arrayelem->elemtypoid,
												arrayelem->arraytypmod);

				/*
				 * If the original array is null, cons up an empty array so
				 * that the assignment can proceed; we'll end with a
				 * one-element array containing just the assigned-to
				 * subscript.  This only works for varlena arrays, though; for
				 * fixed-length array types we skip the assignment.  We can't
				 * support assignment of a null entry into a fixed-length
				 * array, either, so that's a no-op too.  This is all ugly but
				 * corresponds to the current behavior of execExpr*.c.
				 */
				if (arrayelem->arraytyplen > 0 &&	/* fixed-length array? */
					(oldarrayisnull || isNull))
					return;

				/* empty array, if any, and newarraydatum are short-lived */
				oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

				if (oldarrayisnull)
					oldarraydatum = PointerGetDatum(construct_empty_array(arrayelem->elemtypoid));

				/*
				 * Build the modified array value.
				 */
				newarraydatum = array_set_element(oldarraydatum,
												  nsubscripts,
												  subscriptvals,
												  coerced_value,
												  isNull,
												  arrayelem->arraytyplen,
												  arrayelem->elemtyplen,
												  arrayelem->elemtypbyval,
												  arrayelem->elemtypalign);

				MemoryContextSwitchTo(oldcontext);

				/*
				 * Assign the new array to the base variable.  It's never NULL
				 * at this point.  Note that if the target is a domain,
				 * coercing the base array type back up to the domain will
				 * happen within exec_assign_value.
				 */
				exec_assign_value(estate, target,
								  newarraydatum,
								  false,
								  arrayelem->arraytypoid,
								  arrayelem->arraytypmod);
				break;
			}

		default:
			elog(ERROR, "unrecognized dtype: %d", target->dtype);
	}
}
/*
 * exec_eval_datum				Get current value of a PLTSQL_datum
 *
 * The type oid, typmod, value in Datum format, and null flag are returned.
 *
 * At present this doesn't handle PLTSQL_expr or PLTSQL_arrayelem datums.
 *
 * NOTE: caller must not modify the returned value, since it points right
 * at the stored value in the case of pass-by-reference datatypes.	In some
 * cases we have to palloc a return value, and in such cases we put it into
 * the estate's short-term memory context.
 */
static void
exec_eval_datum(PLTSQL_execstate *estate,
				PLTSQL_datum *datum,
				Oid *typeid,
				int32 *typetypmod,
				Datum *value,
				bool *isnull)
{
	MemoryContext oldcontext;

	switch (datum->dtype)
	{
		case PLTSQL_DTYPE_VAR:
			{
				PLTSQL_var *var = (PLTSQL_var *) datum;

				*typeid = var->datatype->typoid;
				*typetypmod = var->datatype->atttypmod;
				*value = var->value;
				*isnull = var->isnull;
				break;
			}

		case PLTSQL_DTYPE_ROW:
			{
				PLTSQL_row *row = (PLTSQL_row *) datum;
				HeapTuple	tup;

				if (!row->rowtupdesc)	/* should not happen */
					elog(ERROR, "row variable has no tupdesc");
				/* Make sure we have a valid type/typmod setting */
				BlessTupleDesc(row->rowtupdesc);
				oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
				tup = make_tuple_from_row(estate, row, row->rowtupdesc);
				if (tup == NULL)	/* should not happen */
					elog(ERROR, "row not compatible with its own tupdesc");
				MemoryContextSwitchTo(oldcontext);
				*typeid = row->rowtupdesc->tdtypeid;
				*typetypmod = row->rowtupdesc->tdtypmod;
				*value = HeapTupleGetDatum(tup);
				*isnull = false;
				break;
			}

		case PLTSQL_DTYPE_REC:
			{
				PLTSQL_rec *rec = (PLTSQL_rec *) datum;
				HeapTupleData worktup;

				if (!HeapTupleIsValid(rec->tup))
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("record \"%s\" is not assigned yet",
								  rec->refname),
						   errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
				Assert(rec->tupdesc != NULL);
				/* Make sure we have a valid type/typmod setting */
				BlessTupleDesc(rec->tupdesc);

				/*
				 * In a trigger, the NEW and OLD parameters are likely to be
				 * on-disk tuples that don't have the desired Datum fields.
				 * Copy the tuple body and insert the right values.
				 */
				oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
				heap_copytuple_with_tuple(rec->tup, &worktup);
				HeapTupleHeaderSetDatumLength(worktup.t_data, worktup.t_len);
				HeapTupleHeaderSetTypeId(worktup.t_data, rec->tupdesc->tdtypeid);
				HeapTupleHeaderSetTypMod(worktup.t_data, rec->tupdesc->tdtypmod);
				MemoryContextSwitchTo(oldcontext);
				*typeid = rec->tupdesc->tdtypeid;
				*typetypmod = rec->tupdesc->tdtypmod;
				*value = HeapTupleGetDatum(&worktup);
				*isnull = false;
				break;
			}

		case PLTSQL_DTYPE_RECFIELD:
			{
				PLTSQL_recfield *recfield = (PLTSQL_recfield *) datum;
				PLTSQL_rec *rec;
				ExpandedRecordHeader *erh;

				rec = (PLTSQL_rec *) (estate->datums[recfield->recparentno]);
				erh = rec->erh;

				/*
				 * If record variable is NULL, instantiate it if it has a
				 * named composite type, else complain.  (This won't change
				 * the logical state of the record: it's still NULL.)
				 */
				if (erh == NULL)
				{
					instantiate_empty_record_variable(estate, rec);
					erh = rec->erh;
				}

				/*
				 * Look up the field's properties if we have not already, or
				 * if the tuple descriptor ID changed since last time.
				 */
				if (unlikely(recfield->rectupledescid != erh->er_tupdesc_id))
				{
					if (!expanded_record_lookup_field(erh,
													  recfield->fieldname,
													  &recfield->finfo))
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("record \"%s\" has no field \"%s\"",
										rec->refname, recfield->fieldname)));
					recfield->rectupledescid = erh->er_tupdesc_id;
				}

				/* Report type data. */
				*typeid = recfield->finfo.ftypeid;
				*typetypmod = recfield->finfo.ftypmod;

				/* And fetch the field value. */
				*value = expanded_record_get_field(erh,
												   recfield->finfo.fnumber,
												   isnull);
				break;
			}

		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
	}
}

/*
 * exec_get_datum_type				Get datatype of a PLTSQL_datum
 *
 * This is the same logic as in exec_eval_datum, except that it can handle
 * some cases where exec_eval_datum has to fail; specifically, we may have
 * a tupdesc but no row value for a record variable.  (This currently can
 * happen only for a trigger's NEW/OLD records.)
 */
Oid
exec_get_datum_type(PLTSQL_execstate *estate,
					PLTSQL_datum *datum)
{
	Oid			typeid;

	switch (datum->dtype)
	{
		case PLTSQL_DTYPE_VAR:
			{
				PLTSQL_var *var = (PLTSQL_var *) datum;

				typeid = var->datatype->typoid;
				break;
			}

		case PLTSQL_DTYPE_ROW:
			{
				PLTSQL_row *row = (PLTSQL_row *) datum;

				if (!row->rowtupdesc)	/* should not happen */
					elog(ERROR, "row variable has no tupdesc");
				/* Make sure we have a valid type/typmod setting */
				BlessTupleDesc(row->rowtupdesc);
				typeid = row->rowtupdesc->tdtypeid;
				break;
			}

		case PLTSQL_DTYPE_REC:
			{
				PLTSQL_rec *rec = (PLTSQL_rec *) datum;

				if (rec->tupdesc == NULL)
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("record \"%s\" is not assigned yet",
								  rec->refname),
						   errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
				/* Make sure we have a valid type/typmod setting */
				BlessTupleDesc(rec->tupdesc);
				typeid = rec->tupdesc->tdtypeid;
				break;
			}

		case PLTSQL_DTYPE_RECFIELD:
			{
				PLTSQL_recfield *recfield = (PLTSQL_recfield *) datum;
				PLTSQL_rec *rec;
				int			fno;

				rec = (PLTSQL_rec *) (estate->datums[recfield->recparentno]);
				if (rec->tupdesc == NULL)
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("record \"%s\" is not assigned yet",
								  rec->refname),
						   errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
				fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
				if (fno == SPI_ERROR_NOATTRIBUTE)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("record \"%s\" has no field \"%s\"",
									rec->refname, recfield->fieldname)));
				typeid = SPI_gettypeid(rec->tupdesc, fno);
				break;
			}

		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			typeid = InvalidOid;	/* keep compiler quiet */
			break;
	}

	return typeid;
}

/*
 * exec_get_datum_type_info			Get datatype etc of a PLTSQL_datum
 *
 * An extended version of exec_get_datum_type, which also retrieves the
 * typmod and collation of the datum.
 */
void
exec_get_datum_type_info(PLTSQL_execstate *estate,
						 PLTSQL_datum *datum,
						 Oid *typeId, int32 *typMod, Oid *collation)
{
	switch (datum->dtype)
	{
		case PLTSQL_DTYPE_VAR:
#ifdef PLTSQL_DTYPE_PROMISE
		case PLTSQL_DTYPE_PROMISE:
#endif
			{
				PLTSQL_var *var = (PLTSQL_var *) datum;

				*typeId = var->datatype->typoid;
				*typMod = var->datatype->atttypmod;
				*collation = var->datatype->collation;
				break;
			}

		case PLTSQL_DTYPE_REC:
			{
				PLTSQL_rec *rec = (PLTSQL_rec *) datum;

				if (rec->erh == NULL || rec->rectypeid != RECORDOID)
				{
					/* Report variable's declared type */
					*typeId = rec->rectypeid;
					*typMod = -1;
				}
				else
				{
					/* Report record's actual type if declared RECORD */
					*typeId = rec->erh->er_typeid;
					/* do NOT return the mutable typmod of a RECORD variable */
					*typMod = -1;
				}
				/* composite types are never collatable */
				*collation = InvalidOid;
				break;
			}

		case PLTSQL_DTYPE_RECFIELD:
			{
				PLTSQL_recfield *recfield = (PLTSQL_recfield *) datum;
				PLTSQL_rec *rec;

				rec = (PLTSQL_rec *) (estate->datums[recfield->recparentno]);

				/*
				 * If record variable is NULL, instantiate it if it has a
				 * named composite type, else complain.  (This won't change
				 * the logical state of the record: it's still NULL.)
				 */
				if (rec->erh == NULL)
					instantiate_empty_record_variable(estate, rec);

				/*
				 * Look up the field's properties if we have not already, or
				 * if the tuple descriptor ID changed since last time.
				 */
				if (unlikely(recfield->rectupledescid != rec->erh->er_tupdesc_id))
				{
					if (!expanded_record_lookup_field(rec->erh,
													  recfield->fieldname,
													  &recfield->finfo))
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("record \"%s\" has no field \"%s\"",
										rec->refname, recfield->fieldname)));
					recfield->rectupledescid = rec->erh->er_tupdesc_id;
				}

				*typeId = recfield->finfo.ftypeid;
				*typMod = recfield->finfo.ftypmod;
				*collation = recfield->finfo.fcollation;
				break;
			}

		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			*typeId = InvalidOid;	/* keep compiler quiet */
			*typMod = -1;
			*collation = InvalidOid;
			break;
	}
}

/* ----------
 * exec_eval_integer		Evaluate an expression, coerce result to int4
 *
 * Note we do not do exec_eval_cleanup here; the caller must do it at
 * some later point.  (We do this because the caller may be holding the
 * results of other, pass-by-reference, expression evaluations, such as
 * an array value to be subscripted.  Also see notes in exec_eval_simple_expr
 * about allocation of the parameter array.)
 * ----------
 */
static int
exec_eval_integer(PLTSQL_execstate *estate,
				  PLTSQL_expr *expr,
				  bool *isNull)
{
	Datum		exprdatum;
	Oid			exprtypeid;
	int32		exprtypmod;

	exprdatum = exec_eval_expr(estate, expr, isNull, &exprtypeid, &exprtypmod);
	exprdatum = exec_cast_value(estate, exprdatum, isNull,
								exprtypeid, exprtypmod,
								INT4OID, -1);
	return DatumGetInt32(exprdatum);
}

/* ----------
 * exec_eval_boolean		Evaluate an expression, coerce result to bool
 *
 * Note we do not do exec_eval_cleanup here; the caller must do it at
 * some later point.
 * ----------
 */
static bool
exec_eval_boolean(PLTSQL_execstate *estate,
				  PLTSQL_expr *expr,
				  bool *isNull)
{
	Datum		exprdatum;
	Oid			exprtypeid;
	int32  		exprtypmod;

	exprdatum = exec_eval_expr(estate, expr, isNull, &exprtypeid, &exprtypmod);
	exprdatum = exec_cast_value(estate, exprdatum, isNull,
								exprtypeid, exprtypmod,
								BOOLOID, -1);
	return DatumGetBool(exprdatum);
}

/* ----------
 * exec_eval_expr			Evaluate an expression and return
 *					the result Datum.
 *
 * NOTE: caller must do exec_eval_cleanup when done with the Datum.
 * ----------
 */
static Datum
exec_eval_expr(PLTSQL_execstate *estate,
			   PLTSQL_expr *expr,
			   bool *isNull,
			   Oid *rettype,
			   int32 *rettypmod)
{
	Datum		result = 0;
	int			rc;
	Form_pg_attribute attr;

	/*
	 * If first time through, create a plan for this expression.
	 */
	if (expr->plan == NULL)
		exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK, true);

	/*
	 * If this is a simple expression, bypass SPI and use the executor
	 * directly
	 */
	if (exec_eval_simple_expr(estate, expr,
							  &result, isNull, rettype, rettypmod))
		return result;

	/*
	 * Else do it the hard way via exec_run_select
	 */
	rc = exec_run_select(estate, expr, 2, NULL);
	if (rc != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("query \"%s\" did not return data", expr->query)));

	/*
	 * Check that the expression returns exactly one column...
	 */
	if (estate->eval_tuptable->tupdesc->natts != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg_plural("query \"%s\" returned %d column",
							   "query \"%s\" returned %d columns",
							   estate->eval_tuptable->tupdesc->natts,
							   expr->query,
							   estate->eval_tuptable->tupdesc->natts)));

	/*
	 * ... and get the column's datatype.
	 */
	attr = TupleDescAttr(estate->eval_tuptable->tupdesc, 0);
	*rettype = attr->atttypid;
	*rettypmod = attr->atttypmod;

	/*
	 * If there are no rows selected, the result is a NULL of that type.
	 */
	if (estate->eval_processed == 0)
	{
		*isNull = true;
		return (Datum) 0;
	}

	/*
	 * Check that the expression returned no more than one row.
	 */
	if (estate->eval_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("query \"%s\" returned more than one row",
						expr->query)));

	/*
	 * Return the single result Datum.
	 */
	return SPI_getbinval(estate->eval_tuptable->vals[0],
						 estate->eval_tuptable->tupdesc, 1, isNull);
}

/* ----------
 * exec_run_select			Execute a select query
 * ----------
 */
static int
exec_run_select(PLTSQL_execstate *estate,
				PLTSQL_expr *expr, long maxtuples, Portal *portalP)
{
	ParamListInfo paramLI;
	int			rc;

	/*
	 * On the first call for this expression generate the plan
	 */
	if (expr->plan == NULL)
		exec_prepare_plan(estate, expr,
						  portalP == NULL ? CURSOR_OPT_PARALLEL_OK : 0, true);

	/*
	 * Set up ParamListInfo (hook function and possibly data values)
	 */
	paramLI = setup_param_list(estate, expr);

	/*
	 * If a portal was requested, put the query into the portal
	 */
	if (portalP != NULL)
	{
		*portalP = SPI_cursor_open_with_paramlist(NULL, expr->plan,
												  paramLI,
												  estate->readonly_func);
		if (*portalP == NULL)
			elog(ERROR, "could not open implicit cursor for query \"%s\": %s",
				 expr->query, SPI_result_code_string(SPI_result));
		if (paramLI)
			pfree(paramLI);
		return SPI_OK_CURSOR;
	}

	/*
	 * Execute the query
	 */
	rc = SPI_execute_plan_with_paramlist(expr->plan, paramLI,
										 estate->readonly_func, maxtuples);
	if (rc != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("query \"%s\" is not a SELECT", expr->query)));

	/* Save query results for eventual cleanup */
	Assert(estate->eval_tuptable == NULL);
	estate->eval_tuptable = SPI_tuptable;
	estate->eval_processed = SPI_processed;
	estate->eval_lastoid = SPI_lastoid;

	if (paramLI)
		pfree(paramLI);

	return rc;
}


/*
 * exec_for_query --- execute body of FOR loop for each row from a portal
 *
 * Used by exec_stmt_fors, exec_stmt_forc and exec_stmt_dynfors
 */
static int
exec_for_query(PLTSQL_execstate *estate, PLTSQL_stmt_forq *stmt,
			   Portal portal, bool prefetch_ok)
{
	PLTSQL_variable *var;
	SPITupleTable *tuptab;
	bool		found = false;
	int			rc = PLTSQL_RC_OK;
	int			n;

	/* Fetch loop variable's datum entry */
	var = (PLTSQL_variable *) estate->datums[stmt->var->dno];

	/*
	 * Make sure the portal doesn't get closed by the user statements we
	 * execute.
	 */
	PinPortal(portal);

	/*
	 * Fetch the initial tuple(s).	If prefetching is allowed then we grab a
	 * few more rows to avoid multiple trips through executor startup
	 * overhead.
	 */
	SPI_cursor_fetch(portal, true, prefetch_ok ? 10 : 1);
	tuptab = SPI_tuptable;
	n = SPI_processed;

	/*
	 * If the query didn't return any rows, set the target to NULL and fall
	 * through with found = false.
	 */
	if (n <= 0)
	{
		exec_move_row(estate, var, NULL, tuptab->tupdesc);
		exec_eval_cleanup(estate);
	}
	else
		found = true;			/* processed at least one tuple */

	/*
	 * Now do the loop
	 */
	while (n > 0)
	{
		int			i;

		for (i = 0; i < n; i++)
		{
			/*
			 * Assign the tuple to the target
			 */
			exec_move_row(estate, var, tuptab->vals[i], tuptab->tupdesc);
			exec_eval_cleanup(estate);

			/*
			 * Execute the statements
			 */
			rc = exec_stmts(estate, stmt->body);

			if (rc != PLTSQL_RC_OK)
			{
				if (rc == PLTSQL_RC_EXIT)
				{
					if (estate->exitlabel == NULL)
					{
						/* unlabelled exit, so exit the current loop */
						rc = PLTSQL_RC_OK;
					}
					else if (stmt->label != NULL &&
							 strcmp(stmt->label, estate->exitlabel) == 0)
					{
						/* label matches this loop, so exit loop */
						estate->exitlabel = NULL;
						rc = PLTSQL_RC_OK;
					}

					/*
					 * otherwise, we processed a labelled exit that does not
					 * match the current statement's label, if any; return
					 * RC_EXIT so that the EXIT continues to recurse upward.
					 */
				}
				else if (rc == PLTSQL_RC_CONTINUE)
				{
					if (estate->exitlabel == NULL)
					{
						/* unlabelled continue, so re-run the current loop */
						rc = PLTSQL_RC_OK;
						continue;
					}
					else if (stmt->label != NULL &&
							 strcmp(stmt->label, estate->exitlabel) == 0)
					{
						/* label matches this loop, so re-run loop */
						estate->exitlabel = NULL;
						rc = PLTSQL_RC_OK;
						continue;
					}

					/*
					 * otherwise, we process a labelled continue that does not
					 * match the current statement's label, if any; return
					 * RC_CONTINUE so that the CONTINUE will propagate up the
					 * stack.
					 */
				}

				/*
				 * We're aborting the loop.  Need a goto to get out of two
				 * levels of loop...
				 */
				goto loop_exit;
			}
		}

		SPI_freetuptable(tuptab);

		/*
		 * Fetch more tuples.  If prefetching is allowed, grab 50 at a time.
		 */
		SPI_cursor_fetch(portal, true, prefetch_ok ? 50 : 1);
		tuptab = SPI_tuptable;
		n = SPI_processed;
	}

loop_exit:

	/*
	 * Release last group of tuples (if any)
	 */
	SPI_freetuptable(tuptab);

	UnpinPortal(portal);

	/*
	 * Set the FOUND variable to indicate the result of executing the loop
	 * (namely, whether we looped one or more times). This must be set last so
	 * that it does not interfere with the value of the FOUND variable inside
	 * the loop processing itself.
	 */
	exec_set_found(estate, found);

	return rc;
}


/* ----------
 * exec_eval_simple_expr -		Evaluate a simple expression returning
 *								a Datum by directly calling ExecEvalExpr().
 *
 * If successful, store results into *result, *isNull, *rettype and return
 * TRUE.  If the expression cannot be handled by simple evaluation,
 * return FALSE.
 *
 * Because we only store one execution tree for a simple expression, we
 * can't handle recursion cases.  So, if we see the tree is already busy
 * with an evaluation in the current xact, we just return FALSE and let the
 * caller run the expression the hard way.	(Other alternatives such as
 * creating a new tree for a recursive call either introduce memory leaks,
 * or add enough bookkeeping to be doubtful wins anyway.)  Another case that
 * is covered by the expr_simple_in_use test is where a previous execution
 * of the tree was aborted by an error: the tree may contain bogus state
 * so we dare not re-use it.
 *

 * It is possible though unlikely for a simple expression to become non-simple
 * (consider for example redefining a trivial view).  We must handle that for
 * correctness; fortunately it's normally inexpensive to do GetCachedPlan on a
 * simple expression.  We do not consider the other direction (non-simple
 * expression becoming simple) because we'll still give correct results if
 * that happens, and it's unlikely to be worth the cycles to check.
 *
 * Note: if pass-by-reference, the result is in the eval_econtext's
 * temporary memory context.  It will be freed when exec_eval_cleanup
 * is done.
 * ----------
 */
static bool
exec_eval_simple_expr(PLTSQL_execstate *estate,
					  PLTSQL_expr *expr,
					  Datum *result,
					  bool *isNull,
					  Oid *rettype,
					  int32 *rettypmod)
{
	ExprContext *econtext = estate->eval_econtext;
	LocalTransactionId curlxid = MyProc->lxid;
	CachedPlan *cplan;
	void	   *save_setup_arg;
	MemoryContext oldcontext;

	/*
	 * Forget it if expression wasn't simple before.
	 */
	if (expr->expr_simple_expr == NULL)
		return false;

	/*
	 * If expression is in use in current xact, don't touch it.
	 */
	if (expr->expr_simple_in_use && expr->expr_simple_lxid == curlxid)
		return false;

	/*
	 * Revalidate cached plan, so that we will notice if it became stale. (We
	 * need to hold a refcount while using the plan, anyway.)  If replanning
	 * is needed, do that work in the eval_mcontext.
	 */
	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
	cplan = SPI_plan_get_cached_plan(expr->plan);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * We can't get a failure here, because the number of CachedPlanSources in
	 * the SPI plan can't change from what exec_simple_check_plan saw; it's a
	 * property of the raw parsetree generated from the query text.
	 */
	Assert(cplan != NULL);

	/* If it got replanned, update our copy of the simple expression */
	if (cplan->generation != expr->expr_simple_generation)
	{
		exec_save_simple_expr(expr, cplan);
		/* better recheck r/w safety, as it could change due to inlining */
		if (expr->rwparam >= 0)
			exec_check_rw_parameter(expr, expr->rwparam);
	}

	/*
	 * Pass back previously-determined result type.
	 */
	*rettype = expr->expr_simple_type;
	*rettypmod = expr->expr_simple_typmod;

	/*
	 * Set up ParamListInfo to pass to executor.  For safety, save and restore
	 * estate->paramLI->parserSetupArg around our use of the param list.
	 */
	save_setup_arg = estate->paramLI->parserSetupArg;

	econtext->ecxt_param_list_info = setup_param_list(estate, expr);

	/*
	 * Prepare the expression for execution, if it's not been done already in
	 * the current transaction.  (This will be forced to happen if we called
	 * exec_save_simple_expr above.)
	 */
	if (expr->expr_simple_lxid != curlxid)
	{
		oldcontext = MemoryContextSwitchTo(estate->simple_eval_estate->es_query_cxt);
		expr->expr_simple_state =
			ExecInitExprWithParams(expr->expr_simple_expr,
								   econtext->ecxt_param_list_info);
		expr->expr_simple_in_use = false;
		expr->expr_simple_lxid = curlxid;
		MemoryContextSwitchTo(oldcontext);
	}

	/*
	 * We have to do some of the things SPI_execute_plan would do, in
	 * particular advance the snapshot if we are in a non-read-only function.
	 * Without this, stable functions within the expression would fail to see
	 * updates made so far by our own function.
	 */
	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
	if (!estate->readonly_func)
	{
		CommandCounterIncrement();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	/*
	 * Mark expression as busy for the duration of the ExecEvalExpr call.
	 */
	expr->expr_simple_in_use = true;

	/*
	 * Finally we can call the executor to evaluate the expression
	 */
	*result = ExecEvalExpr(expr->expr_simple_state,
						   econtext,
						   isNull);

	/* Assorted cleanup */
	expr->expr_simple_in_use = false;

	econtext->ecxt_param_list_info = NULL;

	estate->paramLI->parserSetupArg = save_setup_arg;

	if (!estate->readonly_func)
		PopActiveSnapshot();

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Now we can release our refcount on the cached plan.
	 */
	ReleaseCachedPlan(cplan, true);

	/*
	 * That's it.
	 */
	return true;
}

/*
 * Create a ParamListInfo to pass to SPI
 *
 * We fill in the values for any expression parameters that are plain
 * PLTSQL_var datums; these are cheap and safe to evaluate, and by setting
 * them with PARAM_FLAG_CONST flags, we allow the planner to use those values
 * in custom plans.  However, parameters that are not plain PLTSQL_vars
 * should not be evaluated here, because they could throw errors (for example
 * "no such record field") and we do not want that to happen in a part of
 * the expression that might never be evaluated at runtime.  To handle those
 * parameters, we set up a paramFetch hook for the executor to call when it
 * wants a not-presupplied value.
 *
 * The result is a locally palloc'd array that should be pfree'd after use;
 * but note it can be NULL.
 */
static ParamListInfo
setup_param_list(PLTSQL_execstate *estate, PLTSQL_expr *expr)
{
	ParamListInfo paramLI;

	/*
	 * We must have created the SPIPlan already (hence, query text has been
	 * parsed/analyzed at least once); else we cannot rely on expr->paramnos.
	 */
	Assert(expr->plan != NULL);

	/*
	 * We only need a ParamListInfo if the expression has parameters.  In
	 * principle we should test with bms_is_empty(), but we use a not-null
	 * test because it's faster.  In current usage bits are never removed from
	 * expr->paramnos, only added, so this test is correct anyway.
	 */
	if (expr->paramnos)
	{
		/* Use the common ParamListInfo */
		paramLI = estate->paramLI;

		/*
		 * Set up link to active expr where the hook functions can find it.
		 * Callers must save and restore parserSetupArg if there is any chance
		 * that they are interrupting an active use of parameters.
		 */
		paramLI->parserSetupArg = (void *) expr;

		/*
		 * Also make sure this is set before parser hooks need it.  There is
		 * no need to save and restore, since the value is always correct once
		 * set.  (Should be set already, but let's be sure.)
		 */
		expr->func = estate->func;
	}
	else
	{
		/*
		 * Expression requires no parameters.  Be sure we represent this case
		 * as a NULL ParamListInfo, so that plancache.c knows there is no
		 * point in a custom plan.
		 */
		paramLI = NULL;
	}
	return paramLI;
}

/*
 * exec_move_row			Move one tuple's values into a record or row
 *
 * tup and tupdesc may both be NULL if we're just assigning an indeterminate
 * composite NULL to the target.  Alternatively, can have tup be NULL and
 * tupdesc not NULL, in which case we assign a row of NULLs to the target.
 *
 * Since this uses the mcontext for workspace, caller should eventually call
 * exec_eval_cleanup to prevent long-term memory leaks.
 */
static void
exec_move_row(PLTSQL_execstate *estate,
			  PLTSQL_variable *target,
			  HeapTuple tup, TupleDesc tupdesc)
{
	ExpandedRecordHeader *newerh = NULL;

	/*
	 * If target is RECORD, we may be able to avoid field-by-field processing.
	 */
	if (target->dtype == PLTSQL_DTYPE_REC)
	{
		PLTSQL_rec *rec = (PLTSQL_rec *) target;

		/*
		 * If we have no source tupdesc, just set the record variable to NULL.
		 * (If we have a source tupdesc but not a tuple, we'll set the
		 * variable to a row of nulls, instead.  This is odd perhaps, but
		 * backwards compatible.)
		 */
		if (tupdesc == NULL)
		{
			if (rec->datatype &&
				rec->datatype->typtype == TYPTYPE_DOMAIN)
			{
				/*
				 * If it's a composite domain, NULL might not be a legal
				 * value, so we instead need to make an empty expanded record
				 * and ensure that domain type checking gets done.  If there
				 * is already an expanded record, piggyback on its lookups.
				 */
				newerh = make_expanded_record_for_rec(estate, rec,
													  NULL, rec->erh);
				expanded_record_set_tuple(newerh, NULL, false, false);
				assign_record_var(estate, rec, newerh);
			}
			else
			{
				/* Just clear it to NULL */
				if (rec->erh)
					DeleteExpandedObject(ExpandedRecordGetDatum(rec->erh));
				rec->erh = NULL;
			}
			return;
		}

		/*
		 * Build a new expanded record with appropriate tupdesc.
		 */
		newerh = make_expanded_record_for_rec(estate, rec, tupdesc, NULL);

		/*
		 * If the rowtypes match, or if we have no tuple anyway, we can
		 * complete the assignment without field-by-field processing.
		 *
		 * The tests here are ordered more or less in order of cheapness.  We
		 * can easily detect it will work if the target is declared RECORD or
		 * has the same typeid as the source.  But when assigning from a query
		 * result, it's common to have a source tupdesc that's labeled RECORD
		 * but is actually physically compatible with a named-composite-type
		 * target, so it's worth spending extra cycles to check for that.
		 */
		if (rec->rectypeid == RECORDOID ||
			rec->rectypeid == tupdesc->tdtypeid ||
			!HeapTupleIsValid(tup) ||
			compatible_tupdescs(tupdesc, expanded_record_get_tupdesc(newerh)))
		{
			if (!HeapTupleIsValid(tup))
			{
				/* No data, so force the record into all-nulls state */
				deconstruct_expanded_record(newerh);
			}
			else
			{
				/* No coercion is needed, so just assign the row value */
				expanded_record_set_tuple(newerh, tup, true, !estate->atomic);
			}

			/* Complete the assignment */
			assign_record_var(estate, rec, newerh);

			return;
		}
	}

	/*
	 * Otherwise, deconstruct the tuple and do field-by-field assignment,
	 * using exec_move_row_from_fields.
	 */
	if (tupdesc && HeapTupleIsValid(tup))
	{
		int			td_natts = tupdesc->natts;
		Datum	   *values;
		bool	   *nulls;
		Datum		values_local[64];
		bool		nulls_local[64];

		/*
		 * Need workspace arrays.  If td_natts is small enough, use local
		 * arrays to save doing a palloc.  Even if it's not small, we can
		 * allocate both the Datum and isnull arrays in one palloc chunk.
		 */
		if (td_natts <= lengthof(values_local))
		{
			values = values_local;
			nulls = nulls_local;
		}
		else
		{
			char	   *chunk;

			chunk = eval_mcontext_alloc(estate,
										td_natts * (sizeof(Datum) + sizeof(bool)));
			values = (Datum *) chunk;
			nulls = (bool *) (chunk + td_natts * sizeof(Datum));
		}

		heap_deform_tuple(tup, tupdesc, values, nulls);

		exec_move_row_from_fields(estate, target, newerh,
								  values, nulls, tupdesc);
	}
	else
	{
		/*
		 * Assign all-nulls.
		 */
		exec_move_row_from_fields(estate, target, newerh,
								  NULL, NULL, NULL);
	}
}

/*
 * Build an expanded record object suitable for assignment to "rec".
 *
 * Caller must supply either a source tuple descriptor or a source expanded
 * record (not both).  If the record variable has declared type RECORD,
 * it'll adopt the source's rowtype.  Even if it doesn't, we may be able to
 * piggyback on a source expanded record to save a typcache lookup.
 *
 * Caller must fill the object with data, then do assign_record_var().
 *
 * The new record is initially put into the mcontext, so it will be cleaned up
 * if we fail before reaching assign_record_var().
 */
static ExpandedRecordHeader *
make_expanded_record_for_rec(PLTSQL_execstate *estate,
							 PLTSQL_rec *rec,
							 TupleDesc srctupdesc,
							 ExpandedRecordHeader *srcerh)
{
	ExpandedRecordHeader *newerh;
	MemoryContext mcontext = get_eval_mcontext(estate);

	if (rec->rectypeid != RECORDOID)
	{
		/*
		 * New record must be of desired type, but maybe srcerh has already
		 * done all the same lookups.
		 */
		if (srcerh && rec->rectypeid == srcerh->er_decltypeid)
			newerh = make_expanded_record_from_exprecord(srcerh,
														 mcontext);
		else
			newerh = make_expanded_record_from_typeid(rec->rectypeid, -1,
													  mcontext);
	}
	else
	{
		/*
		 * We'll adopt the input tupdesc.  We can still use
		 * make_expanded_record_from_exprecord, if srcerh isn't a composite
		 * domain.  (If it is, we effectively adopt its base type.)
		 */
		if (srcerh && !ExpandedRecordIsDomain(srcerh))
			newerh = make_expanded_record_from_exprecord(srcerh,
														 mcontext);
		else
		{
			if (!srctupdesc)
				srctupdesc = expanded_record_get_tupdesc(srcerh);
			newerh = make_expanded_record_from_tupdesc(srctupdesc,
													   mcontext);
		}
	}

	return newerh;
}

/*
 * exec_move_row_from_fields	Move arrays of field values into a record or row
 *
 * When assigning to a record, the caller must have already created a suitable
 * new expanded record object, newerh.  Pass NULL when assigning to a row.
 *
 * tupdesc describes the input row, which might have different column
 * types and/or different dropped-column positions than the target.
 * values/nulls/tupdesc can all be NULL if we just want to assign nulls to
 * all fields of the record or row.
 *
 * Since this uses the mcontext for workspace, caller should eventually call
 * exec_eval_cleanup to prevent long-term memory leaks.
 */
static void
exec_move_row_from_fields(PLTSQL_execstate *estate,
						  PLTSQL_variable *target,
						  ExpandedRecordHeader *newerh,
						  Datum *values, bool *nulls,
						  TupleDesc tupdesc)
{
	int			td_natts = tupdesc ? tupdesc->natts : 0;
	int			fnum;
	int			anum;

	/* Handle RECORD-target case */
	if (target->dtype == PLTSQL_DTYPE_REC)
	{
		PLTSQL_rec *rec = (PLTSQL_rec *) target;
		TupleDesc	var_tupdesc;
		Datum		newvalues_local[64];
		bool		newnulls_local[64];

		Assert(newerh != NULL); /* caller must have built new object */

		var_tupdesc = expanded_record_get_tupdesc(newerh);

		/*
		 * Coerce field values if needed.  This might involve dealing with
		 * different sets of dropped columns and/or coercing individual column
		 * types.  That's sort of a pain, but historically plpgsql has allowed
		 * it, so we preserve the behavior.  However, it's worth a quick check
		 * to see if the tupdescs are identical.  (Since expandedrecord.c
		 * prefers to use refcounted tupdescs from the typcache, expanded
		 * records with the same rowtype will have pointer-equal tupdescs.)
		 */
		if (var_tupdesc != tupdesc)
		{
			int			vtd_natts = var_tupdesc->natts;
			Datum	   *newvalues;
			bool	   *newnulls;

			/*
			 * Need workspace arrays.  If vtd_natts is small enough, use local
			 * arrays to save doing a palloc.  Even if it's not small, we can
			 * allocate both the Datum and isnull arrays in one palloc chunk.
			 */
			if (vtd_natts <= lengthof(newvalues_local))
			{
				newvalues = newvalues_local;
				newnulls = newnulls_local;
			}
			else
			{
				char	   *chunk;

				chunk = eval_mcontext_alloc(estate,
											vtd_natts * (sizeof(Datum) + sizeof(bool)));
				newvalues = (Datum *) chunk;
				newnulls = (bool *) (chunk + vtd_natts * sizeof(Datum));
			}

			/* Walk over destination columns */
			anum = 0;
			for (fnum = 0; fnum < vtd_natts; fnum++)
			{
				Form_pg_attribute attr = TupleDescAttr(var_tupdesc, fnum);
				Datum		value;
				bool		isnull;
				Oid			valtype;
				int32		valtypmod;

				if (attr->attisdropped)
				{
					/* expanded_record_set_fields should ignore this column */
					continue;	/* skip dropped column in record */
				}

				while (anum < td_natts &&
					   TupleDescAttr(tupdesc, anum)->attisdropped)
					anum++;		/* skip dropped column in tuple */

				if (anum < td_natts)
				{
					value = values[anum];
					isnull = nulls[anum];
					valtype = TupleDescAttr(tupdesc, anum)->atttypid;
					valtypmod = TupleDescAttr(tupdesc, anum)->atttypmod;
					anum++;
				}
				else
				{
					value = (Datum) 0;
					isnull = true;
					valtype = UNKNOWNOID;
					valtypmod = -1;
				}

				/* Cast the new value to the right type, if needed. */
				newvalues[fnum] = exec_cast_value(estate,
												  value,
												  &isnull,
												  valtype,
												  valtypmod,
												  attr->atttypid,
												  attr->atttypmod);
				newnulls[fnum] = isnull;
			}

			values = newvalues;
			nulls = newnulls;
		}

		/* Insert the coerced field values into the new expanded record */
		expanded_record_set_fields(newerh, values, nulls, !estate->atomic);

		/* Complete the assignment */
		assign_record_var(estate, rec, newerh);

		return;
	}

	/* newerh should not have been passed in non-RECORD cases */
	Assert(newerh == NULL);

	/*
	 * For a row, we assign the individual field values to the variables the
	 * row points to.
	 *
	 * NOTE: both this code and the record code above silently ignore extra
	 * columns in the source and assume NULL for missing columns.  This is
	 * pretty dubious but it's the historical behavior.
	 *
	 * If we have no input data at all, we'll assign NULL to all columns of
	 * the row variable.
	 */
	if (target->dtype == PLTSQL_DTYPE_ROW)
	{
		PLTSQL_row *row = (PLTSQL_row *) target;

		anum = 0;
		for (fnum = 0; fnum < row->nfields; fnum++)
		{
			PLTSQL_var *var;
			Datum		value;
			bool		isnull;
			Oid			valtype;
			int32		valtypmod;

			var = (PLTSQL_var *) (estate->datums[row->varnos[fnum]]);

			while (anum < td_natts &&
				   TupleDescAttr(tupdesc, anum)->attisdropped)
				anum++;			/* skip dropped column in tuple */

			if (anum < td_natts)
			{
				value = values[anum];
				isnull = nulls[anum];
				valtype = TupleDescAttr(tupdesc, anum)->atttypid;
				valtypmod = TupleDescAttr(tupdesc, anum)->atttypmod;
				anum++;
			}
			else
			{
				value = (Datum) 0;
				isnull = true;
				valtype = UNKNOWNOID;
				valtypmod = -1;
			}

			exec_assign_value(estate, (PLTSQL_datum *) var,
							  value, isnull, valtype, valtypmod);
		}

		return;
	}

	elog(ERROR, "unsupported target type: %d", target->dtype);
}

/*
 * compatible_tupdescs: detect whether two tupdescs are physically compatible
 *
 * TRUE indicates that a tuple satisfying src_tupdesc can be used directly as
 * a value for a composite variable using dst_tupdesc.
 */
static bool
compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc)
{
	int			i;

	/* Possibly we could allow src_tupdesc to have extra columns? */
	if (dst_tupdesc->natts != src_tupdesc->natts)
		return false;

	for (i = 0; i < dst_tupdesc->natts; i++)
	{
		Form_pg_attribute dattr = TupleDescAttr(dst_tupdesc, i);
		Form_pg_attribute sattr = TupleDescAttr(src_tupdesc, i);

		if (dattr->attisdropped != sattr->attisdropped)
			return false;
		if (!dattr->attisdropped)
		{
			/* Normal columns must match by type and typmod */
			if (dattr->atttypid != sattr->atttypid ||
				(dattr->atttypmod >= 0 &&
				 dattr->atttypmod != sattr->atttypmod))
				return false;
		}
		else
		{
			/* Dropped columns are OK as long as length/alignment match */
			if (dattr->attlen != sattr->attlen ||
				dattr->attalign != sattr->attalign)
				return false;
		}
	}
	return true;
}

/* ----------
 * make_tuple_from_row		Make a tuple from the values of a row object
 *
 * A NULL return indicates rowtype mismatch; caller must raise suitable error
 * ----------
 */
static HeapTuple
make_tuple_from_row(PLTSQL_execstate *estate,
					PLTSQL_row *row,
					TupleDesc tupdesc)
{
	int			natts = tupdesc->natts;
	HeapTuple	tuple;
	Datum	   *dvalues;
	bool	   *nulls;
	int			i;

	if (natts != row->nfields)
		return NULL;

	dvalues = (Datum *) palloc0(natts * sizeof(Datum));
	nulls = (bool *) palloc(natts * sizeof(bool));

	for (i = 0; i < natts; i++)
	{
		Oid			fieldtypeid;
		int32		fieldtypmod;

		if (tupdesc->attrs[i].attisdropped)
		{
			nulls[i] = true;	/* leave the column as null */
			continue;
		}
		if (row->varnos[i] < 0) /* should not happen */
			elog(ERROR, "dropped rowtype entry for non-dropped column");

		exec_eval_datum(estate, estate->datums[row->varnos[i]],
						&fieldtypeid, &fieldtypmod,
						&dvalues[i], &nulls[i]);
		if (fieldtypeid != tupdesc->attrs[i].atttypid)
			return NULL;
		/* XXX should we insist on typmod match, too? */
	}

	tuple = heap_form_tuple(tupdesc, dvalues, nulls);

	pfree(dvalues);
	pfree(nulls);

	return tuple;
}

/*
 * deconstruct_composite_datum		extract tuple+tupdesc from composite Datum
 *
 * The caller must supply a HeapTupleData variable, in which we set up a
 * tuple header pointing to the composite datum's body.  To make the tuple
 * value outlive that variable, caller would need to apply heap_copytuple...
 * but current callers only need a short-lived tuple value anyway.
 *
 * Returns a pointer to the TupleDesc of the datum's rowtype.
 * Caller is responsible for calling ReleaseTupleDesc when done with it.
 *
 * Note: it's caller's responsibility to be sure value is of composite type.
 * Also, best to call this in a short-lived context, as it might leak memory.
 */
static TupleDesc
deconstruct_composite_datum(Datum value, HeapTupleData *tmptup)
{
	HeapTupleHeader td;
	Oid			tupType;
	int32		tupTypmod;

	/* Get tuple body (note this could involve detoasting) */
	td = DatumGetHeapTupleHeader(value);

	/* Build a temporary HeapTuple control structure */
	tmptup->t_len = HeapTupleHeaderGetDatumLength(td);
	ItemPointerSetInvalid(&(tmptup->t_self));
	tmptup->t_tableOid = InvalidOid;
	tmptup->t_data = td;

	/* Extract rowtype info and find a tupdesc */
	tupType = HeapTupleHeaderGetTypeId(td);
	tupTypmod = HeapTupleHeaderGetTypMod(td);
	return lookup_rowtype_tupdesc(tupType, tupTypmod);
}

/*
 * exec_move_row_from_datum		Move a composite Datum into a record or row
 *
 * This is equivalent to deconstruct_composite_datum() followed by
 * exec_move_row(), but we can optimize things if the Datum is an
 * expanded-record reference.
 *
 * Note: it's caller's responsibility to be sure value is of composite type.
 */
static void
exec_move_row_from_datum(PLTSQL_execstate *estate,
						 PLTSQL_variable *target,
						 Datum value)
{
	/* Check to see if source is an expanded record */
	if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value)))
	{
		ExpandedRecordHeader *erh = (ExpandedRecordHeader *) DatumGetEOHP(value);
		ExpandedRecordHeader *newerh = NULL;

		Assert(erh->er_magic == ER_MAGIC);

		/* These cases apply if the target is record not row... */
		if (target->dtype == PLTSQL_DTYPE_REC)
		{
			PLTSQL_rec *rec = (PLTSQL_rec *) target;

			/*
			 * If it's the same record already stored in the variable, do
			 * nothing.  This would happen only in silly cases like "r := r",
			 * but we need some check to avoid possibly freeing the variable's
			 * live value below.  Note that this applies even if what we have
			 * is a R/O pointer.
			 */
			if (erh == rec->erh)
				return;

			/*
			 * If we have a R/W pointer, we're allowed to just commandeer
			 * ownership of the expanded record.  If it's of the right type to
			 * put into the record variable, do that.  (Note we don't accept
			 * an expanded record of a composite-domain type as a RECORD
			 * value.  We'll treat it as the base composite type instead;
			 * compare logic in make_expanded_record_for_rec.)
			 */
			if (VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(value)) &&
				(rec->rectypeid == erh->er_decltypeid ||
				 (rec->rectypeid == RECORDOID &&
				  !ExpandedRecordIsDomain(erh))))
			{
				assign_record_var(estate, rec, erh);
				return;
			}

			/*
			 * If we already have an expanded record object in the target
			 * variable, and the source record contains a valid tuple
			 * representation with the right rowtype, then we can skip making
			 * a new expanded record and just assign the tuple with
			 * expanded_record_set_tuple.  (We can't do the equivalent if we
			 * have to do field-by-field assignment, since that wouldn't be
			 * atomic if there's an error.)  We consider that there's a
			 * rowtype match only if it's the same named composite type or
			 * same registered rowtype; checking for matches of anonymous
			 * rowtypes would be more expensive than this is worth.
			 */
			if (rec->erh &&
				(erh->flags & ER_FLAG_FVALUE_VALID) &&
				erh->er_typeid == rec->erh->er_typeid &&
				(erh->er_typeid != RECORDOID ||
				 (erh->er_typmod == rec->erh->er_typmod &&
				  erh->er_typmod >= 0)))
			{
				expanded_record_set_tuple(rec->erh, erh->fvalue,
										  true, !estate->atomic);
				return;
			}

			/*
			 * Otherwise we're gonna need a new expanded record object.  Make
			 * it here in hopes of piggybacking on the source object's
			 * previous typcache lookup.
			 */
			newerh = make_expanded_record_for_rec(estate, rec, NULL, erh);

			/*
			 * If the expanded record contains a valid tuple representation,
			 * and we don't need rowtype conversion, then just copying the
			 * tuple is probably faster than field-by-field processing.  (This
			 * isn't duplicative of the previous check, since here we will
			 * catch the case where the record variable was previously empty.)
			 */
			if ((erh->flags & ER_FLAG_FVALUE_VALID) &&
				(rec->rectypeid == RECORDOID ||
				 rec->rectypeid == erh->er_typeid))
			{
				expanded_record_set_tuple(newerh, erh->fvalue,
										  true, !estate->atomic);
				assign_record_var(estate, rec, newerh);
				return;
			}

			/*
			 * Need to special-case empty source record, else code below would
			 * leak newerh.
			 */
			if (ExpandedRecordIsEmpty(erh))
			{
				/* Set newerh to a row of NULLs */
				deconstruct_expanded_record(newerh);
				assign_record_var(estate, rec, newerh);
				return;
			}
		}						/* end of record-target-only cases */

		/*
		 * If the source expanded record is empty, we should treat that like a
		 * NULL tuple value.  (We're unlikely to see such a case, but we must
		 * check this; deconstruct_expanded_record would cause a change of
		 * logical state, which is not OK.)
		 */
		if (ExpandedRecordIsEmpty(erh))
		{
			exec_move_row(estate, target, NULL,
						  expanded_record_get_tupdesc(erh));
			return;
		}

		/*
		 * Otherwise, ensure that the source record is deconstructed, and
		 * assign from its field values.
		 */
		deconstruct_expanded_record(erh);
		exec_move_row_from_fields(estate, target, newerh,
								  erh->dvalues, erh->dnulls,
								  expanded_record_get_tupdesc(erh));
	}
	else
	{
		/*
		 * Nope, we've got a plain composite Datum.  Deconstruct it; but we
		 * don't use deconstruct_composite_datum(), because we may be able to
		 * skip calling lookup_rowtype_tupdesc().
		 */
		HeapTupleHeader td;
		HeapTupleData tmptup;
		Oid			tupType;
		int32		tupTypmod;
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* Ensure that any detoasted data winds up in the eval_mcontext */
		oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
		/* Get tuple body (note this could involve detoasting) */
		td = DatumGetHeapTupleHeader(value);
		MemoryContextSwitchTo(oldcontext);

		/* Build a temporary HeapTuple control structure */
		tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
		ItemPointerSetInvalid(&(tmptup.t_self));
		tmptup.t_tableOid = InvalidOid;
		tmptup.t_data = td;

		/* Extract rowtype info */
		tupType = HeapTupleHeaderGetTypeId(td);
		tupTypmod = HeapTupleHeaderGetTypMod(td);

		/* Now, if the target is record not row, maybe we can optimize ... */
		if (target->dtype == PLTSQL_DTYPE_REC)
		{
			PLTSQL_rec *rec = (PLTSQL_rec *) target;

			/*
			 * If we already have an expanded record object in the target
			 * variable, and the source datum has a matching rowtype, then we
			 * can skip making a new expanded record and just assign the tuple
			 * with expanded_record_set_tuple.  We consider that there's a
			 * rowtype match only if it's the same named composite type or
			 * same registered rowtype.  (Checking to reject an anonymous
			 * rowtype here should be redundant, but let's be safe.)
			 */
			if (rec->erh &&
				tupType == rec->erh->er_typeid &&
				(tupType != RECORDOID ||
				 (tupTypmod == rec->erh->er_typmod &&
				  tupTypmod >= 0)))
			{
				expanded_record_set_tuple(rec->erh, &tmptup,
										  true, !estate->atomic);
				return;
			}

			/*
			 * If the source datum has a rowtype compatible with the target
			 * variable, just build a new expanded record and assign the tuple
			 * into it.  Using make_expanded_record_from_typeid() here saves
			 * one typcache lookup compared to the code below.
			 */
			if (rec->rectypeid == RECORDOID || rec->rectypeid == tupType)
			{
				ExpandedRecordHeader *newerh;
				MemoryContext mcontext = get_eval_mcontext(estate);

				newerh = make_expanded_record_from_typeid(tupType, tupTypmod,
														  mcontext);
				expanded_record_set_tuple(newerh, &tmptup,
										  true, !estate->atomic);
				assign_record_var(estate, rec, newerh);
				return;
			}

			/*
			 * Otherwise, we're going to need conversion, so fall through to
			 * do it the hard way.
			 */
		}

		/*
		 * ROW target, or unoptimizable RECORD target, so we have to expend a
		 * lookup to obtain the source datum's tupdesc.
		 */
		tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

		/* Do the move */
		exec_move_row(estate, target, &tmptup, tupdesc);

		/* Release tupdesc usage count */
		ReleaseTupleDesc(tupdesc);
	}
}

/*
 * If we have not created an expanded record to hold the record variable's
 * value, do so.  The expanded record will be "empty", so this does not
 * change the logical state of the record variable: it's still NULL.
 * However, now we'll have a tupdesc with which we can e.g. look up fields.
 */
static void
instantiate_empty_record_variable(PLTSQL_execstate *estate, PLTSQL_rec *rec)
{
	Assert(rec->erh == NULL);	/* else caller error */

	/* If declared type is RECORD, we can't instantiate */
	if (rec->rectypeid == RECORDOID)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("record \"%s\" is not assigned yet", rec->refname),
				 errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));

	/* OK, do it */
	rec->erh = make_expanded_record_from_typeid(rec->rectypeid, -1,
												estate->datum_context);
}

/* ----------
 * convert_value_to_string			Convert a non-null Datum to C string
 *
 * Note: the result is in the estate's eval_econtext, and will be cleared
 * by the next exec_eval_cleanup() call.  The invoked output function might
 * leave additional cruft there as well, so just pfree'ing the result string
 * would not be enough to avoid memory leaks if we did not do it like this.
 * In most usages the Datum being passed in is also in that context (if
 * pass-by-reference) and so an exec_eval_cleanup() call is needed anyway.
 *
 * Note: not caching the conversion function lookup is bad for performance.
 * ----------
 */
static char *
convert_value_to_string(PLTSQL_execstate *estate, Datum value, Oid valtype)
{
	char	   *result;
	MemoryContext oldcontext;
	Oid			typoutput;
	bool		typIsVarlena;

	oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
	getTypeOutputInfo(valtype, &typoutput, &typIsVarlena);
	result = OidOutputFunctionCall(typoutput, value);
	MemoryContextSwitchTo(oldcontext);

	return result;
}

/* ----------
 * exec_cast_value			Cast a value if required
 *
 * Note: the estate's eval_econtext is used for temporary storage, and may
 * also contain the result Datum if we have to do a conversion to a pass-
 * by-reference data type.  Be sure to do an exec_eval_cleanup() call when
 * done with the result.
 * ----------
 */
static Datum
exec_cast_value(PLTSQL_execstate *estate,
				Datum value, bool *isnull,
				Oid valtype, int32 valtypmod,
				Oid reqtype, int32 reqtypmod)
{
	/*
	 * If the type of the given value isn't what's requested, convert it.
	 */
	if (valtype != reqtype ||
		(valtypmod != reqtypmod && reqtypmod != -1))
	{
		pltsql_CastHashEntry *cast_entry;

		cast_entry = get_cast_hashentry(estate,
										valtype, valtypmod,
										reqtype, reqtypmod);
		if (cast_entry)
		{
			ExprContext *econtext = estate->eval_econtext;
			MemoryContext oldcontext;

			oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

			econtext->caseValue_datum = value;
			econtext->caseValue_isNull = *isnull;

			cast_entry->cast_in_use = true;

			value = ExecEvalExpr(cast_entry->cast_exprstate, econtext,
								 isnull);

			cast_entry->cast_in_use = false;

			MemoryContextSwitchTo(oldcontext);
		}
	}

	return value;
}

/* ----------
 * get_cast_hashentry			Look up how to perform a type cast
 *
 * Returns a plpgsql_CastHashEntry if an expression has to be evaluated,
 * or NULL if the cast is a mere no-op relabeling.  If there's work to be
 * done, the cast_exprstate field contains an expression evaluation tree
 * based on a CaseTestExpr input, and the cast_in_use field should be set
 * true while executing it.
 * ----------
 */
static pltsql_CastHashEntry *
get_cast_hashentry(PLTSQL_execstate *estate,
				   Oid srctype, int32 srctypmod,
				   Oid dsttype, int32 dsttypmod)
{
	pltsql_CastHashKey cast_key;
	pltsql_CastHashEntry *cast_entry;
	bool		found;
	LocalTransactionId curlxid;
	MemoryContext oldcontext;

	/* Look for existing entry */
	cast_key.srctype = srctype;
	cast_key.dsttype = dsttype;
	cast_key.srctypmod = srctypmod;
	cast_key.dsttypmod = dsttypmod;
	cast_entry = (pltsql_CastHashEntry *) hash_search(estate->cast_hash,
													   (void *) &cast_key,
													   HASH_FIND, NULL);

	if (cast_entry == NULL)
	{
		/* We've not looked up this coercion before */
		Node	   *cast_expr;
		CaseTestExpr *placeholder;

		/*
		 * Since we could easily fail (no such coercion), construct a
		 * temporary coercion expression tree in the short-lived
		 * eval_mcontext, then if successful copy it to cast_hash_context.
		 */
		oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

		/*
		 * We use a CaseTestExpr as the base of the coercion tree, since it's
		 * very cheap to insert the source value for that.
		 */
		placeholder = makeNode(CaseTestExpr);
		placeholder->typeId = srctype;
		placeholder->typeMod = srctypmod;
		placeholder->collation = get_typcollation(srctype);

		/*
		 * Apply coercion.  We use ASSIGNMENT coercion because that's the
		 * closest match to plpgsql's historical behavior; in particular,
		 * EXPLICIT coercion would allow silent truncation to a destination
		 * varchar/bpchar's length, which we do not want.
		 *
		 * If source type is UNKNOWN, coerce_to_target_type will fail (it only
		 * expects to see that for Const input nodes), so don't call it; we'll
		 * apply CoerceViaIO instead.  Likewise, it doesn't currently work for
		 * coercing RECORD to some other type, so skip for that too.
		 */
		if (srctype == UNKNOWNOID || srctype == RECORDOID)
			cast_expr = NULL;
		else
			cast_expr = coerce_to_target_type(NULL,
											  (Node *) placeholder, srctype,
											  dsttype, dsttypmod,
											  COERCION_ASSIGNMENT,
											  COERCE_IMPLICIT_CAST,
											  -1);

		/*
		 * If there's no cast path according to the parser, fall back to using
		 * an I/O coercion; this is semantically dubious but matches plpgsql's
		 * historical behavior.  We would need something of the sort for
		 * UNKNOWN literals in any case.
		 */
		if (cast_expr == NULL)
		{
			CoerceViaIO *iocoerce = makeNode(CoerceViaIO);

			iocoerce->arg = (Expr *) placeholder;
			iocoerce->resulttype = dsttype;
			iocoerce->resultcollid = InvalidOid;
			iocoerce->coerceformat = COERCE_IMPLICIT_CAST;
			iocoerce->location = -1;
			cast_expr = (Node *) iocoerce;
			if (dsttypmod != -1)
				cast_expr = coerce_to_target_type(NULL,
												  cast_expr, dsttype,
												  dsttype, dsttypmod,
												  COERCION_ASSIGNMENT,
												  COERCE_IMPLICIT_CAST,
												  -1);
		}

		/* Note: we don't bother labeling the expression tree with collation */

		/* Detect whether we have a no-op (RelabelType) coercion */
		if (IsA(cast_expr, RelabelType) &&
			((RelabelType *) cast_expr)->arg == (Expr *) placeholder)
			cast_expr = NULL;

		if (cast_expr)
		{
			/* ExecInitExpr assumes we've planned the expression */
			cast_expr = (Node *) expression_planner((Expr *) cast_expr);

			/* Now copy the tree into cast_hash_context */
			MemoryContextSwitchTo(estate->cast_hash_context);

			cast_expr = copyObject(cast_expr);
		}

		MemoryContextSwitchTo(oldcontext);

		/* Now we can fill in a hashtable entry. */
		cast_entry = (pltsql_CastHashEntry *) hash_search(estate->cast_hash,
														   (void *) &cast_key,
														   HASH_ENTER, &found);
		Assert(!found);			/* wasn't there a moment ago */
		cast_entry->cast_expr = (Expr *) cast_expr;
		cast_entry->cast_exprstate = NULL;
		cast_entry->cast_in_use = false;
		cast_entry->cast_lxid = InvalidLocalTransactionId;
	}

	/* Done if we have determined that this is a no-op cast. */
	if (cast_entry->cast_expr == NULL)
		return NULL;

	/*
	 * Prepare the expression for execution, if it's not been done already in
	 * the current transaction; also, if it's marked busy in the current
	 * transaction, abandon that expression tree and build a new one, so as to
	 * avoid potential problems with recursive cast expressions and failed
	 * executions.  (We will leak some memory intra-transaction if that
	 * happens a lot, but we don't expect it to.)  It's okay to update the
	 * hash table with the new tree because all plpgsql functions within a
	 * given transaction share the same simple_eval_estate.  (Well, regular
	 * functions do; DO blocks have private simple_eval_estates, and private
	 * cast hash tables to go with them.)
	 */
	curlxid = MyProc->lxid;
	if (cast_entry->cast_lxid != curlxid || cast_entry->cast_in_use)
	{
		oldcontext = MemoryContextSwitchTo(estate->simple_eval_estate->es_query_cxt);
		cast_entry->cast_exprstate = ExecInitExpr(cast_entry->cast_expr, NULL);
		cast_entry->cast_in_use = false;
		cast_entry->cast_lxid = curlxid;
		MemoryContextSwitchTo(oldcontext);
	}

	return cast_entry;
}

/* ----------
 * exec_simple_check_plan -		Check if a plan is simple enough to
 *								be evaluated by ExecEvalExpr() instead
 *								of SPI.
 * ----------
 */
static void
exec_simple_check_plan(PLTSQL_execstate *estate, PLTSQL_expr *expr)
{
	List	   *plansources;
	CachedPlanSource *plansource;
	Query	   *query;
	CachedPlan *cplan;
	MemoryContext oldcontext;

	/*
	 * Initialize to "not simple".
	 */
	expr->expr_simple_expr = NULL;

	/*
	 * Check the analyzed-and-rewritten form of the query to see if we will be
	 * able to treat it as a simple expression.  Since this function is only
	 * called immediately after creating the CachedPlanSource, we need not
	 * worry about the query being stale.
	 */

	/*
	 * We can only test queries that resulted in exactly one CachedPlanSource
	 */
	plansources = SPI_plan_get_plan_sources(expr->plan);
	if (list_length(plansources) != 1)
		return;
	plansource = (CachedPlanSource *) linitial(plansources);

	/*
	 * 1. There must be one single querytree.
	 */
	if (list_length(plansource->query_list) != 1)
		return;
	query = (Query *) linitial(plansource->query_list);

	/*
	 * 2. It must be a plain SELECT query without any input tables
	 */
	if (!IsA(query, Query))
		return;
	if (query->commandType != CMD_SELECT)
		return;
	if (query->rtable != NIL)
		return;

	/*
	 * 3. Can't have any subplans, aggregates, qual clauses either.  (These
	 * tests should generally match what inline_function() checks before
	 * inlining a SQL function; otherwise, inlining could change our
	 * conclusion about whether an expression is simple, which we don't want.)
	 */
	if (query->hasAggs ||
		query->hasWindowFuncs ||
		query->hasTargetSRFs ||
		query->hasSubLinks ||
		query->cteList ||
		query->jointree->fromlist ||
		query->jointree->quals ||
		query->groupClause ||
		query->groupingSets ||
		query->havingQual ||
		query->windowClause ||
		query->distinctClause ||
		query->sortClause ||
		query->limitOffset ||
		query->limitCount ||
		query->setOperations)
		return;

	/*
	 * 4. The query must have a single attribute as result
	 */
	if (list_length(query->targetList) != 1)
		return;

	/*
	 * OK, we can treat it as a simple plan.
	 *
	 * Get the generic plan for the query.  If replanning is needed, do that
	 * work in the eval_mcontext.
	 */
	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
	cplan = SPI_plan_get_cached_plan(expr->plan);
	MemoryContextSwitchTo(oldcontext);

	/* Can't fail, because we checked for a single CachedPlanSource above */
	Assert(cplan != NULL);

	/* Share the remaining work with replan code path */
	exec_save_simple_expr(expr, cplan);

	/* Release our plan refcount */
	ReleaseCachedPlan(cplan, true);
}

/*
 * exec_save_simple_expr --- extract simple expression from CachedPlan
 */
static void
exec_save_simple_expr(PLTSQL_expr *expr, CachedPlan *cplan)
{
	PlannedStmt *stmt;
	Plan	   *plan;
	Expr	   *tle_expr;

	/*
	 * Given the checks that exec_simple_check_plan did, none of the Asserts
	 * here should ever fail.
	 */

	/* Extract the single PlannedStmt */
	Assert(list_length(cplan->stmt_list) == 1);
	stmt = linitial_node(PlannedStmt, cplan->stmt_list);
	Assert(stmt->commandType == CMD_SELECT);

	/*
	 * Ordinarily, the plan node should be a simple Result.  However, if
	 * force_parallel_mode is on, the planner might've stuck a Gather node
	 * atop that.  The simplest way to deal with this is to look through the
	 * Gather node.  The Gather node's tlist would normally contain a Var
	 * referencing the child node's output, but it could also be a Param, or
	 * it could be a Const that setrefs.c copied as-is.
	 */
	plan = stmt->planTree;
	for (;;)
	{
		/* Extract the single tlist expression */
		Assert(list_length(plan->targetlist) == 1);
		tle_expr = castNode(TargetEntry, linitial(plan->targetlist))->expr;

		if (IsA(plan, Result))
		{
			Assert(plan->lefttree == NULL &&
				   plan->righttree == NULL &&
				   plan->initPlan == NULL &&
				   plan->qual == NULL &&
				   ((Result *) plan)->resconstantqual == NULL);
			break;
		}
		else if (IsA(plan, Gather))
		{
			Assert(plan->lefttree != NULL &&
				   plan->righttree == NULL &&
				   plan->initPlan == NULL &&
				   plan->qual == NULL);
			/* If setrefs.c copied up a Const, no need to look further */
			if (IsA(tle_expr, Const))
				break;
			/* Otherwise, it had better be a Param or an outer Var */
			Assert(IsA(tle_expr, Param) ||(IsA(tle_expr, Var) &&
										   ((Var *) tle_expr)->varno == OUTER_VAR));
			/* Descend to the child node */
			plan = plan->lefttree;
		}
		else
			elog(ERROR, "unexpected plan node type: %d",
				 (int) nodeTag(plan));
	}

	/*
	 * Save the simple expression, and initialize state to "not valid in
	 * current transaction".
	 */
	expr->expr_simple_expr = tle_expr;
	expr->expr_simple_generation = cplan->generation;
	expr->expr_simple_state = NULL;
	expr->expr_simple_in_use = false;
	expr->expr_simple_lxid = InvalidLocalTransactionId;
	/* Also stash away the expression result type */
	expr->expr_simple_type = exprType((Node *) tle_expr);
	expr->expr_simple_typmod = exprTypmod((Node *) tle_expr);
}

/*
 * exec_check_rw_parameter --- can we pass expanded object as read/write param?
 *
 * If we have an assignment like "x := array_append(x, foo)" in which the
 * top-level function is trusted not to corrupt its argument in case of an
 * error, then when x has an expanded object as value, it is safe to pass the
 * value as a read/write pointer and let the function modify the value
 * in-place.
 *
 * This function checks for a safe expression, and sets expr->rwparam to the
 * dno of the target variable (x) if safe, or -1 if not safe.
 */
static void
exec_check_rw_parameter(PLTSQL_expr *expr, int target_dno)
{
	Oid			funcid;
	List	   *fargs;
	ListCell   *lc;

	/* Assume unsafe */
	expr->rwparam = -1;

	/*
	 * If the expression isn't simple, there's no point in trying to optimize
	 * (because the exec_run_select code path will flatten any expanded result
	 * anyway).  Even without that, this seems like a good safety restriction.
	 */
	if (expr->expr_simple_expr == NULL)
		return;

	/*
	 * If target variable isn't referenced by expression, no need to look
	 * further.
	 */
	if (!bms_is_member(target_dno, expr->paramnos))
		return;

	/*
	 * Top level of expression must be a simple FuncExpr or OpExpr.
	 */
	if (IsA(expr->expr_simple_expr, FuncExpr))
	{
		FuncExpr   *fexpr = (FuncExpr *) expr->expr_simple_expr;

		funcid = fexpr->funcid;
		fargs = fexpr->args;
	}
	else if (IsA(expr->expr_simple_expr, OpExpr))
	{
		OpExpr	   *opexpr = (OpExpr *) expr->expr_simple_expr;

		funcid = opexpr->opfuncid;
		fargs = opexpr->args;
	}
	else
		return;

	/*
	 * The top-level function must be one that we trust to be "safe".
	 * Currently we hard-wire the list, but it would be very desirable to
	 * allow extensions to mark their functions as safe ...
	 */
	if (!(funcid == F_ARRAY_APPEND ||
		  funcid == F_ARRAY_PREPEND))
		return;

	/*
	 * The target variable (in the form of a Param) must only appear as a
	 * direct argument of the top-level function.
	 */
	foreach(lc, fargs)
	{
		Node	   *arg = (Node *) lfirst(lc);

		/* A Param is OK, whether it's the target variable or not */
		if (arg && IsA(arg, Param))
			continue;
		/* Otherwise, argument expression must not reference target */
		if (contains_target_param(arg, &target_dno))
			return;
	}

	/* OK, we can pass target as a read-write parameter */
	expr->rwparam = target_dno;
}

/*
 * Recursively check for a Param referencing the target variable
 */
static bool
contains_target_param(Node *node, int *target_dno)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXTERN &&
			param->paramid == *target_dno + 1)
			return true;
		return false;
	}
	return expression_tree_walker(node, contains_target_param,
								  (void *) target_dno);
}

/* ----------
 * exec_set_found			Set the global found variable to true/false
 * ----------
 */
static void
exec_set_found(PLTSQL_execstate *estate, bool state)
{
	PLTSQL_var *var;

	var = (PLTSQL_var *) (estate->datums[estate->found_varno]);
	var->value = BoolGetDatum(state);
	var->isnull = false;
}

/*
 * pltsql_create_econtext --- create an eval_econtext for the current function
 *
 * We may need to create a new simple_eval_estate too, if there's not one
 * already for the current transaction.  The EState will be cleaned up at
 * transaction end.
 */
static void
pltsql_create_econtext(PLTSQL_execstate *estate)
{
	SimpleEcontextStackEntry *entry;

	/*
	 * Create an EState for evaluation of simple expressions, if there's not
	 * one already in the current transaction.	The EState is made a child of
	 * TopTransactionContext so it will have the right lifespan.
	 */
	if (estate->simple_eval_estate == NULL)
	{
		MemoryContext oldcontext;

		if (shared_simple_eval_estate == NULL)
		{
			oldcontext = MemoryContextSwitchTo(TopTransactionContext);
			shared_simple_eval_estate = CreateExecutorState();
			MemoryContextSwitchTo(oldcontext);
		}
		estate->simple_eval_estate = shared_simple_eval_estate;
	}

	/*
	 * Create a child econtext for the current function.
	 */
	estate->eval_econtext = CreateExprContext(estate->simple_eval_estate);

	/*
	 * Make a stack entry so we can clean up the econtext at subxact end.
	 * Stack entries are kept in TopTransactionContext for simplicity.
	 */
	entry = (SimpleEcontextStackEntry *)
		MemoryContextAlloc(TopTransactionContext,
						   sizeof(SimpleEcontextStackEntry));

	entry->stack_econtext = estate->eval_econtext;
	entry->xact_subxid = GetCurrentSubTransactionId();

	entry->next = simple_econtext_stack;
	simple_econtext_stack = entry;
}

/*
 * pltsql_destroy_econtext --- destroy function's econtext
 *
 * We check that it matches the top stack entry, and destroy the stack
 * entry along with the context.
 */
static void
pltsql_destroy_econtext(PLTSQL_execstate *estate)
{
	SimpleEcontextStackEntry *next;

	Assert(simple_econtext_stack != NULL);
	Assert(simple_econtext_stack->stack_econtext == estate->eval_econtext);

	next = simple_econtext_stack->next;
	pfree(simple_econtext_stack);
	simple_econtext_stack = next;

	FreeExprContext(estate->eval_econtext, true);
	estate->eval_econtext = NULL;
}

/*
 * pltsql_xact_cb --- post-transaction-commit-or-abort cleanup
 *
 * If a simple-expression EState was created in the current transaction,
 * it has to be cleaned up.
 */
void
pltsql_xact_cb(XactEvent event, void *arg)
{
	/*
	 * If we are doing a clean transaction shutdown, free the EState (so that
	 * any remaining resources will be released correctly). In an abort, we
	 * expect the regular abort recovery procedures to release everything of
	 * interest.
	 */
	if (event != XACT_EVENT_ABORT)
	{
		simple_econtext_stack = NULL;

		if (shared_simple_eval_estate)
			FreeExecutorState(shared_simple_eval_estate);
		shared_simple_eval_estate = NULL;
	}
	else
	{
		simple_econtext_stack = NULL;
		shared_simple_eval_estate = NULL;
	}
}

/*
 * pltsql_subxact_cb --- post-subtransaction-commit-or-abort cleanup
 *
 * Make sure any simple-expression econtexts created in the current
 * subtransaction get cleaned up.  We have to do this explicitly because
 * no other code knows which child econtexts of simple_eval_estate belong
 * to which level of subxact.
 */
void
pltsql_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
				   SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_START_SUB)
		return;

	while (simple_econtext_stack != NULL &&
		   simple_econtext_stack->xact_subxid == mySubid)
	{
		SimpleEcontextStackEntry *next;

		FreeExprContext(simple_econtext_stack->stack_econtext,
						(event == SUBXACT_EVENT_COMMIT_SUB));
		next = simple_econtext_stack->next;
		pfree(simple_econtext_stack);
		simple_econtext_stack = next;
	}
}

/*
 * free_var --- pfree any pass-by-reference value of the variable.
 *
 * This should always be followed by some assignment to var->value,
 * as it leaves a dangling pointer.
 */
static void
free_var(PLTSQL_var *var)
{
	if (var->freeval)
	{
		pfree(DatumGetPointer(var->value));
		var->freeval = false;
	}
}

/*
 * assign_simple_var --- assign a new value to any VAR datum.
 *
 * This should be the only mechanism for assignment to simple variables,
 * lest we do the release of the old value incorrectly (not to mention
 * the detoasting business).
 */
static void
assign_simple_var(PLTSQL_execstate *estate, PLTSQL_var *var,
				  Datum newvalue, bool isnull, bool freeable)
{
	Assert(var->dtype == PLTSQL_DTYPE_VAR ||
		   var->dtype == PLTSQL_DTYPE_PROMISE);

	/*
	 * In non-atomic contexts, we do not want to store TOAST pointers in
	 * variables, because such pointers might become stale after a commit.
	 * Forcibly detoast in such cases.  We don't want to detoast (flatten)
	 * expanded objects, however; those should be OK across a transaction
	 * boundary since they're just memory-resident objects.  (Elsewhere in
	 * this module, operations on expanded records likewise need to request
	 * detoasting of record fields when !estate->atomic.  Expanded arrays are
	 * not a problem since all array entries are always detoasted.)
	 */
	if (!estate->atomic && !isnull && var->datatype->typlen == -1 &&
		VARATT_IS_EXTERNAL_NON_EXPANDED(DatumGetPointer(newvalue)))
	{
		MemoryContext oldcxt;
		Datum		detoasted;

		/*
		 * Do the detoasting in the eval_mcontext to avoid long-term leakage
		 * of whatever memory toast fetching might leak.  Then we have to copy
		 * the detoasted datum to the function's main context, which is a
		 * pain, but there's little choice.
		 */
		oldcxt = MemoryContextSwitchTo(get_eval_mcontext(estate));
		detoasted = PointerGetDatum(heap_tuple_fetch_attr((struct varlena *) DatumGetPointer(newvalue)));
		MemoryContextSwitchTo(oldcxt);
		/* Now's a good time to not leak the input value if it's freeable */
		if (freeable)
			pfree(DatumGetPointer(newvalue));
		/* Once we copy the value, it's definitely freeable */
		newvalue = datumCopy(detoasted, false, -1);
		freeable = true;
		/* Can't clean up eval_mcontext here, but it'll happen before long */
	}

	/* Free the old value if needed */
	if (var->freeval)
	{
		if (DatumIsReadWriteExpandedObject(var->value,
										   var->isnull,
										   var->datatype->typlen))
			DeleteExpandedObject(var->value);
		else
			pfree(DatumGetPointer(var->value));
	}
	/* Assign new value to datum */
	var->value = newvalue;
	var->isnull = isnull;
	var->freeval = freeable;

	/*
	 * If it's a promise variable, then either we just assigned the promised
	 * value, or the user explicitly assigned an overriding value.  Either
	 * way, cancel the promise.
	 */
#ifdef PLTSQL_PROMISE_NONE
	var->promise = PLTSQL_PROMISE_NONE;
#endif
}

/*
 * free old value of a text variable and assign new value from C string
 */
static void
assign_text_var(PLTSQL_var *var, const char *str)
{
	free_var(var);
	var->value = CStringGetTextDatum(str);
	var->isnull = false;
	var->freeval = true;
}

/*
 * assign_record_var --- assign a new value to any REC datum.
 */
static void
assign_record_var(PLTSQL_execstate *estate, PLTSQL_rec *rec,
				  ExpandedRecordHeader *erh)
{
	Assert(rec->dtype == PLTSQL_DTYPE_REC);

	/* Transfer new record object into datum_context */
	TransferExpandedRecord(erh, estate->datum_context);

	/* Free the old value ... */
	if (rec->erh)
		DeleteExpandedObject(ExpandedRecordGetDatum(rec->erh));

	/* ... and install the new */
	rec->erh = erh;
}

/*
 * exec_eval_using_params --- evaluate params of USING clause
 *
 * The result data structure is created in the stmt_mcontext, and should
 * be freed by resetting that context.
 */
static PreparedParamsData *
exec_eval_using_params(PLTSQL_execstate *estate, List *params)
{
	PreparedParamsData *ppd;
	MemoryContext stmt_mcontext = get_stmt_mcontext(estate);
	int			nargs;
	int			i;
	ListCell   *lc;

	ppd = (PreparedParamsData *)
		MemoryContextAlloc(stmt_mcontext, sizeof(PreparedParamsData));
	nargs = list_length(params);

	ppd->nargs = nargs;
	ppd->types = (Oid *)
		MemoryContextAlloc(stmt_mcontext, nargs * sizeof(Oid));
	ppd->values = (Datum *)
		MemoryContextAlloc(stmt_mcontext, nargs * sizeof(Datum));
	ppd->nulls = (char *)
		MemoryContextAlloc(stmt_mcontext, nargs * sizeof(char));

	i = 0;
	foreach(lc, params)
	{
		PLTSQL_expr *param = (PLTSQL_expr *) lfirst(lc);
		bool		isnull;
		int32		ppdtypmod;
		MemoryContext oldcontext;

		ppd->values[i] = exec_eval_expr(estate, param,
										&isnull,
										&ppd->types[i],
										&ppdtypmod);
		ppd->nulls[i] = isnull ? 'n' : ' ';

		oldcontext = MemoryContextSwitchTo(stmt_mcontext);

		if (ppd->types[i] == UNKNOWNOID)
		{
			/*
			 * Treat 'unknown' parameters as text, since that's what most
			 * people would expect. SPI_execute_with_args can coerce unknown
			 * constants in a more intelligent way, but not unknown Params.
			 * This code also takes care of copying into the right context.
			 * Note we assume 'unknown' has the representation of C-string.
			 */
			ppd->types[i] = TEXTOID;
			if (!isnull)
				ppd->values[i] = CStringGetTextDatum(DatumGetCString(ppd->values[i]));
		}
		/* pass-by-ref non null values must be copied into stmt_mcontext */
		else if (!isnull)
		{
			int16		typLen;
			bool		typByVal;

			get_typlenbyval(ppd->types[i], &typLen, &typByVal);
			if (!typByVal)
				ppd->values[i] = datumCopy(ppd->values[i], typByVal, typLen);
		}

		MemoryContextSwitchTo(oldcontext);

		exec_eval_cleanup(estate);

		i++;
	}

	return ppd;
}

/*
 * free_params_data --- pfree all pass-by-reference values used in USING clause
 */
static void
free_params_data(PreparedParamsData *ppd)
{
	int			i;

	for (i = 0; i < ppd->nargs; i++)
	{
		if (ppd->freevals[i])
			pfree(DatumGetPointer(ppd->values[i]));
	}

	pfree(ppd->types);
	pfree(ppd->values);
	pfree(ppd->nulls);
	pfree(ppd->freevals);

	pfree(ppd);
}

/*
 * Open portal for dynamic query
 *
 * Caution: this resets the stmt_mcontext at exit.  We might eventually need
 * to move that responsibility to the callers, but currently no caller needs
 * to have statement-lifetime temp data that survives past this, so it's
 * simpler to do it here.
 */
static Portal
exec_dynquery_with_params(PLTSQL_execstate *estate,
						  PLTSQL_expr *dynquery,
						  List *params,
						  const char *portalname,
						  int cursorOptions)
{
	Portal		portal;
	Datum		query;
	bool		isnull;
	Oid			restype;
	int32		restypmod;
	char	   *querystr;
	MemoryContext stmt_mcontext = get_stmt_mcontext(estate);

	/*
	 * Evaluate the string expression after the EXECUTE keyword. Its result is
	 * the querystring we have to execute.
	 */
	query = exec_eval_expr(estate, dynquery, &isnull, &restype, &restypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("query string argument of EXECUTE is null")));

	/* Get the C-String representation */
	querystr = convert_value_to_string(estate, query, restype);

	/* copy it into the stmt_mcontext before we clean up */
	querystr = MemoryContextStrdup(stmt_mcontext, querystr);

	exec_eval_cleanup(estate);

	/*
	 * Open an implicit cursor for the query.  We use
	 * SPI_cursor_open_with_args even when there are no params, because this
	 * avoids making and freeing one copy of the plan.
	 */
	if (params)
	{
		PreparedParamsData *ppd;

		ppd = exec_eval_using_params(estate, params);
		portal = SPI_cursor_open_with_args(portalname,
										   querystr,
										   ppd->nargs, ppd->types,
										   ppd->values, ppd->nulls,
										   estate->readonly_func,
										   cursorOptions);
	}
	else
	{
		portal = SPI_cursor_open_with_args(portalname,
										   querystr,
										   0, NULL,
										   NULL, NULL,
										   estate->readonly_func,
										   cursorOptions);
	}

	if (portal == NULL)
		elog(ERROR, "could not open implicit cursor for query \"%s\": %s",
			 querystr, SPI_result_code_string(SPI_result));

	/* Release transient data */
	MemoryContextReset(stmt_mcontext);

	return portal;
}

static char *
transform_tsql_temp_tables(char * dynstmt)
{
	StringInfoData ds;
	char		   *cp;
	char		   *word;
	char		   *prev_word;

	initStringInfo(&ds);
	prev_word = NULL;

	for (cp = dynstmt; *cp; cp++)
	{
		if (cp[0] == '#' && is_char_identstart(cp[1]))
		{
			/*
			 * Quote this local temporary table identifier.  next_word stops as
			 * soon as it encounters a non-ident character such as '#', we point
			 * it to the next character as the start of word while specifying
			 * the '#' prefix explicitly in the format string.
			 */
			word = next_word(cp+1);
			appendStringInfo(&ds, "\"#%s\"", word);
			cp += strlen(word);
		}
		else if (is_char_identstart(cp[0]))
		{
			word = next_word(cp);
			cp += (strlen(word) - 1);

			/* CREATE TABLE #<ident> -> CREATE TEMPORARY TABLE #<ident> */
			if ((prev_word && (pg_strcasecmp(prev_word, "CREATE") == 0)) &&
			    (pg_strcasecmp(word, "TABLE") == 0) &&
				is_next_temptbl(cp))
			{
				appendStringInfo(&ds, "TEMPORARY %s", word);
			}
			else
				appendStringInfoString(&ds, word);

			prev_word = word;
		}
		else
			appendStringInfoChar(&ds, *cp);
	}

	return ds.data;
}

static char *
next_word(char *dyntext)
{
	StringInfoData ds;
	initStringInfo(&ds);

	while (*dyntext && is_char_identpart(*dyntext))
		appendStringInfoChar(&ds, *(dyntext++));

	return ds.data;
}

static bool
is_next_temptbl(char *dyntext)
{
	while (*++dyntext && scanner_isspace(*dyntext)); /* skip whitespace */

	return (dyntext[0] == '#' && is_char_identstart(dyntext[1]));
}

static bool
is_char_identstart(char c)
{
	return ((c == '_')             ||
			(c >= 'A' && c <= 'Z') ||
	        (c >= 'a' && c <= 'z') ||
	        (c >= '\200' && c <= '\377'));
}

static bool
is_char_identpart(char c)
{
	return ((is_char_identstart(c)) ||
	        (c >= '0' && c <= '9'));
}
