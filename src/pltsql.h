/*----------------------------------------------------------------------------------
 *
 * pltsql.h	Definitions for the PL/TSQL  procedural language
 *
 * Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pltsql.h
 *
 *----------------------------------------------------------------------------------
 */

#ifndef PLTSQL_H
#define PLTSQL_H

#include "postgres.h"

#include "access/xact.h"
#include "commands/trigger.h"
#include "executor/spi.h"

/**********************************************************************
 * Definitions
 **********************************************************************/

/* define our text domain for translations */
#undef TEXTDOMAIN
#define TEXTDOMAIN PG_TEXTDOMAIN("pltsql")

#undef _
#define _(x) dgettext(TEXTDOMAIN, x)

/* ----------
 * Compiler's namespace item types
 * ----------
 */
enum
{
	PLTSQL_NSTYPE_LABEL,
	PLTSQL_NSTYPE_VAR,
	PLTSQL_NSTYPE_ROW,
	PLTSQL_NSTYPE_REC
};

/* ----------
 * Datum array node types
 * ----------
 */
enum
{
	PLTSQL_DTYPE_VAR,
	PLTSQL_DTYPE_ROW,
	PLTSQL_DTYPE_REC,
	PLTSQL_DTYPE_RECFIELD,
	PLTSQL_DTYPE_ARRAYELEM,
	PLTSQL_DTYPE_EXPR
};

/* ----------
 * Variants distinguished in PLTSQL_type structs
 * ----------
 */
enum
{
	PLTSQL_TTYPE_SCALAR,		/* scalar types and domains */
	PLTSQL_TTYPE_ROW,			/* composite types */
	PLTSQL_TTYPE_REC,			/* RECORD pseudotype */
	PLTSQL_TTYPE_PSEUDO		/* other pseudotypes */
};

/* ----------
 * Execution tree node types
 * ----------
 */
enum PLTSQL_stmt_types
{
	PLTSQL_STMT_BLOCK,
	PLTSQL_STMT_ASSIGN,
	PLTSQL_STMT_IF,
	PLTSQL_STMT_CASE,
	PLTSQL_STMT_LOOP,
	PLTSQL_STMT_WHILE,
	PLTSQL_STMT_FORI,
	PLTSQL_STMT_FORS,
	PLTSQL_STMT_FORC,
	PLTSQL_STMT_FOREACH_A,
	PLTSQL_STMT_EXIT,
	PLTSQL_STMT_RETURN,
	PLTSQL_STMT_RETURN_NEXT,
	PLTSQL_STMT_RETURN_QUERY,
	PLTSQL_STMT_RAISE,
	PLTSQL_STMT_EXECSQL,
	PLTSQL_STMT_DYNEXECUTE,
	PLTSQL_STMT_DYNFORS,
	PLTSQL_STMT_GETDIAG,
	PLTSQL_STMT_OPEN,
	PLTSQL_STMT_FETCH,
	PLTSQL_STMT_CLOSE,
	PLTSQL_STMT_PERFORM
};


/* ----------
 * Execution node return codes
 * ----------
 */
enum
{
	PLTSQL_RC_OK,
	PLTSQL_RC_EXIT,
	PLTSQL_RC_RETURN,
	PLTSQL_RC_CONTINUE
};

/* ----------
 * GET DIAGNOSTICS information items
 * ----------
 */
enum
{
	PLTSQL_GETDIAG_ROW_COUNT,
	PLTSQL_GETDIAG_RESULT_OID,
	PLTSQL_GETDIAG_ERROR_CONTEXT,
	PLTSQL_GETDIAG_ERROR_DETAIL,
	PLTSQL_GETDIAG_ERROR_HINT,
	PLTSQL_GETDIAG_RETURNED_SQLSTATE,
	PLTSQL_GETDIAG_MESSAGE_TEXT
};

/* --------
 * RAISE statement options
 * --------
 */
enum
{
	PLTSQL_RAISEOPTION_ERRCODE,
	PLTSQL_RAISEOPTION_MESSAGE,
	PLTSQL_RAISEOPTION_DETAIL,
	PLTSQL_RAISEOPTION_HINT
};

/* --------
 * Behavioral modes for pltsql variable resolution
 * --------
 */
typedef enum
{
	PLTSQL_RESOLVE_ERROR,		/* throw error if ambiguous */
	PLTSQL_RESOLVE_VARIABLE,	/* prefer pltsql var to table column */
	PLTSQL_RESOLVE_COLUMN		/* prefer table column to pltsql var */
} PLTSQL_resolve_option;


/**********************************************************************
 * Node and structure definitions
 **********************************************************************/


typedef struct
{								/* Postgres data type */
	char	   *typname;		/* (simple) name of the type */
	Oid			typoid;			/* OID of the data type */
	int			ttype;			/* PLTSQL_TTYPE_ code */
	int16		typlen;			/* stuff copied from its pg_type entry */
	bool		typbyval;
	Oid			typrelid;
	Oid			typioparam;
	Oid			collation;		/* from pg_type, but can be overridden */
	FmgrInfo	typinput;		/* lookup info for typinput function */
	int32		atttypmod;		/* typmod (taken from someplace else) */
} PLTSQL_type;


/*
 * PLTSQL_datum is the common supertype for PLTSQL_expr, PLTSQL_var,
 * PLTSQL_row, PLTSQL_rec, PLTSQL_recfield, and PLTSQL_arrayelem
 */
typedef struct
{								/* Generic datum array item		*/
	int			dtype;
	int			dno;
} PLTSQL_datum;

/*
 * The variants PLTSQL_var, PLTSQL_row, and PLTSQL_rec share these
 * fields
 */
typedef struct
{								/* Scalar or composite variable */
	int			dtype;
	int			dno;
	char	   *refname;
	int			lineno;
} PLTSQL_variable;

typedef struct PLTSQL_expr
{								/* SQL Query to plan and execute	*/
	int			dtype;
	int			dno;
	char	   *query;
	SPIPlanPtr	plan;
	Bitmapset  *paramnos;		/* all dnos referenced by this query */

	/* function containing this expr (not set until we first parse query) */
	struct PLTSQL_function *func;

	/* namespace chain visible to this expr */
	struct PLTSQL_nsitem *ns;

	/* fields for "simple expression" fast-path execution: */
	Expr	   *expr_simple_expr;		/* NULL means not a simple expr */
	int			expr_simple_generation; /* plancache generation we checked */
	Oid			expr_simple_type;		/* result type Oid, if simple */

	/*
	 * if expr is simple AND prepared in current transaction,
	 * expr_simple_state and expr_simple_in_use are valid. Test validity by
	 * seeing if expr_simple_lxid matches current LXID.  (If not,
	 * expr_simple_state probably points at garbage!)
	 */
	ExprState  *expr_simple_state;		/* eval tree for expr_simple_expr */
	bool		expr_simple_in_use;		/* true if eval tree is active */
	LocalTransactionId expr_simple_lxid;
} PLTSQL_expr;


typedef struct
{								/* Scalar variable */
	int			dtype;
	int			dno;
	char	   *refname;
	int			lineno;

	PLTSQL_type *datatype;
	int			isconst;
	int			notnull;
	PLTSQL_expr *default_val;
	PLTSQL_expr *cursor_explicit_expr;
	int			cursor_explicit_argrow;
	int			cursor_options;

	Datum		value;
	bool		isnull;
	bool		freeval;
} PLTSQL_var;


typedef struct
{								/* Row variable */
	int			dtype;
	int			dno;
	char	   *refname;
	int			lineno;

	TupleDesc	rowtupdesc;

	/*
	 * Note: TupleDesc is only set up for named rowtypes, else it is NULL.
	 *
	 * Note: if the underlying rowtype contains a dropped column, the
	 * corresponding fieldnames[] entry will be NULL, and there is no
	 * corresponding var (varnos[] will be -1).
	 */
	int			nfields;
	char	  **fieldnames;
	int		   *varnos;
} PLTSQL_row;


typedef struct
{								/* Record variable (non-fixed structure) */
	int			dtype;
	int			dno;
	char	   *refname;
	int			lineno;

	HeapTuple	tup;
	TupleDesc	tupdesc;
	bool		freetup;
	bool		freetupdesc;
} PLTSQL_rec;


typedef struct
{								/* Field in record */
	int			dtype;
	int			dno;
	char	   *fieldname;
	int			recparentno;	/* dno of parent record */
} PLTSQL_recfield;


typedef struct
{								/* Element of array variable */
	int			dtype;
	int			dno;
	PLTSQL_expr *subscript;
	int			arrayparentno;	/* dno of parent array variable */
	/* Remaining fields are cached info about the array variable's type */
	Oid			parenttypoid;	/* type of array variable; 0 if not yet set */
	int32		parenttypmod;	/* typmod of array variable */
	Oid			arraytypoid;	/* OID of actual array type */
	int32		arraytypmod;	/* typmod of array (and its elements too) */
	int16		arraytyplen;	/* typlen of array type */
	Oid			elemtypoid;		/* OID of array element type */
	int16		elemtyplen;		/* typlen of element type */
	bool		elemtypbyval;	/* element type is pass-by-value? */
	char		elemtypalign;	/* typalign of element type */
} PLTSQL_arrayelem;


typedef struct PLTSQL_nsitem
{								/* Item in the compilers namespace tree */
	int			itemtype;
	int			itemno;
	struct PLTSQL_nsitem *prev;
	char		name[1];		/* actually, as long as needed */
} PLTSQL_nsitem;


typedef struct
{								/* Generic execution node		*/
	int			cmd_type;
	int			lineno;
} PLTSQL_stmt;


typedef struct PLTSQL_condition
{								/* One EXCEPTION condition name */
	int			sqlerrstate;	/* SQLSTATE code */
	char	   *condname;		/* condition name (for debugging) */
	struct PLTSQL_condition *next;
} PLTSQL_condition;

typedef struct
{
	int			sqlstate_varno;
	int			sqlerrm_varno;
	List	   *exc_list;		/* List of WHEN clauses */
} PLTSQL_exception_block;

typedef struct
{								/* One EXCEPTION ... WHEN clause */
	int			lineno;
	PLTSQL_condition *conditions;
	List	   *action;			/* List of statements */
} PLTSQL_exception;


typedef struct
{								/* Block of statements			*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	List	   *body;			/* List of statements */
	int			n_initvars;
	int		   *initvarnos;
	PLTSQL_exception_block *exceptions;
} PLTSQL_stmt_block;


typedef struct
{								/* Assign statement			*/
	int			cmd_type;
	int			lineno;
	int			varno;
	PLTSQL_expr *expr;
} PLTSQL_stmt_assign;

typedef struct
{								/* PERFORM statement		*/
	int			cmd_type;
	int			lineno;
	PLTSQL_expr *expr;
} PLTSQL_stmt_perform;

typedef struct
{								/* Get Diagnostics item		*/
	int			kind;			/* id for diagnostic value desired */
	int			target;			/* where to assign it */
} PLTSQL_diag_item;

typedef struct
{								/* Get Diagnostics statement		*/
	int			cmd_type;
	int			lineno;
	bool		is_stacked;		/* STACKED or CURRENT diagnostics area? */
	List	   *diag_items;		/* List of PLTSQL_diag_item */
} PLTSQL_stmt_getdiag;


typedef struct
{								/* IF statement				*/
	int			cmd_type;
	int			lineno;
	PLTSQL_expr *cond;			/* boolean expression for THEN */
	List	   *then_body;		/* List of statements */
	List	   *elsif_list;		/* List of PLTSQL_if_elsif structs */
	List	   *else_body;		/* List of statements */
} PLTSQL_stmt_if;

typedef struct					/* one ELSIF arm of IF statement */
{
	int			lineno;
	PLTSQL_expr *cond;			/* boolean expression for this case */
	List	   *stmts;			/* List of statements */
} PLTSQL_if_elsif;


typedef struct					/* CASE statement */
{
	int			cmd_type;
	int			lineno;
	PLTSQL_expr *t_expr;		/* test expression, or NULL if none */
	int			t_varno;		/* var to store test expression value into */
	List	   *case_when_list; /* List of PLTSQL_case_when structs */
	bool		have_else;		/* flag needed because list could be empty */
	List	   *else_stmts;		/* List of statements */
} PLTSQL_stmt_case;

typedef struct					/* one arm of CASE statement */
{
	int			lineno;
	PLTSQL_expr *expr;			/* boolean expression for this case */
	List	   *stmts;			/* List of statements */
} PLTSQL_case_when;


typedef struct
{								/* Unconditional LOOP statement		*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	List	   *body;			/* List of statements */
} PLTSQL_stmt_loop;


typedef struct
{								/* WHILE cond LOOP statement		*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLTSQL_expr *cond;
	List	   *body;			/* List of statements */
} PLTSQL_stmt_while;


typedef struct
{								/* FOR statement with integer loopvar	*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLTSQL_var *var;
	PLTSQL_expr *lower;
	PLTSQL_expr *upper;
	PLTSQL_expr *step;			/* NULL means default (ie, BY 1) */
	int			reverse;
	List	   *body;			/* List of statements */
} PLTSQL_stmt_fori;


/*
 * PLTSQL_stmt_forq represents a FOR statement running over a SQL query.
 * It is the common supertype of PLTSQL_stmt_fors, PLTSQL_stmt_forc
 * and PLTSQL_dynfors.
 */
typedef struct
{
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLTSQL_rec *rec;
	PLTSQL_row *row;
	List	   *body;			/* List of statements */
} PLTSQL_stmt_forq;

typedef struct
{								/* FOR statement running over SELECT	*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLTSQL_rec *rec;
	PLTSQL_row *row;
	List	   *body;			/* List of statements */
	/* end of fields that must match PLTSQL_stmt_forq */
	PLTSQL_expr *query;
} PLTSQL_stmt_fors;

typedef struct
{								/* FOR statement running over cursor	*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLTSQL_rec *rec;
	PLTSQL_row *row;
	List	   *body;			/* List of statements */
	/* end of fields that must match PLTSQL_stmt_forq */
	int			curvar;
	PLTSQL_expr *argquery;		/* cursor arguments if any */
} PLTSQL_stmt_forc;

typedef struct
{								/* FOR statement running over EXECUTE	*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLTSQL_rec *rec;
	PLTSQL_row *row;
	List	   *body;			/* List of statements */
	/* end of fields that must match PLTSQL_stmt_forq */
	PLTSQL_expr *query;
	List	   *params;			/* USING expressions */
} PLTSQL_stmt_dynfors;


typedef struct
{								/* FOREACH item in array loop */
	int			cmd_type;
	int			lineno;
	char	   *label;
	int			varno;			/* loop target variable */
	int			slice;			/* slice dimension, or 0 */
	PLTSQL_expr *expr;			/* array expression */
	List	   *body;			/* List of statements */
} PLTSQL_stmt_foreach_a;


typedef struct
{								/* OPEN a curvar					*/
	int			cmd_type;
	int			lineno;
	int			curvar;
	int			cursor_options;
	PLTSQL_row *returntype;
	PLTSQL_expr *argquery;
	PLTSQL_expr *query;
	PLTSQL_expr *dynquery;
	List	   *params;			/* USING expressions */
} PLTSQL_stmt_open;


typedef struct
{								/* FETCH or MOVE statement */
	int			cmd_type;
	int			lineno;
	PLTSQL_rec *rec;			/* target, as record or row */
	PLTSQL_row *row;
	int			curvar;			/* cursor variable to fetch from */
	FetchDirection direction;	/* fetch direction */
	long		how_many;		/* count, if constant (expr is NULL) */
	PLTSQL_expr *expr;			/* count, if expression */
	bool		is_move;		/* is this a fetch or move? */
	bool		returns_multiple_rows;	/* can return more than one row? */
} PLTSQL_stmt_fetch;


typedef struct
{								/* CLOSE curvar						*/
	int			cmd_type;
	int			lineno;
	int			curvar;
} PLTSQL_stmt_close;


typedef struct
{								/* EXIT or CONTINUE statement			*/
	int			cmd_type;
	int			lineno;
	bool		is_exit;		/* Is this an exit or a continue? */
	char	   *label;			/* NULL if it's an unlabelled EXIT/CONTINUE */
	PLTSQL_expr *cond;
} PLTSQL_stmt_exit;


typedef struct
{								/* RETURN statement			*/
	int			cmd_type;
	int			lineno;
	PLTSQL_expr *expr;
	int			retvarno;
} PLTSQL_stmt_return;

typedef struct
{								/* RETURN NEXT statement */
	int			cmd_type;
	int			lineno;
	PLTSQL_expr *expr;
	int			retvarno;
} PLTSQL_stmt_return_next;

typedef struct
{								/* RETURN QUERY statement */
	int			cmd_type;
	int			lineno;
	PLTSQL_expr *query;		/* if static query */
	PLTSQL_expr *dynquery;		/* if dynamic query (RETURN QUERY EXECUTE) */
	List	   *params;			/* USING arguments for dynamic query */
} PLTSQL_stmt_return_query;

typedef struct
{								/* RAISE statement			*/
	int			cmd_type;
	int			lineno;
	int			elog_level;
	char	   *condname;		/* condition name, SQLSTATE, or NULL */
	char	   *message;		/* old-style message format literal, or NULL */
	List	   *params;			/* list of expressions for old-style message */
	List	   *options;		/* list of PLTSQL_raise_option */
} PLTSQL_stmt_raise;

typedef struct
{								/* RAISE statement option */
	int			opt_type;
	PLTSQL_expr *expr;
} PLTSQL_raise_option;


typedef struct
{								/* Generic SQL statement to execute */
	int			cmd_type;
	int			lineno;
	PLTSQL_expr *sqlstmt;
	bool		mod_stmt;		/* is the stmt INSERT/UPDATE/DELETE? */
	/* note: mod_stmt is set when we plan the query */
	bool		into;			/* INTO supplied? */
	bool		strict;			/* INTO STRICT flag */
	PLTSQL_rec *rec;			/* INTO target, if record */
	PLTSQL_row *row;			/* INTO target, if row */
} PLTSQL_stmt_execsql;


typedef struct
{								/* Dynamic SQL string to execute */
	int			cmd_type;
	int			lineno;
	PLTSQL_expr *query;		/* string expression */
	bool		into;			/* INTO supplied? */
	bool		strict;			/* INTO STRICT flag */
	PLTSQL_rec *rec;			/* INTO target, if record */
	PLTSQL_row *row;			/* INTO target, if row */
	List	   *params;			/* USING expressions */
} PLTSQL_stmt_dynexecute;


typedef struct PLTSQL_func_hashkey
{								/* Hash lookup key for functions */
	Oid			funcOid;

	bool		isTrigger;		/* true if called as a trigger */

	/* be careful that pad bytes in this struct get zeroed! */

	/*
	 * For a trigger function, the OID of the relation triggered on is part of
	 * the hash key --- we want to compile the trigger separately for each
	 * relation it is used with, in case the rowtype is different.	Zero if
	 * not called as a trigger.
	 */
	Oid			trigrelOid;

	/*
	 * We must include the input collation as part of the hash key too,
	 * because we have to generate different plans (with different Param
	 * collations) for different collation settings.
	 */
	Oid			inputCollation;

	/*
	 * We include actual argument types in the hash key to support polymorphic
	 * PLTSQL functions.  Be careful that extra positions are zeroed!
	 */
	Oid			argtypes[FUNC_MAX_ARGS];
} PLTSQL_func_hashkey;


typedef struct PLTSQL_function
{								/* Complete compiled function	  */
	char	   *fn_signature;
	Oid			fn_oid;
	TransactionId fn_xmin;
	ItemPointerData fn_tid;
	bool		fn_is_trigger;
	Oid			fn_input_collation;
	PLTSQL_func_hashkey *fn_hashkey;	/* back-link to hashtable key */
	MemoryContext fn_cxt;

	Oid			fn_rettype;
	int			fn_rettyplen;
	bool		fn_retbyval;
	FmgrInfo	fn_retinput;
	Oid			fn_rettypioparam;
	bool		fn_retistuple;
	bool		fn_retset;
	bool		fn_readonly;

	int			fn_nargs;
	int			fn_argvarnos[FUNC_MAX_ARGS];
	int			out_param_varno;
	int			found_varno;
	int			new_varno;
	int			old_varno;
	int			tg_name_varno;
	int			tg_when_varno;
	int			tg_level_varno;
	int			tg_op_varno;
	int			tg_relid_varno;
	int			tg_relname_varno;
	int			tg_table_name_varno;
	int			tg_table_schema_varno;
	int			tg_nargs_varno;
	int			tg_argv_varno;

	PLTSQL_resolve_option resolve_option;

	int			ndatums;
	PLTSQL_datum **datums;
	PLTSQL_stmt_block *action;

	/* these fields change when the function is used */
	struct PLTSQL_execstate *cur_estate;
	unsigned long use_count;
} PLTSQL_function;


typedef struct PLTSQL_execstate
{								/* Runtime execution data	*/
	PLTSQL_function *func;		/* function being executed */

	Datum		retval;
	bool		retisnull;
	Oid			rettype;		/* type of current retval */

	Oid			fn_rettype;		/* info about declared function rettype */
	bool		retistuple;
	bool		retisset;

	bool		readonly_func;

	TupleDesc	rettupdesc;
	char	   *exitlabel;		/* the "target" label of the current EXIT or
								 * CONTINUE stmt, if any */
	ErrorData  *cur_error;		/* current exception handler's error */

	Tuplestorestate *tuple_store;		/* SRFs accumulate results here */
	MemoryContext tuple_store_cxt;
	ResourceOwner tuple_store_owner;
	ReturnSetInfo *rsi;

	int			found_varno;
	int			ndatums;
	PLTSQL_datum **datums;

	/* temporary state for results from evaluation of query or expr */
	SPITupleTable *eval_tuptable;
	uint32		eval_processed;
	Oid			eval_lastoid;
	ExprContext *eval_econtext; /* for executing simple expressions */
	PLTSQL_expr *cur_expr;		/* current query/expr being evaluated */

	/* status information for error context reporting */
	PLTSQL_stmt *err_stmt;		/* current stmt */
	const char *err_text;		/* additional state info */

	void	   *plugin_info;	/* reserved for use by optional plugin */
} PLTSQL_execstate;


/*
 * A PLTSQL_plugin structure represents an instrumentation plugin.
 * To instrument PL/TSQL, a plugin library must access the rendezvous
 * variable "PLTSQL_plugin" and set it to point to a PLTSQL_plugin struct.
 * Typically the struct could just be static data in the plugin library.
 * We expect that a plugin would do this at library load time (_PG_init()).
 * It must also be careful to set the rendezvous variable back to NULL
 * if it is unloaded (_PG_fini()).
 *
 * This structure is basically a collection of function pointers --- at
 * various interesting points in pl_exec.c, we call these functions
 * (if the pointers are non-NULL) to give the plugin a chance to watch
 * what we are doing.
 *
 *	func_setup is called when we start a function, before we've initialized
 *	the local variables defined by the function.
 *
 *	func_beg is called when we start a function, after we've initialized
 *	the local variables.
 *
 *	func_end is called at the end of a function.
 *
 *	stmt_beg and stmt_end are called before and after (respectively) each
 *	statement.
 *
 * Also, immediately before any call to func_setup, PL/TSQL fills in the
 * error_callback and assign_expr fields with pointers to its own
 * pltsql_exec_error_callback and exec_assign_expr functions.	This is
 * a somewhat ad-hoc expedient to simplify life for debugger plugins.
 */

typedef struct
{
	/* Function pointers set up by the plugin */
	void		(*func_setup) (PLTSQL_execstate *estate, PLTSQL_function *func);
	void		(*func_beg) (PLTSQL_execstate *estate, PLTSQL_function *func);
	void		(*func_end) (PLTSQL_execstate *estate, PLTSQL_function *func);
	void		(*stmt_beg) (PLTSQL_execstate *estate, PLTSQL_stmt *stmt);
	void		(*stmt_end) (PLTSQL_execstate *estate, PLTSQL_stmt *stmt);

	/* Function pointers set by PL/TSQL itself */
	void		(*error_callback) (void *arg);
	void		(*assign_expr) (PLTSQL_execstate *estate, PLTSQL_datum *target,
											PLTSQL_expr *expr);
} PLTSQL_plugin;


/* Struct types used during parsing */

typedef struct
{
	char	   *ident;			/* palloc'd converted identifier */
	bool		quoted;			/* Was it double-quoted? */
} PLword;

typedef struct
{
	List	   *idents;			/* composite identifiers (list of String) */
} PLcword;

typedef struct
{
	PLTSQL_datum *datum;		/* referenced variable */
	char	   *ident;			/* valid if simple name */
	bool		quoted;
	List	   *idents;			/* valid if composite name */
} PLwdatum;

/**********************************************************************
 * Global variable declarations
 **********************************************************************/

typedef enum
{
	IDENTIFIER_LOOKUP_NORMAL,	/* normal processing of var names */
	IDENTIFIER_LOOKUP_DECLARE,	/* In DECLARE --- don't look up names */
	IDENTIFIER_LOOKUP_EXPR		/* In SQL expression --- special case */
} IdentifierLookup;

extern IdentifierLookup pltsql_IdentifierLookup;

extern int	pltsql_variable_conflict;

extern bool pltsql_check_syntax;
extern bool pltsql_DumpExecTree;

extern PLTSQL_stmt_block *pltsql_parse_result;

extern int	pltsql_nDatums;
extern PLTSQL_datum **pltsql_Datums;

extern char *pltsql_error_funcname;

extern PLTSQL_function *pltsql_curr_compile;
extern MemoryContext compile_tmp_cxt;

extern PLTSQL_plugin **plugin_ptr;

/**********************************************************************
 * Function declarations
 **********************************************************************/

/* ----------
 * Functions in pl_comp.c
 * ----------
 */
extern PLTSQL_function *pltsql_compile(FunctionCallInfo fcinfo,
				bool forValidator);
extern PLTSQL_function *pltsql_compile_inline(char *proc_source);
extern void pltsql_parser_setup(struct ParseState *pstate,
					 PLTSQL_expr *expr);
extern bool pltsql_parse_word(char *word1, const char *yytxt,
				   PLwdatum *wdatum, PLword *word);
extern bool pltsql_parse_dblword(char *word1, char *word2,
					  PLwdatum *wdatum, PLcword *cword);
extern bool pltsql_parse_tripword(char *word1, char *word2, char *word3,
					   PLwdatum *wdatum, PLcword *cword);
extern PLTSQL_type *pltsql_parse_wordtype(char *ident);
extern PLTSQL_type *pltsql_parse_cwordtype(List *idents);
extern PLTSQL_type *pltsql_parse_wordrowtype(char *ident);
extern PLTSQL_type *pltsql_parse_cwordrowtype(List *idents);
extern PLTSQL_type *pltsql_build_datatype(Oid typeOid, int32 typmod,
					   Oid collation);
extern PLTSQL_variable *pltsql_build_variable(const char *refname, int lineno,
					   PLTSQL_type *dtype,
					   bool add2namespace);
extern PLTSQL_rec *pltsql_build_record(const char *refname, int lineno,
					 bool add2namespace);
extern int pltsql_recognize_err_condition(const char *condname,
								bool allow_sqlstate);
extern PLTSQL_condition *pltsql_parse_err_condition(char *condname);
extern void pltsql_adddatum(PLTSQL_datum *new);
extern int	pltsql_add_initdatums(int **varnos);
extern void pltsql_HashTableInit(void);

/* ----------
 * Functions in pl_handler.c
 * ----------
 */
extern void _PG_init(void);
extern Datum pltsql_call_handler(PG_FUNCTION_ARGS);
extern Datum pltsql_inline_handler(PG_FUNCTION_ARGS);
extern Datum pltsql_validator(PG_FUNCTION_ARGS);

/* ----------
 * Functions in pl_exec.c
 * ----------
 */
extern Datum pltsql_exec_function(PLTSQL_function *func,
					  FunctionCallInfo fcinfo);
extern HeapTuple pltsql_exec_trigger(PLTSQL_function *func,
					 TriggerData *trigdata);
extern void pltsql_xact_cb(XactEvent event, void *arg);
extern void pltsql_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
				   SubTransactionId parentSubid, void *arg);
extern Oid exec_get_datum_type(PLTSQL_execstate *estate,
					PLTSQL_datum *datum);
extern void exec_get_datum_type_info(PLTSQL_execstate *estate,
						 PLTSQL_datum *datum,
						 Oid *typeid, int32 *typmod, Oid *collation);

/* ----------
 * Functions for namespace handling in pl_funcs.c
 * ----------
 */
extern void pltsql_ns_init(void);
extern void pltsql_ns_push(const char *label);
extern void pltsql_ns_pop(void);
extern PLTSQL_nsitem *pltsql_ns_top(void);
extern void pltsql_ns_additem(int itemtype, int itemno, const char *name);
extern PLTSQL_nsitem *pltsql_ns_lookup(PLTSQL_nsitem *ns_cur, bool localmode,
				  const char *name1, const char *name2,
				  const char *name3, int *names_used);
extern PLTSQL_nsitem *pltsql_ns_lookup_label(PLTSQL_nsitem *ns_cur,
						const char *name);

/* ----------
 * Other functions in pl_funcs.c
 * ----------
 */
extern const char *pltsql_stmt_typename(PLTSQL_stmt *stmt);
extern const char *pltsql_getdiag_kindname(int kind);
extern void pltsql_free_function_memory(PLTSQL_function *func);
extern void pltsql_dumptree(PLTSQL_function *func);

/* ----------
 * Scanner functions in pl_scanner.c
 * ----------
 */
extern int	pltsql_base_yylex(void);
extern int	pltsql_yylex(void);
extern void pltsql_push_back_token(int token);
extern void pltsql_append_source_text(StringInfo buf,
						   int startlocation, int endlocation);
extern void pltsql_peek2(int *tok1_p, int *tok2_p, int *tok1_loc,
			  int *tok2_loc);
extern int	pltsql_scanner_errposition(int location);
extern void pltsql_yyerror(const char *message);
extern int	pltsql_location_to_lineno(int location);
extern int	pltsql_latest_lineno(void);
extern void pltsql_scanner_init(const char *str);
extern void pltsql_scanner_finish(void);

/* ----------
 * Externs in gram.y
 * ----------
 */
extern int	pltsql_yyparse(void);

#endif   /* PLTSQL_H */
