%{
/*----------------------------------------------------------------------------------
 *
 * gram.y	Parser for the PL/TSQL procedural language
 *
 * Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gram.y
 *
 *-----------------------------------------------------------------------------------
 */

#include "pltsql.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "utils/builtins.h"


/* Location tracking support --- simpler than bison's default */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if (N) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (Rhs)[0]; \
	} while (0)

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

#define TEMPOBJ_QUALIFIER "TEMPORARY "

typedef struct
{
	int			location;
	int			leaderlen;
} sql_error_callback_arg;

typedef struct
{
	int			location;
	char		*ident;
} tsql_ident_ref;

#define parser_errposition(pos)  pltsql_scanner_errposition(pos)

union YYSTYPE;					/* need forward reference for tok_is_keyword */

static	bool			tok_is_keyword(int token, union YYSTYPE *lval,
									   int kw_token, const char *kw_str);
static	void			word_is_not_variable(PLword *word, int location);
static	void			cword_is_not_variable(PLcword *cword, int location);
static	void			current_token_is_not_variable(int tok);
static	char *			quote_tsql_identifiers(const StringInfo src,
											const List *tsql_word_list);
static List * append_if_tsql_identifier(int tok, int start_len, int start_loc,
											List *tsql_idents);
static	PLTSQL_expr	*read_sql_construct(int until,
											int until2,
											int until3,
											const char *expected,
											const char *sqlstart,
											bool isexpression,
											bool valid_sql,
											bool trim,
											int *startloc,
											int *endtoken);
static	PLTSQL_expr	*read_sql_construct_bos(int until,
											int until2,
											int until3,
											const char *expected,
											const char *sqlstart,
											bool isexpression,
											bool valid_sql,
											bool trim,
											int *startloc,
											int *endtoken,
											bool untilbostok);
static	PLTSQL_expr	*read_sql_expression(int until,
											 const char *expected);
static	PLTSQL_expr	*read_sql_expression_bos(int until,
											 const char *expected);
static	PLTSQL_expr	*read_sql_expression2(int until, int until2,
											  const char *expected,
											  int *endtoken);
static PLTSQL_expr * read_sql_expression2_bos(int until, int until2,
											  const char *expected,
											  int *endtoken);
static bool is_terminator(int tok, bool first, int start_loc, int cur_loc,
                          const char *sql_start, const List *tsql_idents);
static bool word_matches(int tok, const char *pattern);
static	PLTSQL_expr	*read_sql_stmt(const char *sqlstart);
static	PLTSQL_expr	*read_sql_stmt_bos(const char *sqlstart);
static	PLTSQL_type	*read_datatype(int tok);
static	PLTSQL_stmt	*make_execsql_stmt(int firsttoken, int location,
										 PLword *firstword);
static	PLTSQL_stmt_fetch *read_fetch_direction(void);
static	void			 complete_direction(PLTSQL_stmt_fetch *fetch,
											bool *check_FROM);
static	PLTSQL_stmt	*make_return_stmt(int location);
static	PLTSQL_stmt	*make_return_next_stmt(int location);
static	PLTSQL_stmt	*make_return_query_stmt(int location);
static	char			*NameOfDatum(PLwdatum *wdatum);
static	void			 check_assignable(PLTSQL_datum *datum, int location);
static	void			 read_into_target(PLTSQL_variable **target,
										  bool *strict);
static	PLTSQL_row		*read_into_scalar_list(char *initial_name,
											   PLTSQL_datum *initial_datum,
											   int initial_location);
static	PLTSQL_row		*make_scalar_list1(char *initial_name,
										   PLTSQL_datum *initial_datum,
										   int lineno, int location);
static	void			 check_sql_expr(const char *stmt, int location,
										int leaderlen);
static	void			 pltsql_sql_error_callback(void *arg);
static	PLTSQL_type	*parse_datatype(const char *string, int location);
static	void			 check_labels(const char *start_label,
									  const char *end_label,
									  int end_location);
static	PLTSQL_expr	*read_cursor_args(PLTSQL_var *cursor,
										  int until, const char *expected);
static	List			*read_raise_options(void);

%}

%expect 0
%name-prefix="pltsql_yy"
%locations

%union {
		core_YYSTYPE			core_yystype;
		/* these fields must match core_YYSTYPE: */
		int						ival;
		char					*str;
		const char				*keyword;

		PLword					word;
		PLcword					cword;
		PLwdatum				wdatum;
		bool					boolean;
		Oid						oid;
		struct
		{
			char *name;
			int  lineno;
		}						varname;
		struct
		{
			char *name;
			int  lineno;
			PLTSQL_datum   *scalar;
			PLTSQL_rec		*rec;
			PLTSQL_row		*row;
		}						forvariable;
		struct
		{
			char *label;
			int  n_initvars;
			int  *initvarnos;
		}						declhdr;
		struct
		{
			List *stmts;
			char *end_label;
			int   end_label_location;
		}						loop_body;
		List					*list;
		PLTSQL_type			*dtype;
		PLTSQL_datum			*datum;
		PLTSQL_var				*var;
		PLTSQL_expr			*expr;
		PLTSQL_stmt			*stmt;
		PLTSQL_condition		*condition;
		PLTSQL_exception		*exception;
		PLTSQL_exception_block	*exception_block;
		PLTSQL_nsitem			*nsitem;
		PLTSQL_diag_item		*diagitem;
		PLTSQL_stmt_fetch		*fetch;
}

%type <declhdr> decl_sect
%type <varname> decl_varname
%type <boolean>	decl_const decl_notnull exit_type
%type <expr>	decl_defval decl_cursor_query
%type <dtype>	decl_datatype
%type <oid>		decl_collate
%type <datum>	decl_cursor_args
%type <list>	decl_cursor_arglist
%type <nsitem>	decl_aliasitem

%type <expr>	expr_until_semi expr_until_semi_or_bos expr_until_rightbracket
%type <expr>	expr_until_loop
%type <expr>	opt_exitcond expr_until_bos

%type <ival>	assign_var foreach_slice
%type <var>		cursor_variable
%type <datum>	decl_cursor_arg
%type <forvariable>	for_variable
%type <stmt>	for_control stmt_foreach_a

%type <str>		any_identifier opt_block_label opt_label

%type <list>	proc_sect proc_stmts
%type <loop_body>	loop_body
%type <stmt>	proc_stmt pl_block
%type <stmt>	stmt_assign stmt_if stmt_loop stmt_while stmt_exit
%type <stmt>	stmt_return stmt_raise stmt_execsql
%type <stmt>	stmt_dynexecute stmt_for stmt_perform stmt_getdiag
%type <stmt>	stmt_open stmt_fetch stmt_move stmt_close stmt_null
%type <stmt>	pltsql_only_stmt plpgsql_only_stmt common_stmt

%type <list>	proc_exceptions
%type <exception_block> exception_sect
%type <exception>	proc_exception
%type <condition>	proc_conditions proc_condition

%type <boolean>	getdiag_area_opt
%type <list>	getdiag_list
%type <diagitem> getdiag_list_item
%type <ival>	getdiag_item getdiag_target

%type <ival>	opt_scrollable
%type <fetch>	opt_fetch_direction

%type <keyword>	unreserved_keyword
%type <stmt>	stmt_print

/*
 * Basic non-keyword token types.  These are hard-wired into the core lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  Keep this list in sync with backend/parser/gram.y!
 *
 * Some of these are not directly referenced in this file, but they must be
 * here anyway.
 */
%token <str>	IDENT FCONST SCONST BCONST XCONST Op
%token <ival>	ICONST PARAM
%token			TYPECAST DOT_DOT COLON_EQUALS

/*
 * Other tokens recognized by pltsql's lexer interface layer (pl_scanner.c).
 */
%token <word>		T_WORD		/* unrecognized simple identifier */
%token <cword>		T_CWORD		/* unrecognized composite identifier */
%token <wdatum>		T_DATUM		/* a VAR, ROW, REC, or RECFIELD variable */
%token				LESS_LESS
%token				GREATER_GREATER

/*
 * Keyword tokens.  Some of these are reserved and some are not;
 * see pl_scanner.c for info.  Be sure unreserved keywords are listed
 * in the "unreserved_keyword" production below.
 */
%token <keyword>	K_ABSOLUTE
%token <keyword>	K_ALIAS
%token <keyword>	K_ALL
%token <keyword>	K_ARRAY
%token <keyword>	K_BACKWARD
%token <keyword>	K_BEGIN
%token <keyword>	K_BY
%token <keyword>	K_CASE
%token <keyword>	K_CLOSE
%token <keyword>	K_COLLATE
%token <keyword>	K_CONSTANT
%token <keyword>	K_CONTINUE
%token <keyword>	K_CURRENT
%token <keyword>	K_CURSOR
%token <keyword>	K_DEBUG
%token <keyword>	K_DECLARE
%token <keyword>	K_DEFAULT
%token <keyword>	K_DETAIL
%token <keyword>	K_DIAGNOSTICS
%token <keyword>	K_DUMP
%token <keyword>	K_ELSE
%token <keyword>	K_ELSIF
%token <keyword>	K_END
%token <keyword>	K_ERRCODE
%token <keyword>	K_ERROR
%token <keyword>	K_EXCEPTION
%token <keyword>	K_EXECUTE
%token <keyword>	K_EXIT
%token <keyword>	K_FETCH
%token <keyword>	K_FIRST
%token <keyword>	K_FOR
%token <keyword>	K_FOREACH
%token <keyword>	K_FORWARD
%token <keyword>	K_FROM
%token <keyword>	K_GET
%token <keyword>	K_HINT
%token <keyword>	K_IF
%token <keyword>	K_IN
%token <keyword>	K_INFO
%token <keyword>	K_INSERT
%token <keyword>	K_INTO
%token <keyword>	K_IS
%token <keyword>	K_LAST
%token <keyword>	K_LOG
%token <keyword>	K_LOOP
%token <keyword>	K_MESSAGE
%token <keyword>	K_MESSAGE_TEXT
%token <keyword>	K_MOVE
%token <keyword>	K_NEXT
%token <keyword>	K_NO
%token <keyword>	K_NOT
%token <keyword>	K_NOTICE
%token <keyword>	K_NULL
%token <keyword>	K_OPEN
%token <keyword>	K_OPTION
%token <keyword>	K_OR
%token <keyword>	K_PERFORM
%token <keyword>	K_PG_EXCEPTION_CONTEXT
%token <keyword>	K_PG_EXCEPTION_DETAIL
%token <keyword>	K_PG_EXCEPTION_HINT
%token <keyword>	K_PRIOR
%token <keyword>	K_QUERY
%token <keyword>	K_RAISE
%token <keyword>	K_RELATIVE
%token <keyword>	K_RESULT_OID
%token <keyword>	K_RETURN
%token <keyword>	K_RETURNED_SQLSTATE
%token <keyword>	K_REVERSE
%token <keyword>	K_ROWTYPE
%token <keyword>	K_ROW_COUNT
%token <keyword>	K_SCROLL
%token <keyword>	K_SLICE
%token <keyword>	K_SQLSTATE
%token <keyword>	K_STACKED
%token <keyword>	K_STRICT
%token <keyword>	K_THEN
%token <keyword>	K_TO
%token <keyword>	K_TYPE
%token <keyword>	K_USE_COLUMN
%token <keyword>	K_USE_VARIABLE
%token <keyword>	K_USING
%token <keyword>	K_VARIABLE_CONFLICT
%token <keyword>	K_WARNING
%token <keyword>	K_WHEN
%token <keyword>	K_WHILE
%token <keyword>	K_PRINT
%token <keyword>	K_SET

%nonassoc LOWER_THAN_ELSE
%nonassoc K_ELSE

%%

pl_function		: comp_options pl_block opt_semi
					{
						pltsql_parse_result = (PLTSQL_stmt_block *) $2;
					}
				;

comp_options	:
				| comp_options comp_option
				;

comp_option		: '#' K_OPTION K_DUMP
					{
						pltsql_DumpExecTree = true;
					}
				| '#' K_VARIABLE_CONFLICT K_ERROR
					{
						pltsql_curr_compile->resolve_option = PLTSQL_RESOLVE_ERROR;
					}
				| '#' K_VARIABLE_CONFLICT K_USE_VARIABLE
					{
						pltsql_curr_compile->resolve_option = PLTSQL_RESOLVE_VARIABLE;
					}
				| '#' K_VARIABLE_CONFLICT K_USE_COLUMN
					{
						pltsql_curr_compile->resolve_option = PLTSQL_RESOLVE_COLUMN;
					}
				;

opt_semi		:
				| ';'
				;

opt_semi_or_commma :
				| ','
				| ';'
				;

pl_block		: decl_sect K_BEGIN proc_sect exception_sect K_END
					{
						PLTSQL_stmt_block *new;
						int				  tok1;
						int				  tok2;
						char			  *label;

						new = palloc0(sizeof(PLTSQL_stmt_block));

						new->cmd_type	= PLTSQL_STMT_BLOCK;
						new->lineno		= pltsql_location_to_lineno(@2);
						new->label		= $1.label;
						new->n_initvars = $1.n_initvars;
						new->initvarnos = $1.initvarnos;
						new->body		= $3;
						new->exceptions	= $4;

						pltsql_peek2(&tok1, &tok2, NULL, NULL);

						if (tok1 == IDENT && tok2 == ';')
						{
							tok1 = yylex();
							label = yylval.word.ident;
							check_labels($1.label, label, yylloc);
							tok2 = yylex(); /* consume optional semicolon */
						}
						else if (tok1 == ';')
						{
							tok1 = yylex(); /* consume optional semicolon */
						}

						pltsql_ns_pop();

						$$ = (PLTSQL_stmt *)new;
					}
				;

decl_sect		: opt_block_label
					{
						/* done with decls, so resume identifier lookup */
						pltsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						$$.n_initvars = 0;
						$$.initvarnos = NULL;
					}
				| opt_block_label decl_start
					{
						pltsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						$$.n_initvars = 0;
						$$.initvarnos = NULL;
					}
				| opt_block_label decl_start decl_stmts
					{
						pltsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						/* Remember variables declared in decl_stmts */
						$$.n_initvars = pltsql_add_initdatums(&($$.initvarnos));
					}
				;

decl_start		: K_DECLARE
					{
						/* Forget any variables created before block */
						pltsql_add_initdatums(NULL);
						/*
						 * Disable scanner lookup of identifiers while
						 * we process the decl_stmts
						 */
						pltsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
					}
				;

decl_stmts		: decl_stmts decl_stmt
				| decl_stmt
				;

decl_stmt		: decl_statement
				| K_DECLARE
					{
						/* We allow useless extra DECLAREs */
					}
				| LESS_LESS any_identifier GREATER_GREATER
					{
						/*
						 * Throw a helpful error if user tries to put block
						 * label just before BEGIN, instead of before DECLARE.
						 */
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("block label must be placed before DECLARE, not after"),
								 parser_errposition(@1)));
					}
				;

decl_statement	: decl_varname decl_const decl_datatype decl_collate decl_notnull decl_defval
					{
						PLTSQL_variable	*var;

						/*
						 * If a collation is supplied, insert it into the
						 * datatype.  We assume decl_datatype always returns
						 * a freshly built struct not shared with other
						 * variables.
						 */
						if (OidIsValid($4))
						{
							if (!OidIsValid($3->collation))
								ereport(ERROR,
										(errcode(ERRCODE_DATATYPE_MISMATCH),
										 errmsg("collations are not supported by type %s",
												format_type_be($3->typoid)),
										 parser_errposition(@4)));
							$3->collation = $4;
						}

						var = pltsql_build_variable($1.name, $1.lineno,
													 $3, true);
						if ($2)
						{
							if (var->dtype == PLTSQL_DTYPE_VAR)
								((PLTSQL_var *) var)->isconst = $2;
							else
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("row or record variable cannot be CONSTANT"),
										 parser_errposition(@2)));
						}
						if ($5)
						{
							if (var->dtype == PLTSQL_DTYPE_VAR)
								((PLTSQL_var *) var)->notnull = $5;
							else
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("row or record variable cannot be NOT NULL"),
										 parser_errposition(@4)));

						}
						if ($6 != NULL)
						{
							if (var->dtype == PLTSQL_DTYPE_VAR)
								((PLTSQL_var *) var)->default_val = $6;
							else
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("default value for row or record variable is not supported"),
										 parser_errposition(@5)));
						}
					}
				| decl_varname K_ALIAS K_FOR decl_aliasitem opt_semi_or_commma
					{
						pltsql_ns_additem($4->itemtype,
										   $4->itemno, $1.name);
					}
				| decl_varname opt_scrollable K_CURSOR
					{ pltsql_ns_push($1.name); }
				  decl_cursor_args decl_is_for decl_cursor_query
					{
						PLTSQL_var *new;
						PLTSQL_expr *curname_def;
						char		buf[1024];
						char		*cp1;
						char		*cp2;

						/* pop local namespace for cursor args */
						pltsql_ns_pop();

						new = (PLTSQL_var *)
							pltsql_build_variable($1.name, $1.lineno,
												   pltsql_build_datatype(REFCURSOROID,
																		  -1,
																		  InvalidOid),
												   true);

						curname_def = palloc0(sizeof(PLTSQL_expr));

						curname_def->dtype = PLTSQL_DTYPE_EXPR;
						strcpy(buf, "SELECT ");
						cp1 = new->refname;
						cp2 = buf + strlen(buf);
						/*
						 * Don't trust standard_conforming_strings here;
						 * it might change before we use the string.
						 */
						if (strchr(cp1, '\\') != NULL)
							*cp2++ = ESCAPE_STRING_SYNTAX;
						*cp2++ = '\'';
						while (*cp1)
						{
							if (SQL_STR_DOUBLE(*cp1, true))
								*cp2++ = *cp1;
							*cp2++ = *cp1++;
						}
						strcpy(cp2, "'::pg_catalog.refcursor");
						curname_def->query = pstrdup(buf);
						new->default_val = curname_def;

						new->cursor_explicit_expr = $7;
						if ($5 == NULL)
							new->cursor_explicit_argrow = -1;
						else
							new->cursor_explicit_argrow = $5->dno;
						new->cursor_options = CURSOR_OPT_FAST_PLAN | $2;
					}
				;

opt_scrollable :
					{
						$$ = 0;
					}
				| K_NO K_SCROLL
					{
						$$ = CURSOR_OPT_NO_SCROLL;
					}
				| K_SCROLL
					{
						$$ = CURSOR_OPT_SCROLL;
					}
				;

decl_cursor_query :
					{
						$$ = read_sql_stmt_bos("");
					}
				;

decl_cursor_args :
					{
						$$ = NULL;
					}
				| '(' decl_cursor_arglist ')'
					{
						PLTSQL_row *new;
						int i;
						ListCell *l;

						new = palloc0(sizeof(PLTSQL_row));
						new->dtype = PLTSQL_DTYPE_ROW;
						new->lineno = pltsql_location_to_lineno(@1);
						new->rowtupdesc = NULL;
						new->nfields = list_length($2);
						new->fieldnames = palloc(new->nfields * sizeof(char *));
						new->varnos = palloc(new->nfields * sizeof(int));

						i = 0;
						foreach (l, $2)
						{
							PLTSQL_variable *arg = (PLTSQL_variable *) lfirst(l);
							new->fieldnames[i] = arg->refname;
							new->varnos[i] = arg->dno;
							i++;
						}
						list_free($2);

						pltsql_adddatum((PLTSQL_datum *) new);
						$$ = (PLTSQL_datum *) new;
					}
				;

decl_cursor_arglist : decl_cursor_arg
					{
						$$ = list_make1($1);
					}
				| decl_cursor_arglist ',' decl_cursor_arg
					{
						$$ = lappend($1, $3);
					}
				;

decl_cursor_arg : decl_varname decl_datatype
					{
						$$ = (PLTSQL_datum *)
							pltsql_build_variable($1.name, $1.lineno,
												   $2, true);
					}
				;

decl_is_for		:	K_IS |		/* Oracle */
					K_FOR;		/* SQL standard */

decl_aliasitem	: T_WORD
					{
						PLTSQL_nsitem *nsi;

						nsi = pltsql_ns_lookup(pltsql_ns_top(), false,
												$1.ident, NULL, NULL,
												NULL);
						if (nsi == NULL)
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_OBJECT),
									 errmsg("variable \"%s\" does not exist",
											$1.ident),
									 parser_errposition(@1)));
						$$ = nsi;
					}
				| T_CWORD
					{
						PLTSQL_nsitem *nsi;

						if (list_length($1.idents) == 2)
							nsi = pltsql_ns_lookup(pltsql_ns_top(), false,
													strVal(linitial($1.idents)),
													strVal(lsecond($1.idents)),
													NULL,
													NULL);
						else if (list_length($1.idents) == 3)
							nsi = pltsql_ns_lookup(pltsql_ns_top(), false,
													strVal(linitial($1.idents)),
													strVal(lsecond($1.idents)),
													strVal(lthird($1.idents)),
													NULL);
						else
							nsi = NULL;
						if (nsi == NULL)
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_OBJECT),
									 errmsg("variable \"%s\" does not exist",
											NameListToString($1.idents)),
									 parser_errposition(@1)));
						$$ = nsi;
					}
				;

decl_varname	: T_WORD
					{
						$$.name = $1.ident;
						$$.lineno = pltsql_location_to_lineno(@1);
						/*
						 * Check to make sure name isn't already declared
						 * in the current block.
						 */
						if (pltsql_ns_lookup(pltsql_ns_top(), true,
											  $1.ident, NULL, NULL,
											  NULL) != NULL)
							yyerror("duplicate declaration");
					}
				| unreserved_keyword
					{
						$$.name = pstrdup($1);
						$$.lineno = pltsql_location_to_lineno(@1);
						/*
						 * Check to make sure name isn't already declared
						 * in the current block.
						 */
						if (pltsql_ns_lookup(pltsql_ns_top(), true,
											  $1, NULL, NULL,
											  NULL) != NULL)
							yyerror("duplicate declaration");
					}
				;

decl_const		:
					{ $$ = false; }
				| K_CONSTANT
					{ $$ = true; }
				;

decl_datatype	:
					{
						/*
						 * If there's a lookahead token, read_datatype
						 * should consume it.
						 */
						$$ = read_datatype(yychar);
						yyclearin;
					}
				;

decl_collate	:
					{ $$ = InvalidOid; }
				| K_COLLATE T_WORD
					{
						$$ = get_collation_oid(list_make1(makeString($2.ident)),
											   false);
					}
				| K_COLLATE T_CWORD
					{
						$$ = get_collation_oid($2.idents, false);
					}
				;

decl_notnull	:
					{ $$ = false; }
				| K_NOT K_NULL
					{ $$ = true; }
				;

decl_defval		: ';'
					{ $$ = NULL; }
				| ','
					{ $$ = NULL; }
				| K_DECLARE
					{
                        pltsql_push_back_token(K_DECLARE);
                        $$ = NULL;
                    }
				| K_BEGIN
					{
                        pltsql_push_back_token(K_BEGIN);
                        $$ = NULL;
                    }
				| T_WORD
					{
                        pltsql_push_back_token(T_WORD);
                        $$ = NULL;
                    }
				| decl_defkey
					{
						$$ = read_sql_expression2_bos(';', ',', ", or ;", NULL);
					}
				;

decl_defkey		: assign_operator
				| K_DEFAULT
				;

assign_operator	: '='
				| COLON_EQUALS
				;

proc_sect		:
					{ $$ = NIL; }
				| proc_stmts
					{ $$ = $1; }
				;

proc_stmts		: proc_stmts proc_stmt
						{
							if ($2 == NULL)
								$$ = $1;
							else
								$$ = lappend($1, $2);
						}
				| proc_stmt
						{
							if ($1 == NULL)
								$$ = NIL;
							else
								$$ = list_make1($1);
						}
				;

proc_stmt		: common_stmt |
				  plpgsql_only_stmt |
				  pltsql_only_stmt
				;

common_stmt	    : pl_block
						{ $$ = $1; }
				| stmt_assign
						{ $$ = $1; }
				| stmt_if
						{ $$ = $1; }
				| stmt_while
						{ $$ = $1; }
				| stmt_for
						{ $$ = $1; }
				| stmt_foreach_a
						{ $$ = $1; }
				| stmt_exit
						{ $$ = $1; }
				| stmt_return
						{ $$ = $1; }
				| stmt_raise
						{ $$ = $1; }
				| stmt_execsql
						{ $$ = $1; }
				| stmt_dynexecute
						{ $$ = $1; }
				| stmt_perform
						{ $$ = $1; }
				| stmt_getdiag
						{ $$ = $1; }
				| stmt_open
						{ $$ = $1; }
				| stmt_fetch
						{ $$ = $1; }
				| stmt_move
						{ $$ = $1; }
				| stmt_close
						{ $$ = $1; }
				| stmt_null
						{ $$ = $1; }

plpgsql_only_stmt	:	stmt_loop
						{ $$ = $1; }
					;

pltsql_only_stmt	:	stmt_print
						{ $$ = $1; }
					;

stmt_perform	: K_PERFORM expr_until_semi_or_bos
					{
						PLTSQL_stmt_perform *new;

						new = palloc0(sizeof(PLTSQL_stmt_perform));
						new->cmd_type = PLTSQL_STMT_PERFORM;
						new->lineno   = pltsql_location_to_lineno(@1);
						new->expr  = $2;

						$$ = (PLTSQL_stmt *)new;
					}
				;

opt_set		:
			| K_SET
			;


stmt_assign		: opt_set assign_var assign_operator expr_until_semi_or_bos
					{
						PLTSQL_stmt_assign *new;

						new = palloc0(sizeof(PLTSQL_stmt_assign));
						new->cmd_type = PLTSQL_STMT_ASSIGN;
						new->lineno   = pltsql_location_to_lineno(@2);
						new->varno = $2;
						new->expr  = $4;

						$$ = (PLTSQL_stmt *)new;
					}
				;

stmt_getdiag	: K_GET getdiag_area_opt K_DIAGNOSTICS getdiag_list opt_semi
					{
						PLTSQL_stmt_getdiag	 *new;
						ListCell		*lc;

						new = palloc0(sizeof(PLTSQL_stmt_getdiag));
						new->cmd_type = PLTSQL_STMT_GETDIAG;
						new->lineno   = pltsql_location_to_lineno(@1);
						new->is_stacked = $2;
						new->diag_items = $4;

						/*
						 * Check information items are valid for area option.
						 */
						foreach(lc, new->diag_items)
						{
							PLTSQL_diag_item *ditem = (PLTSQL_diag_item *) lfirst(lc);

							switch (ditem->kind)
							{
								/* these fields are disallowed in stacked case */
								case PLTSQL_GETDIAG_ROW_COUNT:
								case PLTSQL_GETDIAG_RESULT_OID:
									if (new->is_stacked)
										ereport(ERROR,
												(errcode(ERRCODE_SYNTAX_ERROR),
												 errmsg("diagnostics item %s is not allowed in GET STACKED DIAGNOSTICS",
														pltsql_getdiag_kindname(ditem->kind)),
												 parser_errposition(@1)));
									break;
								/* these fields are disallowed in current case */
								case PLTSQL_GETDIAG_ERROR_CONTEXT:
								case PLTSQL_GETDIAG_ERROR_DETAIL:
								case PLTSQL_GETDIAG_ERROR_HINT:
								case PLTSQL_GETDIAG_RETURNED_SQLSTATE:
								case PLTSQL_GETDIAG_MESSAGE_TEXT:
									if (!new->is_stacked)
										ereport(ERROR,
												(errcode(ERRCODE_SYNTAX_ERROR),
												 errmsg("diagnostics item %s is not allowed in GET CURRENT DIAGNOSTICS",
														pltsql_getdiag_kindname(ditem->kind)),
												 parser_errposition(@1)));
									break;
								default:
									elog(ERROR, "unrecognized diagnostic item kind: %d",
										 ditem->kind);
									break;
							}
						}

						$$ = (PLTSQL_stmt *)new;
					}
				;

getdiag_area_opt :
					{
						$$ = false;
					}
				| K_CURRENT
					{
						$$ = false;
					}
				| K_STACKED
					{
						$$ = true;
					}
				;

getdiag_list : getdiag_list ',' getdiag_list_item
					{
						$$ = lappend($1, $3);
					}
				| getdiag_list_item
					{
						$$ = list_make1($1);
					}
				;

getdiag_list_item : getdiag_target assign_operator getdiag_item
					{
						PLTSQL_diag_item *new;

						new = palloc(sizeof(PLTSQL_diag_item));
						new->target = $1;
						new->kind = $3;

						$$ = new;
					}
				;

getdiag_item :
					{
						int	tok = yylex();

						if (tok_is_keyword(tok, &yylval,
										   K_ROW_COUNT, "row_count"))
							$$ = PLTSQL_GETDIAG_ROW_COUNT;
						else if (tok_is_keyword(tok, &yylval,
												K_RESULT_OID, "result_oid"))
							$$ = PLTSQL_GETDIAG_RESULT_OID;
						else if (tok_is_keyword(tok, &yylval,
												K_PG_EXCEPTION_DETAIL, "pg_exception_detail"))
							$$ = PLTSQL_GETDIAG_ERROR_DETAIL;
						else if (tok_is_keyword(tok, &yylval,
												K_PG_EXCEPTION_HINT, "pg_exception_hint"))
							$$ = PLTSQL_GETDIAG_ERROR_HINT;
						else if (tok_is_keyword(tok, &yylval,
												K_PG_EXCEPTION_CONTEXT, "pg_exception_context"))
							$$ = PLTSQL_GETDIAG_ERROR_CONTEXT;
						else if (tok_is_keyword(tok, &yylval,
												K_MESSAGE_TEXT, "message_text"))
							$$ = PLTSQL_GETDIAG_MESSAGE_TEXT;
						else if (tok_is_keyword(tok, &yylval,
												K_RETURNED_SQLSTATE, "returned_sqlstate"))
							$$ = PLTSQL_GETDIAG_RETURNED_SQLSTATE;
						else
							yyerror("unrecognized GET DIAGNOSTICS item");
					}
				;

getdiag_target	: T_DATUM
					{
						check_assignable($1.datum, @1);
						if ($1.datum->dtype == PLTSQL_DTYPE_ROW ||
							$1.datum->dtype == PLTSQL_DTYPE_REC)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("\"%s\" is not a scalar variable",
											NameOfDatum(&($1))),
									 parser_errposition(@1)));
						$$ = $1.datum->dno;
					}
				| T_WORD
					{
						/* just to give a better message than "syntax error" */
						word_is_not_variable(&($1), @1);
					}
				| T_CWORD
					{
						/* just to give a better message than "syntax error" */
						cword_is_not_variable(&($1), @1);
					}
				;


assign_var		: T_DATUM
					{
						check_assignable($1.datum, @1);
						$$ = $1.datum->dno;
					}
				| assign_var '[' expr_until_rightbracket
					{
						PLTSQL_arrayelem	*new;

						new = palloc0(sizeof(PLTSQL_arrayelem));
						new->dtype		= PLTSQL_DTYPE_ARRAYELEM;
						new->subscript	= $3;
						new->arrayparentno = $1;
						/* initialize cached type data to "not valid" */
						new->parenttypoid = InvalidOid;

						pltsql_adddatum((PLTSQL_datum *) new);

						$$ = new->dno;
					}
				;

stmt_if			: K_IF expr_until_bos proc_stmt %prec LOWER_THAN_ELSE
				{
						PLTSQL_stmt_if *new;

						new = palloc0(sizeof(PLTSQL_stmt_if));
						new->cmd_type	= PLTSQL_STMT_IF;
						new->lineno		= pltsql_location_to_lineno(@1);
						new->cond		= $2;
						new->then_body	= list_make1($3);

						$$ = (PLTSQL_stmt *)new;
				}
				| K_IF expr_until_bos proc_stmt K_ELSE proc_stmt
				{
						PLTSQL_stmt_if *new;

						new = palloc0(sizeof(PLTSQL_stmt_if));
						new->cmd_type	= PLTSQL_STMT_IF;
						new->lineno		= pltsql_location_to_lineno(@1);
						new->cond		= $2;
						new->then_body	= list_make1($3);
						new->else_body  = list_make1($5);

						$$ = (PLTSQL_stmt *)new;
				}
				;

stmt_loop		: opt_block_label K_LOOP loop_body
					{
						PLTSQL_stmt_loop *new;

						new = palloc0(sizeof(PLTSQL_stmt_loop));
						new->cmd_type = PLTSQL_STMT_LOOP;
						new->lineno   = pltsql_location_to_lineno(@2);
						new->label	  = $1;
						new->body	  = $3.stmts;

						check_labels($1, $3.end_label, $3.end_label_location);
						pltsql_ns_pop();

						$$ = (PLTSQL_stmt *)new;
					}
				;

stmt_while		: opt_block_label K_WHILE expr_until_bos K_LOOP loop_body
					{
						PLTSQL_stmt_while *new;

						new = palloc0(sizeof(PLTSQL_stmt_while));
						new->cmd_type = PLTSQL_STMT_WHILE;
						new->lineno   = pltsql_location_to_lineno(@2);
						new->label	  = $1;
						new->cond	  = $3;
						new->body	  = $5.stmts;

						check_labels($1, $5.end_label, $5.end_label_location);
						pltsql_ns_pop();

						$$ = (PLTSQL_stmt *)new;
					}
				| opt_block_label K_WHILE expr_until_bos common_stmt
					{
						PLTSQL_stmt_while *new;

						new = palloc0(sizeof(PLTSQL_stmt_while));
						new->cmd_type = PLTSQL_STMT_WHILE;
						new->lineno   = pltsql_location_to_lineno(@2);
						new->label	  = $1;
						new->cond	  = $3;
						new->body	  = list_make1($4);

						pltsql_ns_pop();

						$$ = (PLTSQL_stmt *)new;
					}
				;



stmt_for		: opt_block_label K_FOR for_control loop_body
					{
						/* This runs after we've scanned the loop body */
						if ($3->cmd_type == PLTSQL_STMT_FORI)
						{
							PLTSQL_stmt_fori		*new;

							new = (PLTSQL_stmt_fori *) $3;
							new->lineno   = pltsql_location_to_lineno(@2);
							new->label	  = $1;
							new->body	  = $4.stmts;
							$$ = (PLTSQL_stmt *) new;
						}
						else
						{
							PLTSQL_stmt_forq		*new;

							Assert($3->cmd_type == PLTSQL_STMT_FORS ||
								   $3->cmd_type == PLTSQL_STMT_FORC ||
								   $3->cmd_type == PLTSQL_STMT_DYNFORS);
							/* forq is the common supertype of all three */
							new = (PLTSQL_stmt_forq *) $3;
							new->lineno   = pltsql_location_to_lineno(@2);
							new->label	  = $1;
							new->body	  = $4.stmts;
							$$ = (PLTSQL_stmt *) new;
						}

						check_labels($1, $4.end_label, $4.end_label_location);
						/* close namespace started in opt_block_label */
						pltsql_ns_pop();
					}
				;

for_control		: for_variable K_IN
					{
						int			tok = yylex();
						int			tokloc = yylloc;

						if (tok == K_EXECUTE)
						{
							/* EXECUTE means it's a dynamic FOR loop */
							PLTSQL_stmt_dynfors	*new;
							PLTSQL_expr			*expr;
							int						term;

							expr = read_sql_expression2(K_LOOP, K_USING,
														"LOOP or USING",
														&term);

							new = palloc0(sizeof(PLTSQL_stmt_dynfors));
							new->cmd_type = PLTSQL_STMT_DYNFORS;
							if ($1.rec)
							{
								new->rec = $1.rec;
								check_assignable((PLTSQL_datum *) new->rec, @1);
							}
							else if ($1.row)
							{
								new->row = $1.row;
								check_assignable((PLTSQL_datum *) new->row, @1);
							}
							else if ($1.scalar)
							{
								/* convert single scalar to list */
								new->row = make_scalar_list1($1.name, $1.scalar,
															 $1.lineno, @1);
								/* no need for check_assignable */
							}
							else
							{
								ereport(ERROR,
										(errcode(ERRCODE_DATATYPE_MISMATCH),
										 errmsg("loop variable of loop over rows must be a record or row variable or list of scalar variables"),
										 parser_errposition(@1)));
							}
							new->query = expr;

							if (term == K_USING)
							{
								do
								{
									expr = read_sql_expression2(',', K_LOOP,
																", or LOOP",
																&term);
									new->params = lappend(new->params, expr);
								} while (term == ',');
							}

							$$ = (PLTSQL_stmt *) new;
						}
						else if (tok == T_DATUM &&
								 yylval.wdatum.datum->dtype == PLTSQL_DTYPE_VAR &&
								 ((PLTSQL_var *) yylval.wdatum.datum)->datatype->typoid == REFCURSOROID)
						{
							/* It's FOR var IN cursor */
							PLTSQL_stmt_forc	*new;
							PLTSQL_var			*cursor = (PLTSQL_var *) yylval.wdatum.datum;

							new = (PLTSQL_stmt_forc *) palloc0(sizeof(PLTSQL_stmt_forc));
							new->cmd_type = PLTSQL_STMT_FORC;
							new->curvar = cursor->dno;

							/* Should have had a single variable name */
							if ($1.scalar && $1.row)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("cursor FOR loop must have only one target variable"),
										 parser_errposition(@1)));

							/* can't use an unbound cursor this way */
							if (cursor->cursor_explicit_expr == NULL)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("cursor FOR loop must use a bound cursor variable"),
										 parser_errposition(tokloc)));

							/* collect cursor's parameters if any */
							new->argquery = read_cursor_args(cursor,
															 K_LOOP,
															 "LOOP");

							/* create loop's private RECORD variable */
							new->rec = pltsql_build_record($1.name,
															$1.lineno,
															true);

							$$ = (PLTSQL_stmt *) new;
						}
						else
						{
							PLTSQL_expr	*expr1;
							int				expr1loc;
							bool			reverse = false;

							/*
							 * We have to distinguish between two
							 * alternatives: FOR var IN a .. b and FOR
							 * var IN query. Unfortunately this is
							 * tricky, since the query in the second
							 * form needn't start with a SELECT
							 * keyword.  We use the ugly hack of
							 * looking for two periods after the first
							 * token. We also check for the REVERSE
							 * keyword, which means it must be an
							 * integer loop.
							 */
							if (tok_is_keyword(tok, &yylval,
											   K_REVERSE, "reverse"))
								reverse = true;
							else
								pltsql_push_back_token(tok);

							/*
							 * Read tokens until we see either a ".."
							 * or a LOOP. The text we read may not
							 * necessarily be a well-formed SQL
							 * statement, so we need to invoke
							 * read_sql_construct directly.
							 */
							expr1 = read_sql_construct(DOT_DOT,
													   K_LOOP,
													   0,
													   "LOOP",
													   "SELECT ",
													   true,
													   false,
													   true,
													   &expr1loc,
													   &tok);

							if (tok == DOT_DOT)
							{
								/* Saw "..", so it must be an integer loop */
								PLTSQL_expr		*expr2;
								PLTSQL_expr		*expr_by;
								PLTSQL_var			*fvar;
								PLTSQL_stmt_fori	*new;

								/* Check first expression is well-formed */
								check_sql_expr(expr1->query, expr1loc, 7);

								/* Read and check the second one */
								expr2 = read_sql_expression2(K_LOOP, K_BY,
															 "LOOP",
															 &tok);

								/* Get the BY clause if any */
								if (tok == K_BY)
									expr_by = read_sql_expression(K_LOOP,
																  "LOOP");
								else
									expr_by = NULL;

								/* Should have had a single variable name */
								if ($1.scalar && $1.row)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
											 errmsg("integer FOR loop must have only one target variable"),
											 parser_errposition(@1)));

								/* create loop's private variable */
								fvar = (PLTSQL_var *)
									pltsql_build_variable($1.name,
														   $1.lineno,
														   pltsql_build_datatype(INT4OID,
																				  -1,
																				  InvalidOid),
														   true);

								new = palloc0(sizeof(PLTSQL_stmt_fori));
								new->cmd_type = PLTSQL_STMT_FORI;
								new->var	  = fvar;
								new->reverse  = reverse;
								new->lower	  = expr1;
								new->upper	  = expr2;
								new->step	  = expr_by;

								$$ = (PLTSQL_stmt *) new;
							}
							else
							{
								/*
								 * No "..", so it must be a query loop. We've
								 * prefixed an extra SELECT to the query text,
								 * so we need to remove that before performing
								 * syntax checking.
								 */
								char				*tmp_query;
								PLTSQL_stmt_fors	*new;

								if (reverse)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
											 errmsg("cannot specify REVERSE in query FOR loop"),
											 parser_errposition(tokloc)));

								Assert(strncmp(expr1->query, "SELECT ", 7) == 0);
								tmp_query = pstrdup(expr1->query + 7);
								pfree(expr1->query);
								expr1->query = tmp_query;

								check_sql_expr(expr1->query, expr1loc, 0);

								new = palloc0(sizeof(PLTSQL_stmt_fors));
								new->cmd_type = PLTSQL_STMT_FORS;
								if ($1.rec)
								{
									new->rec = $1.rec;
									check_assignable((PLTSQL_datum *) new->rec, @1);
								}
								else if ($1.row)
								{
									new->row = $1.row;
									check_assignable((PLTSQL_datum *) new->row, @1);
								}
								else if ($1.scalar)
								{
									/* convert single scalar to list */
									new->row = make_scalar_list1($1.name, $1.scalar,
																 $1.lineno, @1);
									/* no need for check_assignable */
								}
								else
								{
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
											 errmsg("loop variable of loop over rows must be a record or row variable or list of scalar variables"),
											 parser_errposition(@1)));
								}

								new->query = expr1;
								$$ = (PLTSQL_stmt *) new;
							}
						}
					}
				;

/*
 * Processing the for_variable is tricky because we don't yet know if the
 * FOR is an integer FOR loop or a loop over query results.  In the former
 * case, the variable is just a name that we must instantiate as a loop
 * local variable, regardless of any other definition it might have.
 * Therefore, we always save the actual identifier into $$.name where it
 * can be used for that case.  We also save the outer-variable definition,
 * if any, because that's what we need for the loop-over-query case.  Note
 * that we must NOT apply check_assignable() or any other semantic check
 * until we know what's what.
 *
 * However, if we see a comma-separated list of names, we know that it
 * can't be an integer FOR loop and so it's OK to check the variables
 * immediately.  In particular, for T_WORD followed by comma, we should
 * complain that the name is not known rather than say it's a syntax error.
 * Note that the non-error result of this case sets *both* $$.scalar and
 * $$.row; see the for_control production.
 */
for_variable	: T_DATUM
					{
						$$.name = NameOfDatum(&($1));
						$$.lineno = pltsql_location_to_lineno(@1);
						if ($1.datum->dtype == PLTSQL_DTYPE_ROW)
						{
							$$.scalar = NULL;
							$$.rec = NULL;
							$$.row = (PLTSQL_row *) $1.datum;
						}
						else if ($1.datum->dtype == PLTSQL_DTYPE_REC)
						{
							$$.scalar = NULL;
							$$.rec = (PLTSQL_rec *) $1.datum;
							$$.row = NULL;
						}
						else
						{
							int			tok;

							$$.scalar = $1.datum;
							$$.rec = NULL;
							$$.row = NULL;
							/* check for comma-separated list */
							tok = yylex();
							pltsql_push_back_token(tok);
							if (tok == ',')
								$$.row = read_into_scalar_list($$.name,
															   $$.scalar,
															   @1);
						}
					}
				| T_WORD
					{
						int			tok;

						$$.name = $1.ident;
						$$.lineno = pltsql_location_to_lineno(@1);
						$$.scalar = NULL;
						$$.rec = NULL;
						$$.row = NULL;
						/* check for comma-separated list */
						tok = yylex();
						pltsql_push_back_token(tok);
						if (tok == ',')
							word_is_not_variable(&($1), @1);
					}
				| T_CWORD
					{
						/* just to give a better message than "syntax error" */
						cword_is_not_variable(&($1), @1);
					}
				;

stmt_foreach_a	: opt_block_label K_FOREACH for_variable foreach_slice K_IN K_ARRAY expr_until_loop loop_body
					{
						PLTSQL_stmt_foreach_a *new;

						new = palloc0(sizeof(PLTSQL_stmt_foreach_a));
						new->cmd_type = PLTSQL_STMT_FOREACH_A;
						new->lineno = pltsql_location_to_lineno(@2);
						new->label = $1;
						new->slice = $4;
						new->expr = $7;
						new->body = $8.stmts;

						if ($3.rec)
						{
							new->varno = $3.rec->dno;
							check_assignable((PLTSQL_datum *) $3.rec, @3);
						}
						else if ($3.row)
						{
							new->varno = $3.row->dno;
							check_assignable((PLTSQL_datum *) $3.row, @3);
						}
						else if ($3.scalar)
						{
							new->varno = $3.scalar->dno;
							check_assignable($3.scalar, @3);
						}
						else
						{
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("loop variable of FOREACH must be a known variable or list of variables"),
											 parser_errposition(@3)));
						}

						check_labels($1, $8.end_label, $8.end_label_location);
						pltsql_ns_pop();

						$$ = (PLTSQL_stmt *) new;
					}
				;

foreach_slice	:
					{
						$$ = 0;
					}
				| K_SLICE ICONST
					{
						$$ = $2;
					}
				;

stmt_exit		: exit_type opt_label opt_exitcond
					{
						PLTSQL_stmt_exit *new;

						new = palloc0(sizeof(PLTSQL_stmt_exit));
						new->cmd_type = PLTSQL_STMT_EXIT;
						new->is_exit  = $1;
						new->lineno	  = pltsql_location_to_lineno(@1);
						new->label	  = $2;
						new->cond	  = $3;

						$$ = (PLTSQL_stmt *)new;
					}
				;

exit_type		: K_EXIT
					{
						$$ = true;
					}
				| K_CONTINUE
					{
						$$ = false;
					}
				;

stmt_return		: K_RETURN
					{
						int	tok;

						tok = yylex();
						if (tok == 0)
							yyerror("unexpected end of function definition");

						if (tok_is_keyword(tok, &yylval,
										   K_NEXT, "next"))
						{
							$$ = make_return_next_stmt(@1);
						}
						else if (tok_is_keyword(tok, &yylval,
												K_QUERY, "query"))
						{
							$$ = make_return_query_stmt(@1);
						}
						else
						{
							pltsql_push_back_token(tok);
							$$ = make_return_stmt(@1);
						}
					}
				;

stmt_raise		: K_RAISE
					{
						PLTSQL_stmt_raise		*new;
						int	tok;

						new = palloc(sizeof(PLTSQL_stmt_raise));

						new->cmd_type	= PLTSQL_STMT_RAISE;
						new->lineno		= pltsql_location_to_lineno(@1);
						new->elog_level = ERROR;	/* default */
						new->condname	= NULL;
						new->message	= NULL;
						new->params		= NIL;
						new->options	= NIL;

						tok = yylex();
						if (tok == 0)
							yyerror("unexpected end of function definition");

						/*
						 * We could have just RAISE, meaning to re-throw
						 * the current error.
						 */
						if (tok != ';')
						{
							/*
							 * First is an optional elog severity level.
							 */
							if (tok_is_keyword(tok, &yylval,
											   K_EXCEPTION, "exception"))
							{
								new->elog_level = ERROR;
								tok = yylex();
							}
							else if (tok_is_keyword(tok, &yylval,
													K_WARNING, "warning"))
							{
								new->elog_level = WARNING;
								tok = yylex();
							}
							else if (tok_is_keyword(tok, &yylval,
													K_NOTICE, "notice"))
							{
								new->elog_level = NOTICE;
								tok = yylex();
							}
							else if (tok_is_keyword(tok, &yylval,
													K_INFO, "info"))
							{
								new->elog_level = INFO;
								tok = yylex();
							}
							else if (tok_is_keyword(tok, &yylval,
													K_LOG, "log"))
							{
								new->elog_level = LOG;
								tok = yylex();
							}
							else if (tok_is_keyword(tok, &yylval,
													K_DEBUG, "debug"))
							{
								new->elog_level = DEBUG1;
								tok = yylex();
							}
							if (tok == 0)
								yyerror("unexpected end of function definition");

							/*
							 * Next we can have a condition name, or
							 * equivalently SQLSTATE 'xxxxx', or a string
							 * literal that is the old-style message format,
							 * or USING to start the option list immediately.
							 */
							if (tok == SCONST)
							{
								/* old style message and parameters */
								new->message = yylval.str;
								/*
								 * We expect either a semi-colon, which
								 * indicates no parameters, or a comma that
								 * begins the list of parameter expressions,
								 * or USING to begin the options list.
								 */
								tok = yylex();
								if (tok != ',' && tok != ';' && tok != K_USING)
									yyerror("syntax error");

								while (tok == ',')
								{
									PLTSQL_expr *expr;

									expr = read_sql_construct(',', ';', K_USING,
															  ", or ; or USING",
															  "SELECT ",
															  true, true, true,
															  NULL, &tok);
									new->params = lappend(new->params, expr);
								}
							}
							else if (tok != K_USING)
							{
								/* must be condition name or SQLSTATE */
								if (tok_is_keyword(tok, &yylval,
												   K_SQLSTATE, "sqlstate"))
								{
									/* next token should be a string literal */
									char   *sqlstatestr;

									if (yylex() != SCONST)
										yyerror("syntax error");
									sqlstatestr = yylval.str;

									if (strlen(sqlstatestr) != 5)
										yyerror("invalid SQLSTATE code");
									if (strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5)
										yyerror("invalid SQLSTATE code");
									new->condname = sqlstatestr;
								}
								else
								{
									if (tok != T_WORD)
										yyerror("syntax error");
									new->condname = yylval.word.ident;
									pltsql_recognize_err_condition(new->condname,
																	false);
								}
								tok = yylex();
								if (tok != ';' && tok != K_USING)
									yyerror("syntax error");
							}

							if (tok == K_USING)
								new->options = read_raise_options();
						}

						$$ = (PLTSQL_stmt *)new;
					}
				;

stmt_print		: K_PRINT
					{
						PLTSQL_stmt_raise		*new;
						int	tok;
						PLTSQL_expr *expr;

						new = palloc(sizeof(PLTSQL_stmt_raise));

						new->cmd_type	= PLTSQL_STMT_RAISE;
						new->lineno		= pltsql_location_to_lineno(@1);
						/*
						 * Of all the message levels that are usually visible to
						 * clients, INFO is the least likely to be configured to
						 * show up in the server logs.
						 */
						new->elog_level = INFO;
						new->condname	= NULL;
						new->message	= NULL;
						new->params		= NIL;
						new->options	= NIL;

						tok = yylex();
						if (tok == 0)
							yyerror("unexpected end of function definition");

						if (tok == ';')
							yyerror("Incorrect syntax");

						/*
						 * We expect only one parameter: either a string
						 * literal, a local variable or a global variable.
						 */
						if (tok == SCONST)
							new->message = yylval.str;
						else if (tok == T_DATUM)
						{
							pltsql_push_back_token(tok);

							new->message = "%";
							expr = read_sql_construct_bos(';', 0, 0,
													  " ",
													  "SELECT ",
													  true, true, true,
													  NULL, &tok, true);
							new->params = lappend(new->params, expr);
						}

						/*
						 * Make semicolon statement termination optional.
						 */
						tok = yylex();
						if (tok != ';')
						   pltsql_push_back_token(tok);

						$$ = (PLTSQL_stmt *)new;
					}
				;

loop_body		: proc_sect K_END K_LOOP opt_label ';'
					{
						$$.stmts = $1;
						$$.end_label = $4;
						$$.end_label_location = @4;
					}
				;

/*
 * T_WORD+T_CWORD match any initial identifier that is not a known pltsql
 * variable.  (The composite case is probably a syntax error, but we'll let
 * the core parser decide that.)  Normally, we should assume that such a
 * word is a SQL statement keyword that isn't also a pltsql keyword.
 * However, if the next token is assignment or '[', it can't be a valid
 * SQL statement, and what we're probably looking at is an intended variable
 * assignment.  Give an appropriate complaint for that, instead of letting
 * the core parser throw an unhelpful "syntax error".
 */
stmt_execsql	: K_INSERT
					{
						$$ = make_execsql_stmt(K_INSERT, @1, NULL);
					}
				| T_WORD
					{
						int			tok;

						tok = yylex();
						pltsql_push_back_token(tok);
						if (tok == '=' || tok == COLON_EQUALS || tok == '[')
							word_is_not_variable(&($1), @1);
						$$ = make_execsql_stmt(T_WORD, @1, &($1));
					}
				| T_CWORD
					{
						int			tok;

						tok = yylex();
						pltsql_push_back_token(tok);
						if (tok == '=' || tok == COLON_EQUALS || tok == '[')
							cword_is_not_variable(&($1), @1);
						$$ = make_execsql_stmt(T_CWORD, @1, NULL);
					}
				;

stmt_dynexecute : K_EXECUTE
					{
						PLTSQL_stmt_dynexecute *new;
						PLTSQL_expr *expr;
						int endtoken;

						expr = read_sql_construct(K_INTO, K_USING, ';',
												  "INTO or USING or ;",
												  "SELECT ",
												  true, true, true,
												  NULL, &endtoken);

						new = palloc(sizeof(PLTSQL_stmt_dynexecute));
						new->cmd_type = PLTSQL_STMT_DYNEXECUTE;
						new->lineno = pltsql_location_to_lineno(@1);
						new->query = expr;
						new->into = false;
						new->strict = false;
						new->target = NULL;
						new->params = NIL;

						/*
						 * We loop to allow the INTO and USING clauses to
						 * appear in either order, since people easily get
						 * that wrong.  This coding also prevents "INTO foo"
						 * from getting absorbed into a USING expression,
						 * which is *really* confusing.
						 */
						for (;;)
						{
							if (endtoken == K_INTO)
							{
								if (new->into)			/* multiple INTO */
									yyerror("syntax error");
								new->into = true;
								read_into_target(&new->target, &new->strict);
								endtoken = yylex();
							}
							else if (endtoken == K_USING)
							{
								if (new->params)		/* multiple USING */
									yyerror("syntax error");
								do
								{
									expr = read_sql_construct(',', ';', K_INTO,
															  ", or ; or INTO",
															  "SELECT ",
															  true, true, true,
															  NULL, &endtoken);
									new->params = lappend(new->params, expr);
								} while (endtoken == ',');
							}
							else if (endtoken == ';')
								break;
							else
								yyerror("syntax error");
						}

						$$ = (PLTSQL_stmt *)new;
					}
				;


stmt_open		: K_OPEN cursor_variable
					{
						PLTSQL_stmt_open *new;
						int				  tok;

						new = palloc0(sizeof(PLTSQL_stmt_open));
						new->cmd_type = PLTSQL_STMT_OPEN;
						new->lineno = pltsql_location_to_lineno(@1);
						new->curvar = $2->dno;
						new->cursor_options = CURSOR_OPT_FAST_PLAN;

						if ($2->cursor_explicit_expr == NULL)
						{
							/* be nice if we could use opt_scrollable here */
							tok = yylex();
							if (tok_is_keyword(tok, &yylval,
											   K_NO, "no"))
							{
								tok = yylex();
								if (tok_is_keyword(tok, &yylval,
												   K_SCROLL, "scroll"))
								{
									new->cursor_options |= CURSOR_OPT_NO_SCROLL;
									tok = yylex();
								}
							}
							else if (tok_is_keyword(tok, &yylval,
													K_SCROLL, "scroll"))
							{
								new->cursor_options |= CURSOR_OPT_SCROLL;
								tok = yylex();
							}

							if (tok != K_FOR)
								yyerror("syntax error, expected \"FOR\"");

							tok = yylex();
							if (tok == K_EXECUTE)
							{
								int		endtoken;

								new->dynquery =
									read_sql_expression2(K_USING, ';',
														 "USING or ;",
														 &endtoken);

								/* If we found "USING", collect argument(s) */
								if (endtoken == K_USING)
								{
									PLTSQL_expr *expr;

									do
									{
										expr = read_sql_expression2(',', ';',
																	", or ;",
																	&endtoken);
										new->params = lappend(new->params,
															  expr);
									} while (endtoken == ',');
								}
							}
							else
							{
								pltsql_push_back_token(tok);
								new->query = read_sql_stmt("");
							}
						}
						else
						{
							/* predefined cursor query, so read args */
							new->argquery = read_cursor_args($2, ';', ";");
						}

						$$ = (PLTSQL_stmt *)new;
					}
				;

stmt_fetch		: K_FETCH opt_fetch_direction cursor_variable K_INTO
					{
						PLTSQL_stmt_fetch *fetch = $2;
						PLTSQL_variable *target;

						/* We have already parsed everything through the INTO keyword */
						read_into_target(&target, NULL);

						if (yylex() != ';')
							yyerror("syntax error");

						/*
						 * We don't allow multiple rows in PL/TSQL's FETCH
						 * statement, only in MOVE.
						 */
						if (fetch->returns_multiple_rows)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("FETCH statement cannot return multiple rows"),
									 parser_errposition(@1)));

						fetch->lineno = pltsql_location_to_lineno(@1);
						fetch->target   = target;
						fetch->curvar	= $3->dno;
						fetch->is_move	= false;

						$$ = (PLTSQL_stmt *)fetch;
					}
				;

stmt_move		: K_MOVE opt_fetch_direction cursor_variable opt_semi
					{
						PLTSQL_stmt_fetch *fetch = $2;

						fetch->lineno = pltsql_location_to_lineno(@1);
						fetch->curvar	= $3->dno;
						fetch->is_move	= true;

						$$ = (PLTSQL_stmt *)fetch;
					}
				;

opt_fetch_direction	:
					{
						$$ = read_fetch_direction();
					}
				;

stmt_close		: K_CLOSE cursor_variable opt_semi
					{
						PLTSQL_stmt_close *new;

						new = palloc(sizeof(PLTSQL_stmt_close));
						new->cmd_type = PLTSQL_STMT_CLOSE;
						new->lineno = pltsql_location_to_lineno(@1);
						new->curvar = $2->dno;

						$$ = (PLTSQL_stmt *)new;
					}
				;

stmt_null		: K_NULL ';'
					{
						/* We do not bother building a node for NULL */
						$$ = NULL;
					}
				;

cursor_variable	: T_DATUM
					{
						if ($1.datum->dtype != PLTSQL_DTYPE_VAR)
							ereport(ERROR,
									(errcode(ERRCODE_DATATYPE_MISMATCH),
									 errmsg("cursor variable must be a simple variable"),
									 parser_errposition(@1)));

						if (((PLTSQL_var *) $1.datum)->datatype->typoid != REFCURSOROID)
							ereport(ERROR,
									(errcode(ERRCODE_DATATYPE_MISMATCH),
									 errmsg("variable \"%s\" must be of type cursor or refcursor",
											((PLTSQL_var *) $1.datum)->refname),
									 parser_errposition(@1)));
						$$ = (PLTSQL_var *) $1.datum;
					}
				| T_WORD
					{
						/* just to give a better message than "syntax error" */
						word_is_not_variable(&($1), @1);
					}
				| T_CWORD
					{
						/* just to give a better message than "syntax error" */
						cword_is_not_variable(&($1), @1);
					}
				;

exception_sect	:
					{ $$ = NULL; }
				| K_EXCEPTION
					{
						/*
						 * We use a mid-rule action to add these
						 * special variables to the namespace before
						 * parsing the WHEN clauses themselves.  The
						 * scope of the names extends to the end of the
						 * current block.
						 */
						int			lineno = pltsql_location_to_lineno(@1);
						PLTSQL_exception_block *new = palloc(sizeof(PLTSQL_exception_block));
						PLTSQL_variable *var;

						var = pltsql_build_variable("sqlstate", lineno,
													 pltsql_build_datatype(TEXTOID,
																			-1,
																			pltsql_curr_compile->fn_input_collation),
													 true);
						((PLTSQL_var *) var)->isconst = true;
						new->sqlstate_varno = var->dno;

						var = pltsql_build_variable("sqlerrm", lineno,
													 pltsql_build_datatype(TEXTOID,
																			-1,
																			pltsql_curr_compile->fn_input_collation),
													 true);
						((PLTSQL_var *) var)->isconst = true;
						new->sqlerrm_varno = var->dno;

						$<exception_block>$ = new;
					}
					proc_exceptions
					{
						PLTSQL_exception_block *new = $<exception_block>2;
						new->exc_list = $3;

						$$ = new;
					}
				;

proc_exceptions	: proc_exceptions proc_exception
						{
							$$ = lappend($1, $2);
						}
				| proc_exception
						{
							$$ = list_make1($1);
						}
				;

proc_exception	: K_WHEN proc_conditions K_THEN proc_sect
					{
						PLTSQL_exception *new;

						new = palloc0(sizeof(PLTSQL_exception));
						new->lineno = pltsql_location_to_lineno(@1);
						new->conditions = $2;
						new->action = $4;

						$$ = new;
					}
				;

proc_conditions	: proc_conditions K_OR proc_condition
						{
							PLTSQL_condition	*old;

							for (old = $1; old->next != NULL; old = old->next)
								/* skip */ ;
							old->next = $3;
							$$ = $1;
						}
				| proc_condition
						{
							$$ = $1;
						}
				;

proc_condition	: any_identifier
						{
							if (strcmp($1, "sqlstate") != 0)
							{
								$$ = pltsql_parse_err_condition($1);
							}
							else
							{
								PLTSQL_condition *new;
								char   *sqlstatestr;

								/* next token should be a string literal */
								if (yylex() != SCONST)
									yyerror("syntax error");
								sqlstatestr = yylval.str;

								if (strlen(sqlstatestr) != 5)
									yyerror("invalid SQLSTATE code");
								if (strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5)
									yyerror("invalid SQLSTATE code");

								new = palloc(sizeof(PLTSQL_condition));
								new->sqlerrstate =
									MAKE_SQLSTATE(sqlstatestr[0],
												  sqlstatestr[1],
												  sqlstatestr[2],
												  sqlstatestr[3],
												  sqlstatestr[4]);
								new->condname = sqlstatestr;
								new->next = NULL;

								$$ = new;
							}
						}
				;

expr_until_semi	:
				{ $$ = read_sql_expression(';', ";"); }
				;

expr_until_semi_or_bos :
					{ $$ = read_sql_expression_bos(';', ";"); }
					;

expr_until_bos :
					{ $$ = read_sql_expression_bos(0, ""); }
				;

expr_until_rightbracket :
					{ $$ = read_sql_expression(']', "]"); }
				;

expr_until_loop :
					{ $$ = read_sql_expression(K_LOOP, "LOOP"); }
				;

opt_block_label	:
					{
						pltsql_ns_push(NULL);
						$$ = NULL;
					}
				| LESS_LESS any_identifier GREATER_GREATER
					{
						pltsql_ns_push($2);
						$$ = $2;
					}
				;

opt_label	:
					{
						$$ = NULL;
					}
				| any_identifier
					{
						if (pltsql_ns_lookup_label(pltsql_ns_top(), $1) == NULL)
							yyerror("label does not exist");
						$$ = $1;
					}
				;

opt_exitcond	: ';'
					{ $$ = NULL; }
				| K_WHEN expr_until_semi
					{ $$ = $2; }
				;

/*
 * need both options because scanner will have tried to resolve as variable
 */
any_identifier	: T_WORD
					{
						$$ = $1.ident;
					}
				| T_DATUM
					{
						if ($1.ident == NULL) /* composite name not OK */
							yyerror("syntax error");
						$$ = $1.ident;
					}
				;

unreserved_keyword	:
				K_ABSOLUTE
				| K_ALIAS
				| K_ARRAY
				| K_BACKWARD
				| K_CONSTANT
				| K_CURRENT
				| K_CURSOR
				| K_DEBUG
				| K_DETAIL
				| K_DUMP
				| K_ERRCODE
				| K_ERROR
				| K_FIRST
				| K_FORWARD
				| K_HINT
				| K_INFO
				| K_IS
				| K_LAST
				| K_LOG
				| K_MESSAGE
				| K_MESSAGE_TEXT
				| K_NEXT
				| K_NO
				| K_NOTICE
				| K_OPTION
				| K_PG_EXCEPTION_CONTEXT
				| K_PG_EXCEPTION_DETAIL
				| K_PG_EXCEPTION_HINT
				| K_PRIOR
				| K_QUERY
				| K_RELATIVE
				| K_RESULT_OID
				| K_RETURNED_SQLSTATE
				| K_REVERSE
				| K_ROW_COUNT
				| K_ROWTYPE
				| K_SCROLL
				| K_SET
				| K_SLICE
				| K_SQLSTATE
				| K_STACKED
				| K_TYPE
				| K_USE_COLUMN
				| K_USE_VARIABLE
				| K_VARIABLE_CONFLICT
				| K_WARNING
				;

%%

/*
 * Check whether a token represents an "unreserved keyword".
 * We have various places where we want to recognize a keyword in preference
 * to a variable name, but not reserve that keyword in other contexts.
 * Hence, this kluge.
 */
static bool
tok_is_keyword(int token, union YYSTYPE *lval,
			   int kw_token, const char *kw_str)
{
	if (token == kw_token)
	{
		/* Normal case, was recognized by scanner (no conflicting variable) */
		return true;
	}
	else if (token == T_DATUM)
	{
		/*
		 * It's a variable, so recheck the string name.  Note we will not
		 * match composite names (hence an unreserved word followed by "."
		 * will not be recognized).
		 */
		if (!lval->wdatum.quoted && lval->wdatum.ident != NULL &&
			strcmp(lval->wdatum.ident, kw_str) == 0)
			return true;
	}
	return false;				/* not the keyword */
}

/*
 * Convenience routine to complain when we expected T_DATUM and got T_WORD,
 * ie, unrecognized variable.
 */
static void
word_is_not_variable(PLword *word, int location)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("\"%s\" is not a known variable",
					word->ident),
			 parser_errposition(location)));
}

/* Same, for a CWORD */
static void
cword_is_not_variable(PLcword *cword, int location)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("\"%s\" is not a known variable",
					NameListToString(cword->idents)),
			 parser_errposition(location)));
}

/*
 * Convenience routine to complain when we expected T_DATUM and got
 * something else.  "tok" must be the current token, since we also
 * look at yylval and yylloc.
 */
static void
current_token_is_not_variable(int tok)
{
	if (tok == T_WORD)
		word_is_not_variable(&(yylval.word), yylloc);
	else if (tok == T_CWORD)
		cword_is_not_variable(&(yylval.cword), yylloc);
	else
		yyerror("syntax error");
}

/* Convenience routine to read an expression with one possible terminator */
static PLTSQL_expr *
read_sql_expression(int until, const char *expected)
{
	return read_sql_construct(until, 0, 0, expected,
							  "SELECT ", true, true, true, NULL, NULL);
}

/* Convenience routine to read an expression with two possible terminators */
static PLTSQL_expr *
read_sql_expression2(int until, int until2, const char *expected,
					 int *endtoken)
{
	return read_sql_construct(until, until2, 0, expected,
							  "SELECT ", true, true, true, NULL, endtoken);
}

/*
 * Convenience routine to read an expression with an explicit or
 * beginning-of-statement token.
 */
static PLTSQL_expr *
read_sql_expression_bos(int until, const char *expected)
{
	return read_sql_construct_bos(until, 0, 0, expected,
							  "SELECT ", true, true, true, NULL, NULL, true);
}

/*
 * Convenience routine to read an expression with two possible explicit
 * terminators or a beginning-of-statement token.
 */
static PLTSQL_expr *
read_sql_expression2_bos(int until, int until2, const char *expected,
					 int *endtoken)
{
	return read_sql_construct_bos(until, until2, 0, expected,
							  "SELECT ", true, true, true, NULL, endtoken,
							  true);
}

/* Convenience routine to read a SQL statement that must end with ';' */
static PLTSQL_expr *
read_sql_stmt(const char *sqlstart)
{
	return read_sql_construct(';', 0, 0, ";",
                                  sqlstart, false, true, true, NULL, NULL);
}

/*
 * Convenience routine to read a SQL statement that must end with ';' or at
 * a beginning-of-statement token.
 */
static PLTSQL_expr *
read_sql_stmt_bos(const char *sqlstart)
{
	return read_sql_construct_bos(';', 0, 0, ";",
                                  sqlstart, false, true, true, NULL, NULL, true);
}

/*
 * Read a SQL construct and build a PLTSQL_expr for it.
 *
 * until:		token code for expected terminator
 * until2:		token code for alternate terminator (pass 0 if none)
 * until3:		token code for another alternate terminator (pass 0 if none)
 * expected:	text to use in complaining that terminator was not found
 * sqlstart:	text to prefix to the accumulated SQL text
 * isexpression: whether to say we're reading an "expression" or a "statement"
 * valid_sql:   whether to check the syntax of the expr (prefixed with sqlstart)
 * trim:		trim trailing whitespace
 * startloc:	if not NULL, location of first token is stored at *startloc
 * endtoken:	if not NULL, ending token is stored at *endtoken
 *				(this is only interesting if until2 or until3 isn't zero)
 */
static PLTSQL_expr *
read_sql_construct(int until,
				   int until2,
				   int until3,
				   const char *expected,
				   const char *sqlstart,
				   bool isexpression,
				   bool valid_sql,
				   bool trim,
				   int *startloc,
				   int *endtoken)
{
	return read_sql_construct_bos(until, until2, until3, expected, sqlstart,
							  isexpression, valid_sql, trim, startloc, endtoken,
							  false);
}

/*
 * Read a SQL construct and build a PLTSQL_expr for it, read until one of the
 * terminator tokens is encountered or optionally until the end of the line.
 *
 * until:		token code for expected terminator
 * until2:		token code for alternate terminator (pass 0 if none)
 * until3:		token code for another alternate terminator (pass 0 if none)
 * expected:	text to use in complaining that terminator was not found
 * sqlstart:	text to prefix to the accumulated SQL text
 * isexpression: whether to say we're reading an "expression" or a "statement"
 * valid_sql:   whether to check the syntax of the expr (prefixed with sqlstart)
 * trim:		trim trailing whitespace
 * startloc:	if not NULL, location of first token is stored at *startloc
 * endtoken:	if not NULL, ending token is stored at *endtoken
 *				(this is only interesting if until2 or until3 isn't zero)
 * untilbostok:	whether a beginning-of-statement token is a terminator
 */
static PLTSQL_expr *
read_sql_construct_bos(int until,
					   int until2,
					   int until3,
					   const char *expected,
					   const char *sqlstart,
					   bool isexpression,
					   bool valid_sql,
					   bool trim,
					   int *startloc,
					   int *endtoken,
					   bool untilbostok)
{
	int					tok;
	StringInfoData		ds;
	IdentifierLookup	save_IdentifierLookup;
	int					startlocation = -1;
	int					parenlevel = 0;
	int					sqlstartlen = strlen(sqlstart);
	PLTSQL_expr			*expr;
	List				*tsql_idents = NIL;

	initStringInfo(&ds);
	appendStringInfoString(&ds, sqlstart);

	/* special lookup mode for identifiers within the SQL text */
	save_IdentifierLookup = pltsql_IdentifierLookup;
	pltsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;

	for (;;)
	{
		tok = yylex();
		if (startlocation < 0)			/* remember loc of first token */
			startlocation = yylloc;
		if (tok == until && parenlevel == 0)
			break;
		if (tok == until2 && parenlevel == 0)
			break;
		if (tok == until3 && parenlevel == 0)
			break;
		if (untilbostok &&
		    is_terminator(tok,
		                  (startlocation == yylloc),
		                  startlocation, yylloc, sqlstart, tsql_idents) &&
		    parenlevel == 0)
		{
			pltsql_push_back_token(tok);
			break;
		}

		if (tok == '(' || tok == '[')
			parenlevel++;
		else if (tok == ')' || tok == ']')
		{
			parenlevel--;
			if (parenlevel < 0)
				yyerror("mismatched parentheses");
		}

		tsql_idents = append_if_tsql_identifier(tok, sqlstartlen, startlocation,
		                                     tsql_idents);

		/*
		 * End of function definition is an error, and we don't expect to
		 * hit a semicolon either (unless it's the until symbol, in which
		 * case we should have fallen out above).
		 */
		if (tok == 0 || tok == ';')
		{
			if (parenlevel != 0)
				yyerror("mismatched parentheses");
			if (isexpression)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("missing \"%s\" at end of SQL expression",
								expected),
						 parser_errposition(yylloc)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("missing \"%s\" at end of SQL statement",
								expected),
						 parser_errposition(yylloc)));
		}
	}

	pltsql_IdentifierLookup = save_IdentifierLookup;

	if (startloc)
		*startloc = startlocation;
	if (endtoken)
		*endtoken = tok;

	/* give helpful complaint about empty input */
	if (startlocation >= yylloc)
	{
		if (isexpression)
			yyerror("missing expression");
		else
			yyerror("missing SQL statement");
	}

	pltsql_append_source_text(&ds, startlocation, yylloc);

	/* trim any trailing whitespace, for neatness */
	if (trim)
	{
		while (ds.len > 0 && scanner_isspace(ds.data[ds.len - 1]))
			ds.data[--ds.len] = '\0';
	}

	expr = palloc0(sizeof(PLTSQL_expr));
	expr->dtype			= PLTSQL_DTYPE_EXPR;
	expr->query			= pstrdup(quote_tsql_identifiers(&ds, tsql_idents));
	expr->plan			= NULL;
	expr->paramnos		= NULL;
	expr->rwparam		= -1;
	expr->ns			= pltsql_ns_top();
	pfree(ds.data);

	if (valid_sql)
		check_sql_expr(expr->query, startlocation, strlen(sqlstart));
	return expr;
}

static List *
append_if_tsql_identifier(int tok, int start_len, int start_loc,
						List *tsql_idents)
{
	char				*ident;
	tsql_ident_ref		*tident_ref;
	int					tsql_ident_len;
	bool				is_ident_quoted;

	ident = NULL;
	
	if (tok == T_WORD)
	{
		ident = yylval.word.ident;
		is_ident_quoted = yylval.word.quoted;
	}
	else if (tok == T_DATUM)
	{
		ident = NameOfDatum(&(yylval.wdatum));
		is_ident_quoted = yylval.wdatum.quoted;
	}

	/*
	 * Create and append a reference to this word if it is a TSQL (at-prefixed)
	 * identifier.
	 */
	if (ident && !is_ident_quoted)
	{
		tsql_ident_len = strlen(ident);

		if (tsql_ident_len > 0 && (ident[0] == '@' || ident[0] == '#'))
		{
			tident_ref = palloc(sizeof(tsql_ident_ref));
			tident_ref->location = (start_len - 1) +
				(yylloc - start_loc);
			tident_ref->ident = pstrdup(ident);
			tsql_idents = lappend(tsql_idents, tident_ref);
		}
	}

	return tsql_idents;
}

static char *
quote_tsql_identifiers(const StringInfo src, const List *tsql_idents)
{
	StringInfoData	dest;
	ListCell		*lc;
	int				prev = 0;

	if (list_length(tsql_idents) == 0)
		return src->data;

	initStringInfo(&dest);

	foreach(lc, tsql_idents)
	{
		tsql_ident_ref *tword = (tsql_ident_ref *) lfirst(lc);

		/*
		 * Append the part of the source text appearing before the identifier
		 * that we haven't inserted already.
		 */
		appendBinaryStringInfo(&dest, &(src->data[prev]),
							   tword->location - prev + 1);
		appendStringInfo(&dest, "\"%s\"", tword->ident);
		prev = tword->location + 1 + strlen(tword->ident);
	}

	appendStringInfoString(&dest, &(src->data[prev]));
	return dest.data;
}

/*
 * Determines whether there the specified token is a statement terminator.
 *
 * "tok" must be the current token, since we also look at yylval.
 */
static bool
is_terminator(int tok, bool first, int start_loc, int cur_loc,
			  const char *sql_start, const List *tsql_idents)
{
	StringInfoData	ds;
	bool			validsql = true;
	MemoryContext	oldcontext = CurrentMemoryContext;

	switch (tok)
	{
		/* Ambiguous tokens not included: NULL */
		case ';':
		case K_BEGIN:
		case K_CLOSE:
		case K_DECLARE:
		case K_EXIT:
		case K_FETCH:
		case K_FOR:
		case K_FOREACH:
		case K_GET:
		case K_IF:
		case K_INSERT:
		case K_MOVE:
		case K_OPEN:
		case K_PERFORM:
		case K_PRINT:
		case K_RAISE:
		case K_RETURN:
		case K_SET:
		case K_WHILE:
		case LESS_LESS:			/* Optional label for many statements */
		/*
		 * These are not core TSQL statements but are needed to support dual
		 * syntax, in particular, the PL/pgSQL syntax for their respective
		 * statements.
		 */
		case K_LOOP:
			return true;
		/*
		 * We work harder in the ambiguous cases and perform a basic syntax
		 * analysis to guide us.
		 */
		case K_ELSE:			/* Ambiguous: CASE expr versus IF-ELSE stmt */
		case K_END:				/* Ambiguous: CASE expr versus block END */

			if (first)
				yyerror("syntax error");
			
			initStringInfo(&ds);
			
			if (sql_start)
				appendStringInfoString(&ds, sql_start);

			pltsql_append_source_text(&ds, start_loc, cur_loc);

			PG_TRY();
			{
				check_sql_expr(quote_tsql_identifiers(&ds, tsql_idents),
				               start_loc,
				               sql_start ? strlen(sql_start) : 0);
			}
			PG_CATCH();
			{
				MemoryContextSwitchTo(oldcontext);
				FlushErrorState();
				validsql = false;
			}
			PG_END_TRY();
			return validsql;
	}

	/* List of words that are not tokens but mark the beginning of a statement. */
	return word_matches(tok, "UPDATE") ||
		   word_matches(tok, "DELETE") ||
		   (!first && word_matches(tok, "SELECT"));
}

static bool
word_matches(int tok, const char *pattern)
{
	return ((tok == T_WORD) &&
	        (pg_strcasecmp(yylval.word.ident, pattern) == 0));
}

static PLTSQL_type *
read_datatype(int tok)
{
	StringInfoData		ds;
	char			   *type_name;
	int					startlocation;
	PLTSQL_type		*result;
	int					parenlevel = 0;
	bool				first = true;

	/* Should only be called while parsing DECLARE sections */
	Assert(pltsql_IdentifierLookup == IDENTIFIER_LOOKUP_DECLARE);

	/* Often there will be a lookahead token, but if not, get one */
	if (tok == YYEMPTY)
		tok = yylex();

	startlocation = yylloc;

	/*
	 * If we have a simple or composite identifier, check for %TYPE
	 * and %ROWTYPE constructs.
	 */
	if (tok == T_WORD)
	{
		char   *dtname = yylval.word.ident;

		tok = yylex();
		if (tok == '%')
		{
			tok = yylex();
			if (tok_is_keyword(tok, &yylval,
							   K_TYPE, "type"))
			{
				result = pltsql_parse_wordtype(dtname);
				if (result)
					return result;
			}
			else if (tok_is_keyword(tok, &yylval,
									K_ROWTYPE, "rowtype"))
			{
				result = pltsql_parse_wordrowtype(dtname);
				if (result)
					return result;
			}
		}
	}
	else if (tok == T_CWORD)
	{
		List   *dtnames = yylval.cword.idents;

		tok = yylex();
		if (tok == '%')
		{
			tok = yylex();
			if (tok_is_keyword(tok, &yylval,
							   K_TYPE, "type"))
			{
				result = pltsql_parse_cwordtype(dtnames);
				if (result)
					return result;
			}
			else if (tok_is_keyword(tok, &yylval,
									K_ROWTYPE, "rowtype"))
			{
				result = pltsql_parse_cwordrowtype(dtnames);
				if (result)
					return result;
			}
		}
	}

	while (tok != ';')
	{
		if (tok == 0)
		{
			if (parenlevel != 0)
				yyerror("mismatched parentheses");
			else
				yyerror("incomplete data type declaration");
		}
		/* Possible followers for datatype in a declaration */
		if (tok == K_COLLATE || tok == K_NOT ||
		    tok == '=' || tok == COLON_EQUALS || tok == K_DEFAULT)
			break;
		/* Possible followers for datatype in a cursor_arg list */
		if ((tok == ',' || tok == ')') && parenlevel == 0)
			break;
		if (tok == '(')
			parenlevel++;
		else if (tok == ')')
			parenlevel--;
		if (is_terminator(tok, first, startlocation, yylloc, NULL, NIL) &&
		    parenlevel == 0)
			break;

		tok = yylex();
		first = false;
	}

	/* set up ds to contain complete typename text */
	initStringInfo(&ds);
	pltsql_append_source_text(&ds, startlocation, yylloc);

	/* trim any trailing whitespace, for neatness */
	while (ds.len > 0 && scanner_isspace(ds.data[ds.len - 1]))
		ds.data[--ds.len] = '\0';

	type_name = ds.data;

	if (type_name[0] == '\0')
		yyerror("missing data type declaration");

	result = parse_datatype(type_name, startlocation);

	pfree(ds.data);

	pltsql_push_back_token(tok);

	return result;
}

static PLTSQL_stmt *
make_execsql_stmt(int firsttoken, int location, PLword *firstword)
{
	StringInfoData		ds;
	IdentifierLookup	save_IdentifierLookup;
	PLTSQL_stmt_execsql *execsql;
	PLTSQL_expr			*expr;
	PLTSQL_variable		*target = NULL;
	int					tok;
	int					prev_tok;
	bool				have_into = false;
	bool				have_strict = false;
	bool				have_temptbl = false;
	bool				is_prev_tok_create = false;
	int					into_start_loc = -1;
	int					into_end_loc = -1;
	int					temptbl_loc = -1;
	List				*tsql_idents = NIL;

	initStringInfo(&ds);

	/* special lookup mode for identifiers within the SQL text */
	save_IdentifierLookup = pltsql_IdentifierLookup;
	pltsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;

	/*
	 * We have to special-case the sequence INSERT INTO, because we don't want
	 * that to be taken as an INTO-variables clause.  Fortunately, this is the
	 * only valid use of INTO in a pl/pgsql SQL command, and INTO is already a
	 * fully reserved word in the main grammar.  We have to treat it that way
	 * anywhere in the string, not only at the start; consider CREATE RULE
	 * containing an INSERT statement.
	 */
	tok = firsttoken;
	is_prev_tok_create =
		(firstword && (pg_strcasecmp(firstword->ident, "CREATE") == 0));
	for (;;)
	{
		prev_tok = tok;
		tok = yylex();
		if (have_into && into_end_loc < 0)
			into_end_loc = yylloc;		/* token after the INTO part */
		if (tok == ';')
			break;
		tsql_idents = append_if_tsql_identifier(tok,
		                                        (have_temptbl ?
		                                         strlen(TEMPOBJ_QUALIFIER) : 0),
		                                        location, tsql_idents);
		if (tok == 0)
			yyerror("unexpected end of function definition");

		if (tok == K_INTO && prev_tok != K_INSERT)
		{
			if (have_into)
				yyerror("INTO specified more than once");
			have_into = true;
			into_start_loc = yylloc;
			pltsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
			read_into_target(&target, &have_strict);
			pltsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;
		}
		/*
		 * We need to identify CREATE TABLE <#ident> as a local temporary table
		 * so we can translate it to a CREATE TEMPORARY TABLE statement later.
		 */
		if (is_prev_tok_create && word_matches(tok, "TABLE"))
		{
			temptbl_loc = yylloc;

			tok = yylex();
			if (tok == T_WORD && (!yylval.word.quoted) &&
			    (yylval.word.ident[0] == '#'))
				have_temptbl = true;

			pltsql_push_back_token(tok);
		}
		/* See the call to check_sql_expr below if you change this */
		is_prev_tok_create = false;
	}

	pltsql_IdentifierLookup = save_IdentifierLookup;

	if (have_into)
	{
		/*
		 * Insert an appropriate number of spaces corresponding to the
		 * INTO text, so that locations within the redacted SQL statement
		 * still line up with those in the original source text.
		 */
		pltsql_append_source_text(&ds, location, into_start_loc);
		appendStringInfoSpaces(&ds, into_end_loc - into_start_loc);
		pltsql_append_source_text(&ds, into_end_loc, yylloc);
	}
	else
	{
		if (have_temptbl)
		{
			/*
			 * We have a local temporary table identifier after the CREATE
			 * TABLE tokens, we need to transform CREATE TABLE -> CREATE
			 * TEMPORARY TABLE in this case.
			 */
			pltsql_append_source_text(&ds, location, temptbl_loc);
			appendStringInfoString(&ds, TEMPOBJ_QUALIFIER);
			pltsql_append_source_text(&ds, temptbl_loc, yylloc);
		}
		else
		{
			pltsql_append_source_text(&ds, location, yylloc);
		}
	}

	/* trim any trailing whitespace, for neatness */
	while (ds.len > 0 && scanner_isspace(ds.data[ds.len - 1]))
		ds.data[--ds.len] = '\0';

	expr = palloc0(sizeof(PLTSQL_expr));
	expr->dtype			= PLTSQL_DTYPE_EXPR;
	expr->query			= pstrdup(quote_tsql_identifiers(&ds, tsql_idents));
	expr->plan			= NULL;
	expr->paramnos		= NULL;
	expr->rwparam		= -1;
	expr->ns			= pltsql_ns_top();
	pfree(ds.data);

	/*
	 * If have_temptbl is true, the first two tokens were valid so we expect
	 * that check_sql_expr will raise errors from a location occurring after
	 * the TEMPORARY token.  Because the original statement did not include it,
	 * we offset the error location with its length so it points back to the
	 * correct location in the original source.
	 */
	check_sql_expr(expr->query, location, (have_temptbl ?
	                                       strlen(TEMPOBJ_QUALIFIER) : 0));

	execsql = palloc(sizeof(PLTSQL_stmt_execsql));
	execsql->cmd_type = PLTSQL_STMT_EXECSQL;
	execsql->lineno  = pltsql_location_to_lineno(location);
	execsql->sqlstmt = expr;
	execsql->into	 = have_into;
	execsql->strict	 = have_strict;
	execsql->target	 = target;

	return (PLTSQL_stmt *) execsql;
}


/*
 * Read FETCH or MOVE direction clause (everything through FROM/IN).
 */
static PLTSQL_stmt_fetch *
read_fetch_direction(void)
{
	PLTSQL_stmt_fetch *fetch;
	int			tok;
	bool		check_FROM = true;

	/*
	 * We create the PLTSQL_stmt_fetch struct here, but only fill in
	 * the fields arising from the optional direction clause
	 */
	fetch = (PLTSQL_stmt_fetch *) palloc0(sizeof(PLTSQL_stmt_fetch));
	fetch->cmd_type = PLTSQL_STMT_FETCH;
	/* set direction defaults: */
	fetch->direction = FETCH_FORWARD;
	fetch->how_many  = 1;
	fetch->expr		 = NULL;
	fetch->returns_multiple_rows = false;

	tok = yylex();
	if (tok == 0)
		yyerror("unexpected end of function definition");

	if (tok_is_keyword(tok, &yylval,
					   K_NEXT, "next"))
	{
		/* use defaults */
	}
	else if (tok_is_keyword(tok, &yylval,
							K_PRIOR, "prior"))
	{
		fetch->direction = FETCH_BACKWARD;
	}
	else if (tok_is_keyword(tok, &yylval,
							K_FIRST, "first"))
	{
		fetch->direction = FETCH_ABSOLUTE;
	}
	else if (tok_is_keyword(tok, &yylval,
							K_LAST, "last"))
	{
		fetch->direction = FETCH_ABSOLUTE;
		fetch->how_many  = -1;
	}
	else if (tok_is_keyword(tok, &yylval,
							K_ABSOLUTE, "absolute"))
	{
		fetch->direction = FETCH_ABSOLUTE;
		fetch->expr = read_sql_expression2(K_FROM, K_IN,
										   "FROM or IN",
										   NULL);
		check_FROM = false;
	}
	else if (tok_is_keyword(tok, &yylval,
							K_RELATIVE, "relative"))
	{
		fetch->direction = FETCH_RELATIVE;
		fetch->expr = read_sql_expression2(K_FROM, K_IN,
										   "FROM or IN",
										   NULL);
		check_FROM = false;
	}
	else if (tok_is_keyword(tok, &yylval,
							K_ALL, "all"))
	{
		fetch->how_many = FETCH_ALL;
		fetch->returns_multiple_rows = true;
	}
	else if (tok_is_keyword(tok, &yylval,
							K_FORWARD, "forward"))
	{
		complete_direction(fetch, &check_FROM);
	}
	else if (tok_is_keyword(tok, &yylval,
							K_BACKWARD, "backward"))
	{
		fetch->direction = FETCH_BACKWARD;
		complete_direction(fetch, &check_FROM);
	}
	else if (tok == K_FROM || tok == K_IN)
	{
		/* empty direction */
		check_FROM = false;
	}
	else if (tok == T_DATUM)
	{
		/* Assume there's no direction clause and tok is a cursor name */
		pltsql_push_back_token(tok);
		check_FROM = false;
	}
	else
	{
		/*
		 * Assume it's a count expression with no preceding keyword.
		 * Note: we allow this syntax because core SQL does, but we don't
		 * document it because of the ambiguity with the omitted-direction
		 * case.  For instance, "MOVE n IN c" will fail if n is a variable.
		 * Perhaps this can be improved someday, but it's hardly worth a
		 * lot of work.
		 */
		pltsql_push_back_token(tok);
		fetch->expr = read_sql_expression2(K_FROM, K_IN,
										   "FROM or IN",
										   NULL);
		fetch->returns_multiple_rows = true;
		check_FROM = false;
	}

	/* check FROM or IN keyword after direction's specification */
	if (check_FROM)
	{
		tok = yylex();
		if (tok != K_FROM && tok != K_IN)
			yyerror("expected FROM or IN");
	}

	return fetch;
}

/*
 * Process remainder of FETCH/MOVE direction after FORWARD or BACKWARD.
 * Allows these cases:
 *   FORWARD expr,  FORWARD ALL,  FORWARD
 *   BACKWARD expr, BACKWARD ALL, BACKWARD
 */
static void
complete_direction(PLTSQL_stmt_fetch *fetch,  bool *check_FROM)
{
	int			tok;

	tok = yylex();
	if (tok == 0)
		yyerror("unexpected end of function definition");

	if (tok == K_FROM || tok == K_IN)
	{
		*check_FROM = false;
		return;
	}

	if (tok == K_ALL)
	{
		fetch->how_many = FETCH_ALL;
		fetch->returns_multiple_rows = true;
		*check_FROM = true;
		return;
	}

	pltsql_push_back_token(tok);
	fetch->expr = read_sql_expression2(K_FROM, K_IN,
									   "FROM or IN",
									   NULL);
	fetch->returns_multiple_rows = true;
	*check_FROM = false;
}


static PLTSQL_stmt *
make_return_stmt(int location)
{
	PLTSQL_stmt_return *new;

	new = palloc0(sizeof(PLTSQL_stmt_return));
	new->cmd_type = PLTSQL_STMT_RETURN;
	new->lineno   = pltsql_location_to_lineno(location);
	new->expr	  = NULL;
	new->retvarno = -1;

	if (pltsql_curr_compile->fn_retset)
	{
		if (yylex() != ';')
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("RETURN cannot have a parameter in function returning set"),
					 errhint("Use RETURN NEXT or RETURN QUERY."),
					 parser_errposition(yylloc)));
	}
	else if (pltsql_curr_compile->out_param_varno >= 0)
	{
		if (yylex() != ';')
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("RETURN cannot have a parameter in function with OUT parameters"),
					 parser_errposition(yylloc)));
		new->retvarno = pltsql_curr_compile->out_param_varno;
	}
	else if (pltsql_curr_compile->fn_rettype == VOIDOID)
	{
		if (yylex() != ';')
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("RETURN cannot have a parameter in function returning void"),
					 parser_errposition(yylloc)));
	}
	else if (pltsql_curr_compile->fn_retistuple)
	{
		switch (yylex())
		{
			case K_NULL:
				/* we allow this to support RETURN NULL in triggers */
				break;

			case T_DATUM:
				if (yylval.wdatum.datum->dtype == PLTSQL_DTYPE_ROW ||
					yylval.wdatum.datum->dtype == PLTSQL_DTYPE_REC)
					new->retvarno = yylval.wdatum.datum->dno;
				else
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("RETURN must specify a record or row variable in function returning row"),
							 parser_errposition(yylloc)));
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("RETURN must specify a record or row variable in function returning row"),
						 parser_errposition(yylloc)));
				break;
		}
		if (yylex() != ';')
			yyerror("syntax error");
	}
	else
	{
		/*
		 * Note that a well-formed expression is _required_ here;
		 * anything else is a compile-time error.
		 */
		new->expr = read_sql_expression(';', ";");
	}

	return (PLTSQL_stmt *) new;
}


static PLTSQL_stmt *
make_return_next_stmt(int location)
{
	PLTSQL_stmt_return_next *new;

	if (!pltsql_curr_compile->fn_retset)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot use RETURN NEXT in a non-SETOF function"),
				 parser_errposition(location)));

	new = palloc0(sizeof(PLTSQL_stmt_return_next));
	new->cmd_type	= PLTSQL_STMT_RETURN_NEXT;
	new->lineno		= pltsql_location_to_lineno(location);
	new->expr		= NULL;
	new->retvarno	= -1;

	if (pltsql_curr_compile->out_param_varno >= 0)
	{
		if (yylex() != ';')
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("RETURN NEXT cannot have a parameter in function with OUT parameters"),
					 parser_errposition(yylloc)));
		new->retvarno = pltsql_curr_compile->out_param_varno;
	}
	else if (pltsql_curr_compile->fn_retistuple)
	{
		switch (yylex())
		{
			case T_DATUM:
				if (yylval.wdatum.datum->dtype == PLTSQL_DTYPE_ROW ||
					yylval.wdatum.datum->dtype == PLTSQL_DTYPE_REC)
					new->retvarno = yylval.wdatum.datum->dno;
				else
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("RETURN NEXT must specify a record or row variable in function returning row"),
							 parser_errposition(yylloc)));
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("RETURN NEXT must specify a record or row variable in function returning row"),
						 parser_errposition(yylloc)));
				break;
		}
		if (yylex() != ';')
			yyerror("syntax error");
	}
	else
		new->expr = read_sql_expression(';', ";");

	return (PLTSQL_stmt *) new;
}


static PLTSQL_stmt *
make_return_query_stmt(int location)
{
	PLTSQL_stmt_return_query *new;
	int			tok;

	if (!pltsql_curr_compile->fn_retset)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot use RETURN QUERY in a non-SETOF function"),
				 parser_errposition(location)));

	new = palloc0(sizeof(PLTSQL_stmt_return_query));
	new->cmd_type = PLTSQL_STMT_RETURN_QUERY;
	new->lineno = pltsql_location_to_lineno(location);

	/* check for RETURN QUERY EXECUTE */
	if ((tok = yylex()) != K_EXECUTE)
	{
		/* ordinary static query */
		pltsql_push_back_token(tok);
		new->query = read_sql_stmt("");
	}
	else
	{
		/* dynamic SQL */
		int		term;

		new->dynquery = read_sql_expression2(';', K_USING, "; or USING",
											 &term);
		if (term == K_USING)
		{
			do
			{
				PLTSQL_expr *expr;

				expr = read_sql_expression2(',', ';', ", or ;", &term);
				new->params = lappend(new->params, expr);
			} while (term == ',');
		}
	}

	return (PLTSQL_stmt *) new;
}


/* convenience routine to fetch the name of a T_DATUM */
static char *
NameOfDatum(PLwdatum *wdatum)
{
	if (wdatum->ident)
		return wdatum->ident;
	Assert(wdatum->idents != NIL);
	return NameListToString(wdatum->idents);
}

static void
check_assignable(PLTSQL_datum *datum, int location)
{
	switch (datum->dtype)
	{
		case PLTSQL_DTYPE_VAR:
			if (((PLTSQL_var *) datum)->isconst)
				ereport(ERROR,
						(errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
						 errmsg("\"%s\" is declared CONSTANT",
								((PLTSQL_var *) datum)->refname),
						 parser_errposition(location)));
			break;
		case PLTSQL_DTYPE_ROW:
			/* always assignable? */
			break;
		case PLTSQL_DTYPE_REC:
			/* always assignable?  What about NEW/OLD? */
			break;
		case PLTSQL_DTYPE_RECFIELD:
			/* always assignable? */
			break;
		case PLTSQL_DTYPE_ARRAYELEM:
			/* always assignable? */
			break;
		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			break;
	}
}
#if 1
/*
 * Read the argument of an INTO clause.  On entry, we have just read the
 * INTO keyword.
 */
static void
read_into_target(PLTSQL_variable **target, bool *strict)
{
	int			tok;

	/* Set default results */
	*target = NULL;
	if (strict)
		*strict = false;

	tok = yylex();
	if (strict && tok == K_STRICT)
	{
		*strict = true;
		tok = yylex();
	}

	/*
	 * Currently, a row or record variable can be the single INTO target,
	 * but not a member of a multi-target list.  So we throw error if there
	 * is a comma after it, because that probably means the user tried to
	 * write a multi-target list.  If this ever gets generalized, we should
	 * probably refactor read_into_scalar_list so it handles all cases.
	 */
	switch (tok)
	{
		case T_DATUM:
			if (yylval.wdatum.datum->dtype == PLTSQL_DTYPE_ROW ||
				yylval.wdatum.datum->dtype == PLTSQL_DTYPE_REC)
			{
				check_assignable(yylval.wdatum.datum, yylloc);
				*target = (PLTSQL_variable *) yylval.wdatum.datum;

				if ((tok = yylex()) == ',')
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("record variable cannot be part of multiple-item INTO list"),
							 parser_errposition(yylloc)));
				pltsql_push_back_token(tok);
			}
			else
			{
				*target = (PLTSQL_variable *)
					read_into_scalar_list(NameOfDatum(&(yylval.wdatum)),
										  yylval.wdatum.datum, yylloc);
			}
			break;

		default:
			/* just to give a better message than "syntax error" */
			current_token_is_not_variable(tok);
	}
}
#else
/*
 * Read the argument of an INTO clause.  On entry, we have just read the
 * INTO keyword.
 */
static void
read_into_target(PLTSQL_rec **rec, PLTSQL_row **row, bool *strict)
{
	int			tok;

	/* Set default results */
	*rec = NULL;
	*row = NULL;
	if (strict)
		*strict = false;

	tok = yylex();
	if (strict && tok == K_STRICT)
	{
		*strict = true;
		tok = yylex();
	}

	/*
	 * Currently, a row or record variable can be the single INTO target,
	 * but not a member of a multi-target list.  So we throw error if there
	 * is a comma after it, because that probably means the user tried to
	 * write a multi-target list.  If this ever gets generalized, we should
	 * probably refactor read_into_scalar_list so it handles all cases.
	 */
	switch (tok)
	{
		case T_DATUM:
			if (yylval.wdatum.datum->dtype == PLTSQL_DTYPE_ROW)
			{
				check_assignable(yylval.wdatum.datum, yylloc);
				*row = (PLTSQL_row *) yylval.wdatum.datum;

				if ((tok = yylex()) == ',')
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("record or row variable cannot be part of multiple-item INTO list"),
							 parser_errposition(yylloc)));
				pltsql_push_back_token(tok);
			}
			else if (yylval.wdatum.datum->dtype == PLTSQL_DTYPE_REC)
			{
				check_assignable(yylval.wdatum.datum, yylloc);
				*rec = (PLTSQL_rec *) yylval.wdatum.datum;

				if ((tok = yylex()) == ',')
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("record or row variable cannot be part of multiple-item INTO list"),
							 parser_errposition(yylloc)));
				pltsql_push_back_token(tok);
			}
			else
			{
				*row = read_into_scalar_list(NameOfDatum(&(yylval.wdatum)),
											 yylval.wdatum.datum, yylloc);
			}
			break;

		default:
			/* just to give a better message than "syntax error" */
			current_token_is_not_variable(tok);
	}
}
#endif
/*
 * Given the first datum and name in the INTO list, continue to read
 * comma-separated scalar variables until we run out. Then construct
 * and return a fake "row" variable that represents the list of
 * scalars.
 */
static PLTSQL_row *
read_into_scalar_list(char *initial_name,
					  PLTSQL_datum *initial_datum,
					  int initial_location)
{
	int				 nfields;
	char			*fieldnames[1024];
	int				 varnos[1024];
	PLTSQL_row		*row;
	int				 tok;

	check_assignable(initial_datum, initial_location);
	fieldnames[0] = initial_name;
	varnos[0]	  = initial_datum->dno;
	nfields		  = 1;

	while ((tok = yylex()) == ',')
	{
		/* Check for array overflow */
		if (nfields >= 1024)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("too many INTO variables specified"),
					 parser_errposition(yylloc)));

		tok = yylex();
		switch (tok)
		{
			case T_DATUM:
				check_assignable(yylval.wdatum.datum, yylloc);
				if (yylval.wdatum.datum->dtype == PLTSQL_DTYPE_ROW ||
					yylval.wdatum.datum->dtype == PLTSQL_DTYPE_REC)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("\"%s\" is not a scalar variable",
									NameOfDatum(&(yylval.wdatum))),
							 parser_errposition(yylloc)));
				fieldnames[nfields] = NameOfDatum(&(yylval.wdatum));
				varnos[nfields++]	= yylval.wdatum.datum->dno;
				break;

			default:
				/* just to give a better message than "syntax error" */
				current_token_is_not_variable(tok);
		}
	}

	/*
	 * We read an extra, non-comma token from yylex(), so push it
	 * back onto the input stream
	 */
	pltsql_push_back_token(tok);

	row = palloc(sizeof(PLTSQL_row));
	row->dtype = PLTSQL_DTYPE_ROW;
	row->refname = pstrdup("*internal*");
	row->lineno = pltsql_location_to_lineno(initial_location);
	row->rowtupdesc = NULL;
	row->nfields = nfields;
	row->fieldnames = palloc(sizeof(char *) * nfields);
	row->varnos = palloc(sizeof(int) * nfields);
	while (--nfields >= 0)
	{
		row->fieldnames[nfields] = fieldnames[nfields];
		row->varnos[nfields] = varnos[nfields];
	}

	pltsql_adddatum((PLTSQL_datum *)row);

	return row;
}

/*
 * Convert a single scalar into a "row" list.  This is exactly
 * like read_into_scalar_list except we never consume any input.
 *
 * Note: lineno could be computed from location, but since callers
 * have it at hand already, we may as well pass it in.
 */
static PLTSQL_row *
make_scalar_list1(char *initial_name,
				  PLTSQL_datum *initial_datum,
				  int lineno, int location)
{
	PLTSQL_row		*row;

	check_assignable(initial_datum, location);

	row = palloc(sizeof(PLTSQL_row));
	row->dtype = PLTSQL_DTYPE_ROW;
	row->refname = pstrdup("*internal*");
	row->lineno = lineno;
	row->rowtupdesc = NULL;
	row->nfields = 1;
	row->fieldnames = palloc(sizeof(char *));
	row->varnos = palloc(sizeof(int));
	row->fieldnames[0] = initial_name;
	row->varnos[0] = initial_datum->dno;

	pltsql_adddatum((PLTSQL_datum *)row);

	return row;
}

/*
 * When the PL/TSQL parser expects to see a SQL statement, it is very
 * liberal in what it accepts; for example, we often assume an
 * unrecognized keyword is the beginning of a SQL statement. This
 * avoids the need to duplicate parts of the SQL grammar in the
 * PL/TSQL grammar, but it means we can accept wildly malformed
 * input. To try and catch some of the more obviously invalid input,
 * we run the strings we expect to be SQL statements through the main
 * SQL parser.
 *
 * We only invoke the raw parser (not the analyzer); this doesn't do
 * any database access and does not check any semantic rules, it just
 * checks for basic syntactic correctness. We do this here, rather
 * than after parsing has finished, because a malformed SQL statement
 * may cause the PL/TSQL parser to become confused about statement
 * borders. So it is best to bail out as early as we can.
 *
 * It is assumed that "stmt" represents a copy of the function source text
 * beginning at offset "location", with leader text of length "leaderlen"
 * (typically "SELECT ") prefixed to the source text.  We use this assumption
 * to transpose any error cursor position back to the function source text.
 * If no error cursor is provided, we'll just point at "location".
 */
static void
check_sql_expr(const char *stmt, int location, int leaderlen)
{
	sql_error_callback_arg cbarg;
	ErrorContextCallback  syntax_errcontext;
	MemoryContext oldCxt;

	if (!pltsql_check_syntax)
		return;

	cbarg.location = location;
	cbarg.leaderlen = leaderlen;

	syntax_errcontext.callback = pltsql_sql_error_callback;
	syntax_errcontext.arg = &cbarg;
	syntax_errcontext.previous = error_context_stack;
	error_context_stack = &syntax_errcontext;

	oldCxt = MemoryContextSwitchTo(compile_tmp_cxt);
	(void) raw_parser(stmt);
	MemoryContextSwitchTo(oldCxt);

	/* Restore former ereport callback */
	error_context_stack = syntax_errcontext.previous;
}

static void
pltsql_sql_error_callback(void *arg)
{
	sql_error_callback_arg *cbarg = (sql_error_callback_arg *) arg;
	int			errpos;

	/*
	 * First, set up internalerrposition to point to the start of the
	 * statement text within the function text.  Note this converts
	 * location (a byte offset) to a character number.
	 */
	parser_errposition(cbarg->location);

	/*
	 * If the core parser provided an error position, transpose it.
	 * Note we are dealing with 1-based character numbers at this point.
	 */
	errpos = geterrposition();
	if (errpos > cbarg->leaderlen)
	{
		int		myerrpos = getinternalerrposition();

		if (myerrpos > 0)		/* safety check */
			internalerrposition(myerrpos + errpos - cbarg->leaderlen - 1);
	}

	/* In any case, flush errposition --- we want internalerrpos only */
	errposition(0);
}

/*
 * Parse a SQL datatype name and produce a PLTSQL_type structure.
 *
 * The heavy lifting is done elsewhere.  Here we are only concerned
 * with setting up an errcontext link that will let us give an error
 * cursor pointing into the pltsql function source, if necessary.
 * This is handled the same as in check_sql_expr(), and we likewise
 * expect that the given string is a copy from the source text.
 */
static PLTSQL_type *
parse_datatype(const char *string, int location)
{
	Oid			type_id;
	int32		typmod;
	sql_error_callback_arg cbarg;
	ErrorContextCallback  syntax_errcontext;

	cbarg.location = location;
	cbarg.leaderlen = 0;

	syntax_errcontext.callback = pltsql_sql_error_callback;
	syntax_errcontext.arg = &cbarg;
	syntax_errcontext.previous = error_context_stack;
	error_context_stack = &syntax_errcontext;

	/* Let the main parser try to parse it under standard SQL rules */
	parseTypeString(string, &type_id, &typmod, false);

	/* Restore former ereport callback */
	error_context_stack = syntax_errcontext.previous;

	/* Okay, build a PLTSQL_type data structure for it */
	return pltsql_build_datatype(type_id, typmod,
								  pltsql_curr_compile->fn_input_collation);
}

/*
 * Check block starting and ending labels match.
 */
static void
check_labels(const char *start_label, const char *end_label, int end_location)
{
	if (end_label)
	{
		if (!start_label)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("end label \"%s\" specified for unlabelled block",
							end_label),
					 parser_errposition(end_location)));

		if (strcmp(start_label, end_label) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("end label \"%s\" differs from block's label \"%s\"",
							end_label, start_label),
					 parser_errposition(end_location)));
	}
}

/*
 * Read the arguments (if any) for a cursor, followed by the until token
 *
 * If cursor has no args, just swallow the until token and return NULL.
 * If it does have args, we expect to see "( arg [, arg ...] )" followed
 * by the until token, where arg may be a plain expression, or a named
 * parameter assignment of the form argname := expr. Consume all that and
 * return a SELECT query that evaluates the expression(s) (without the outer
 * parens).
 */
static PLTSQL_expr *
read_cursor_args(PLTSQL_var *cursor, int until, const char *expected)
{
	PLTSQL_expr *expr;
	PLTSQL_row *row;
	int			tok;
	int			argc;
	char	  **argv;
	StringInfoData ds;
	char	   *sqlstart = "SELECT ";
	bool		any_named = false;

	tok = yylex();
	if (cursor->cursor_explicit_argrow < 0)
	{
		/* No arguments expected */
		if (tok == '(')
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cursor \"%s\" has no arguments",
							cursor->refname),
					 parser_errposition(yylloc)));

		if (tok != until)
			yyerror("syntax error");

		return NULL;
	}

	/* Else better provide arguments */
	if (tok != '(')
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cursor \"%s\" has arguments",
						cursor->refname),
				 parser_errposition(yylloc)));

	/*
	 * Read the arguments, one by one.
	 */
	row = (PLTSQL_row *) pltsql_Datums[cursor->cursor_explicit_argrow];
	argv = (char **) palloc0(row->nfields * sizeof(char *));

	for (argc = 0; argc < row->nfields; argc++)
	{
		PLTSQL_expr *item;
		int		endtoken;
		int		argpos;
		int		tok1,
				tok2;
		int		arglocation;

		/* Check if it's a named parameter: "param := value" */
		pltsql_peek2(&tok1, &tok2, &arglocation, NULL);
		if (tok1 == IDENT && tok2 == COLON_EQUALS)
		{
			char   *argname;
			IdentifierLookup save_IdentifierLookup;

			/* Read the argument name, ignoring any matching variable */
			save_IdentifierLookup = pltsql_IdentifierLookup;
			pltsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
			yylex();
			argname = yylval.str;
			pltsql_IdentifierLookup = save_IdentifierLookup;

			/* Match argument name to cursor arguments */
			for (argpos = 0; argpos < row->nfields; argpos++)
			{
				if (strcmp(row->fieldnames[argpos], argname) == 0)
					break;
			}
			if (argpos == row->nfields)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cursor \"%s\" has no argument named \"%s\"",
								cursor->refname, argname),
						 parser_errposition(yylloc)));

			/*
			 * Eat the ":=". We already peeked, so the error should never
			 * happen.
			 */
			tok2 = yylex();
			if (tok2 != COLON_EQUALS)
				yyerror("syntax error");

			any_named = true;
		}
		else
			argpos = argc;

		if (argv[argpos] != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("duplicate value for cursor \"%s\" parameter \"%s\"",
							cursor->refname, row->fieldnames[argpos]),
					 parser_errposition(arglocation)));

		/*
		 * Read the value expression. To provide the user with meaningful
		 * parse error positions, we check the syntax immediately, instead of
		 * checking the final expression that may have the arguments
		 * reordered. Trailing whitespace must not be trimmed, because
		 * otherwise input of the form (param -- comment\n, param) would be
		 * translated into a form where the second parameter is commented
		 * out.
		 */
		item = read_sql_construct(',', ')', 0,
								  ",\" or \")",
								  sqlstart,
								  true, true,
								  false, /* do not trim */
								  NULL, &endtoken);

		argv[argpos] = item->query + strlen(sqlstart);

		if (endtoken == ')' && !(argc == row->nfields - 1))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("not enough arguments for cursor \"%s\"",
							cursor->refname),
					 parser_errposition(yylloc)));

		if (endtoken == ',' && (argc == row->nfields - 1))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("too many arguments for cursor \"%s\"",
							cursor->refname),
					 parser_errposition(yylloc)));
	}

	/* Make positional argument list */
	initStringInfo(&ds);
	appendStringInfoString(&ds, sqlstart);
	for (argc = 0; argc < row->nfields; argc++)
	{
		Assert(argv[argc] != NULL);

		/*
		 * Because named notation allows permutated argument lists, include
		 * the parameter name for meaningful runtime errors.
		 */
		appendStringInfoString(&ds, argv[argc]);
		if (any_named)
			appendStringInfo(&ds, " AS %s",
							 quote_identifier(row->fieldnames[argc]));
		if (argc < row->nfields - 1)
			appendStringInfoString(&ds, ", ");
	}
	appendStringInfoChar(&ds, ';');

	expr = palloc0(sizeof(PLTSQL_expr));
	expr->dtype			= PLTSQL_DTYPE_EXPR;
	expr->query			= pstrdup(ds.data);
	expr->plan			= NULL;
	expr->paramnos		= NULL;
	expr->rwparam		= -1;
	expr->ns            = pltsql_ns_top();
	pfree(ds.data);

	/* Next we'd better find the until token */
	tok = yylex();
	if (tok != until)
		yyerror("syntax error");

	return expr;
}

/*
 * Parse RAISE ... USING options
 */
static List *
read_raise_options(void)
{
	List	   *result = NIL;

	for (;;)
	{
		PLTSQL_raise_option *opt;
		int		tok;

		if ((tok = yylex()) == 0)
			yyerror("unexpected end of function definition");

		opt = (PLTSQL_raise_option *) palloc(sizeof(PLTSQL_raise_option));

		if (tok_is_keyword(tok, &yylval,
						   K_ERRCODE, "errcode"))
			opt->opt_type = PLTSQL_RAISEOPTION_ERRCODE;
		else if (tok_is_keyword(tok, &yylval,
								K_MESSAGE, "message"))
			opt->opt_type = PLTSQL_RAISEOPTION_MESSAGE;
		else if (tok_is_keyword(tok, &yylval,
								K_DETAIL, "detail"))
			opt->opt_type = PLTSQL_RAISEOPTION_DETAIL;
		else if (tok_is_keyword(tok, &yylval,
								K_HINT, "hint"))
			opt->opt_type = PLTSQL_RAISEOPTION_HINT;
		else
			yyerror("unrecognized RAISE statement option");

		tok = yylex();
		if (tok != '=' && tok != COLON_EQUALS)
			yyerror("syntax error, expected \"=\"");

		opt->expr = read_sql_expression2(',', ';', ", or ;", &tok);

		result = lappend(result, opt);

		if (tok == ';')
			break;
	}

	return result;
}
