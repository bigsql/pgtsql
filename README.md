# pgTSQL

## Overview
Transact-SQL is now available in PostgreSQL (in addition to Sybase & SQL Server).


pgTSQL is an extension that provides:

* PostgreSQL procedural language that implements the Transact-SQL language. 
* Built-in functions, such as getdate() and isnull()
* System catalog views, such as SYS.TABLES and SYS.OBJECTS

T-SQL (Transact-SQL) is the procedural programming language built into Sybase & SQL Server. 
It's functionally the equivalent of PostgreSQL's PL/pgSQL. It looks a bit different
because it has unique conventions like:

* Semicolons are not required at the end of each line of code
* Variable names always are prefixed with @ signs 
* IF statements do not need to be closed by END IF's     
* Temporary tables are automagic if the table name has a # prefix

## History of project
This project was originally part of an OpenSCG  PostgreSQL distribution call tPostgres that 
fizzled about seven years ago.  Five years ago Jim Mlodgenski ported it as a standalone extension
for PG95 BigSQL.   Earlier in 2019 Denis Lussier upgraded it to suport PG 9.6 & PG 10.
Now Korry Douglas is working to get it running properly with PG 11 (including support for stored 
procedures instead of just stored functions) and then PG 12.


## Installation
Use the BigSQL command line to install PostgreSQL 10 and then the pgtsql extension

	./apg install pg10
  	./apg start pg10
	./apg install pgtsql


## Example
Now lets use psql to create a table and a small sample TSQL function as follows.

	CREATE TABLE rdbms_supports_tsql (
	  organization  varchar(10) primary key
	);
 	INSERT INTO rdbms_supports_tsql VALUES ('SYBASE');
	INSERT INTO rdbms_supports_tsql VALUES ('SQL-SERVER');
	INSERT INTO rdbms_supports_tsql VALUES ('POSTGRES') ;
  
  
	CREATE OR REPLACE FUNCTION query_tsql_rdbms() RETURNS void AS $$
	  DECLARE @val int = 0. 
	BEGIN
	  SELECT count(*) INTO @val FROM rdbms_supports_tsql;
	  IF @val = 2
	    PRINT 'Proprietary market featuring vendor lock-in'
	  ELSE
	    PRINT 'Open Source innovation means choices'
	END
	$$ LANGUAGE pltsql;


## Building from Source

The "master" branch is for building against PG 10.

For installation there must be PostgreSQL dev environment installed
and pg_config in the PATH.   Then just run:

	$ make
	$ make install

To run regression tests:

	$ make installcheck

Notes:

* Location to pg_config can be set via PG_CONFIG variable:

	$ make PG_CONFIG=/path/to/pg_config
	$ make install PG_CONFIG=/path/to/pg_config
	$ make installcheck PG_CONFIG=/path/to/pg_config

