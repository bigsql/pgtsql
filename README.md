# pgTSQL

## Overview

pgTSQL is an extension that provides a PostgreSQL procedural language that
implements the Transact-SQL language. Additionally, the extension provides a 
number of built-in functions that are compatible with Sybase and SQL Server.

T-SQL (Transact-SQL) is the procedural programming language built into Sybase & SQL Server. It's functionally the equivalent of PostgreSQL's PL/pgSQL. It looks a bit odd because it has unique conventions like:

    Semicolons are not required at the end of each line of code
     
    Variable names always are prefixed with @ signs
     
    IF statements do not need to be closed by END IF's
     
    Temporary tables are automagic if the table name has a # prefix


## Installation
Use theapg commandline to install PostgreSQL 10 and then the pgtsql extension

./pgc install pg10
./pgc start pg10 -d demo
./pgc install pgtsql -d demo


## Example
Now lets use psql to create a table and a small sample TSQL function as follows.

``
CREATE TABLE rdbms_supports_tsql (
  organization  varchar(10) primary key
);
INSERT INTO rdbms_supports_tsql VALUES ('SYBASE');
INSERT INTO rdbms_supports_tsql VALUES ('SQL-SERVER');
INSERT INTO rdbms_supports_tsql VALUES ('POSTGRES');


CREATE OR REPLACE FUNCTION query_tsql_rdbms() RETURNS void AS $$
  DECLARE @val int = 0
BEGIN
  SELECT count(*) INTO @val FROM rdbms_supports_tsql;
  IF @val = 2
    PRINT 'Proprietary market featuring vendor lock-in'
  ELSE
    PRINT 'Open Source innovation means choices'
END
$$ LANGUAGE pltsql;
``


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

