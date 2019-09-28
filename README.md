# pgTSQL

## Overview

pgTSQL is an extension that provides a PostgreSQL procedural language that
implements the Transact-SQL language. Additionally, the extension provides a 
number of built-in functions that are compatible with Sybase and SQL Server.



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

