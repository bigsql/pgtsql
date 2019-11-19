#---------------------------------------------------------------------------------
#
# Makefile for the pgtsql extension 
#
# Portions Copyright (c) 1994, Regents of the University of California
#
#---------------------------------------------------------------------------------

EXTENSION = pgtsql

DISTVERSION = 3.0

PG_CONFIG = pg_config
PQINCSERVER = $(shell $(PG_CONFIG) --includedir-server)
PQINC = $(shell $(PG_CONFIG) --includedir)
PQLIB = $(shell $(PG_CONFIG) --libdir)

MODULE_big = $(EXTENSION)
OBJS = src/pl_gram.o src/pl_handler.o src/pl_comp.o src/pl_exec.o \
	src/pl_funcs.o src/pl_scanner.o

DATA = pgtsql.control sql/pgtsql--3.0.sql 

TESTS = $(wildcard test/sql/*.sql)
REGRESS_OPTS = --dbname=regression --inputdir=test --load-extension=$(EXTENSION)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))

PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Force these dependencies to be known even without dependency info built:
src/pl_gram.o src/pl_handler.o src/pl_comp.o src/pl_exec.o src/pl_funcs.o src/pl_scanner.o: src/pltsql.h src/pl_gram.h src/plerrcodes.h

# See notes in src/backend/parser/Makefile about the following two rules

src/pl_gram.h: src/pl_gram.c 

src/pl_gram.c: src/gram.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
else
	@$(missing) bison $< $@
endif


