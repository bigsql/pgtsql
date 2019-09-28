
CREATE SCHEMA sys;

GRANT USAGE ON SCHEMA sys TO PUBLIC;

CREATE VIEW sys.tables AS
SELECT c.relname::varchar as name, c.oid::int as object_id, 
       c.relowner::int as principal_id,c.relnamespace::int as schema_id, 
       0::int as parent_object_id,
       'U'::varchar as type, 'USER_TABLE'::varchar as type_desc,
       TIMESTAMP 'epoch' as create_date, TIMESTAMP 'epoch' as modify_date, 
       0::bit as is_ms_shipped, 0::bit as is_published, 0::bit as is_schema_published,
       0::int as lob_data_space_id, null::int as filestream_data_space_id,
       (SELECT max(attnum) FROM pg_attribute WHERE attrelid = c.oid)::int as max_column_id_used,
       0::bit as lock_on_bulk_load, 1::bit as uses_ansi_nulls, 0::bit as is_replicated,
       0::bit as has_replication_filter, 0::bit as is_merge_published,
       0::bit as is_sync_tran_subscribed, 0::bit as has_unchecked_assembly_data,
       0::int as text_in_row_limit, 0::bit as large_value_types_out_of_row,
       0::bit as is_tracked_by_cdc, 0::smallint as lock_escalation,
       'TABLE'::varchar as lock_escalation_desc
  FROM pg_class c, pg_namespace n
 WHERE c.relnamespace = n.oid 
   AND c.relkind = 'r'
   AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema', 'sys');
  
GRANT SELECT ON sys.tables TO PUBLIC;

CREATE VIEW sys.views AS
SELECT c.relname::varchar as name, c.oid::int as object_id, 
       c.relowner::int as principal_id,c.relnamespace::int as schema_id, 
       0::int as parent_object_id,
       'V'::varchar as type, 'VIEW'::varchar as type_desc,
       TIMESTAMP 'epoch' as create_date, TIMESTAMP 'epoch' as modify_date, 
       0::bit as is_ms_shipped, 0::bit as is_published, 0::bit as is_schema_published,
       0::bit as is_replicated, 0::bit as has_replication_filter,
       0::bit as has_opaque_metadata, 0::bit as has_unchecked_assembly_data,
       0::bit as with_check_option, 0::bit as is_date_correlation_view
  FROM pg_class c, pg_namespace n
 WHERE c.relnamespace = n.oid 
   AND c.relkind = 'v'
   AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema', 'sys');

GRANT SELECT ON sys.views TO PUBLIC;


CREATE VIEW sys.objects AS
SELECT name, object_id, principal_id, schema_id, parent_object_id,
       type, type_desc, create_date, modify_date, is_ms_shipped,
       is_published, is_schema_published
  FROM sys.tables
 UNION
SELECT name, object_id, principal_id, schema_id, parent_object_id,
       type, type_desc, create_date, modify_date, is_ms_shipped,
       is_published, is_schema_published
  FROM sys.views;

GRANT SELECT ON sys.objects TO PUBLIC;

/* Built in functions */
CREATE FUNCTION sys.sysdatetime() RETURNS timestamp
    AS $$select now()::timestamp;$$
    LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.sysdatetime() TO PUBLIC; 


CREATE FUNCTION sys.sysdatetimeoffset() RETURNS timestamp with time zone
    AS $$select now();$$
    LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.sysdatetimeoffset() TO PUBLIC; 


CREATE FUNCTION sys.sysutcdatetime() RETURNS timestamp
    AS $$select now() AT TIME ZONE 'UTC';$$
    LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.sysutcdatetime() TO PUBLIC; 


CREATE FUNCTION sys.getdate() RETURNS timestamp
    AS $$select date_trunc('millisecond', now()::timestamp);$$
    LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.getdate() TO PUBLIC; 


CREATE FUNCTION sys.getutcdate() RETURNS timestamp
    AS $$select date_trunc('millisecond', now() AT TIME ZONE 'UTC');$$
    LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.getutcdate() TO PUBLIC; 


CREATE FUNCTION sys.isnull(text,text) RETURNS text AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(text,text) TO PUBLIC;

CREATE FUNCTION sys.isnull(boolean,boolean) RETURNS boolean AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(boolean,boolean) TO PUBLIC;

CREATE FUNCTION sys.isnull(smallint,smallint) RETURNS smallint AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(smallint,smallint) TO PUBLIC;

CREATE FUNCTION sys.isnull(integer,integer) RETURNS integer AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(integer,integer) TO PUBLIC;

CREATE FUNCTION sys.isnull(bigint,bigint) RETURNS bigint AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(bigint,bigint) TO PUBLIC;

CREATE FUNCTION sys.isnull(real,real) RETURNS real AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(real,real) TO PUBLIC;

CREATE FUNCTION sys.isnull(double precision, double precision) RETURNS double precision AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(double precision, double precision) TO PUBLIC;

CREATE FUNCTION sys.isnull(numeric,numeric) RETURNS numeric AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(numeric,numeric) TO PUBLIC;

CREATE FUNCTION sys.isnull(date, date) RETURNS date AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(date,date) TO PUBLIC;

CREATE FUNCTION sys.isnull(timestamp,timestamp) RETURNS timestamp AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(timestamp,timestamp) TO PUBLIC;

CREATE FUNCTION sys.isnull(timestamp with time zone,timestamp with time zone) RETURNS timestamp with time zone AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION sys.isnull(timestamp with time zone,timestamp with time zone) TO PUBLIC;




/* PL/TSQL procedural language */

CREATE FUNCTION pltsql_call_handler ()
RETURNS language_handler AS 'pgtsql' LANGUAGE C;

CREATE FUNCTION pltsql_validator (oid)
RETURNS void AS 'pgtsql' LANGUAGE C;

-- language
CREATE LANGUAGE pltsql 
       HANDLER pltsql_call_handler 
       VALIDATOR pltsql_validator;

COMMENT ON LANGUAGE pltsql IS 'PL/TSQL procedural language';
