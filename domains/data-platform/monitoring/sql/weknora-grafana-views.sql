\set ON_ERROR_STOP on
\getenv reader_password GRAFANA_WEKNORA_DB_PASSWORD

-- Restricted account. This role can query only the aggregate views below;
-- it receives no SELECT privilege on WeKnora's content-bearing tables.
SELECT format(
    'CREATE ROLE grafana_knowledge_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %L',
    :'reader_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'grafana_knowledge_reader'
)
\gexec

SELECT format(
    'ALTER ROLE grafana_knowledge_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %L',
    :'reader_password'
)
\gexec

GRANT CONNECT ON DATABASE "WeKnora" TO grafana_knowledge_reader;

CREATE SCHEMA IF NOT EXISTS grafana_readonly AUTHORIZATION postgres;
REVOKE ALL ON SCHEMA grafana_readonly FROM PUBLIC;

CREATE OR REPLACE VIEW grafana_readonly.knowledge_summary
WITH (security_barrier = true) AS
SELECT
    (SELECT count(*) FROM knowledge_bases WHERE deleted_at IS NULL) AS knowledge_bases,
    count(*) AS documents,
    count(*) FILTER (WHERE parse_status = 'completed') AS completed,
    count(*) FILTER (WHERE parse_status IN ('pending', 'processing', 'finalizing')) AS processing,
    count(*) FILTER (WHERE parse_status = 'failed') AS failed,
    count(*) FILTER (WHERE parse_status = 'cancelled') AS cancelled,
    coalesce(sum(file_size), 0)::bigint AS source_bytes,
    max(created_at) AS last_document_added
FROM knowledges
WHERE deleted_at IS NULL;

CREATE OR REPLACE VIEW grafana_readonly.knowledge_parse_status
WITH (security_barrier = true) AS
SELECT coalesce(parse_status, 'unknown') AS parse_status, count(*) AS documents
FROM knowledges
WHERE deleted_at IS NULL
GROUP BY coalesce(parse_status, 'unknown');

CREATE OR REPLACE VIEW grafana_readonly.knowledge_daily_ingestion
WITH (security_barrier = true) AS
SELECT date_trunc('day', created_at) AS created_day,
       coalesce(parse_status, 'unknown') AS parse_status,
       count(*) AS documents
FROM knowledges
WHERE deleted_at IS NULL
GROUP BY 1, 2;

CREATE OR REPLACE VIEW grafana_readonly.knowledge_file_types
WITH (security_barrier = true) AS
SELECT coalesce(nullif(file_type, ''), 'unknown') AS file_type,
       count(*) AS documents
FROM knowledges
WHERE deleted_at IS NULL
GROUP BY coalesce(nullif(file_type, ''), 'unknown');

CREATE OR REPLACE VIEW grafana_readonly.processing_stages
WITH (security_barrier = true) AS
SELECT name AS stage,
       status,
       count(*) AS runs,
       round(avg(duration_ms)::numeric, 1) AS avg_duration_ms,
       max(created_at) AS last_seen
FROM knowledge_processing_spans
WHERE created_at >= now() - interval '90 days'
  AND kind = 'stage'
GROUP BY name, status;

-- Vector-store metrics only: counts, dimensions and allocated storage sizes.
-- No embedding values, chunk text, document titles or tenant names are exposed.
CREATE OR REPLACE VIEW grafana_readonly.vector_summary
WITH (security_barrier = true) AS
SELECT coalesce(stats.n_live_tup, 0)::bigint AS vector_rows_estimate,
       pg_relation_size('public.embeddings'::regclass) AS table_bytes,
       pg_indexes_size('public.embeddings'::regclass) AS all_index_bytes,
       coalesce((
           SELECT count(*)
           FROM pg_index i
           JOIN pg_class t ON t.oid = i.indrelid
           JOIN pg_class idx ON idx.oid = i.indexrelid
           JOIN pg_am am ON am.oid = idx.relam
           WHERE t.oid = 'public.embeddings'::regclass AND am.amname = 'hnsw'
       ), 0) AS hnsw_indexes,
       coalesce((
           SELECT sum(pg_relation_size(i.indexrelid))
           FROM pg_index i
           JOIN pg_class t ON t.oid = i.indrelid
           JOIN pg_class idx ON idx.oid = i.indexrelid
           JOIN pg_am am ON am.oid = idx.relam
           WHERE t.oid = 'public.embeddings'::regclass AND am.amname = 'hnsw'
       ), 0) AS hnsw_index_bytes
FROM pg_stat_user_tables stats
WHERE stats.schemaname = 'public' AND stats.relname = 'embeddings';

CREATE OR REPLACE VIEW grafana_readonly.vector_extensions
WITH (security_barrier = true) AS
SELECT extname AS extension, extversion AS version
FROM pg_extension
WHERE extname IN ('vector', 'pg_search');

GRANT USAGE ON SCHEMA grafana_readonly TO grafana_knowledge_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA grafana_readonly TO grafana_knowledge_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA grafana_readonly
    GRANT SELECT ON TABLES TO grafana_knowledge_reader;
