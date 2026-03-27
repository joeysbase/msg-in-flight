-- ---------------------------------------------------------------
-- PostgreSQL runtime metrics snapshot
-- Run with: psql -U chatflow -d chatflow -f pg_stats.sql
-- ---------------------------------------------------------------

-- Active connections
SELECT count(*), state
FROM   pg_stat_activity
WHERE  datname = 'chatflow'
GROUP  BY state;

-- Buffer pool hit ratio (target > 99%)
SELECT
    round(100.0 * blks_hit / nullif(blks_hit + blks_read, 0), 2) AS buffer_hit_ratio_pct,
    blks_read,
    blks_hit,
    tup_inserted,
    tup_updated,
    tup_deleted
FROM pg_stat_database
WHERE datname = 'chatflow';

-- Table-level I/O
SELECT
    relname,
    seq_scan, idx_scan,
    n_live_tup,
    n_dead_tup,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;

-- Index usage
SELECT
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- Lock waits
SELECT count(*) AS blocking_locks
FROM pg_locks
WHERE NOT granted;

-- Checkpoint activity
SELECT
    checkpoints_timed,
    checkpoints_req,
    checkpoint_write_time,
    checkpoint_sync_time,
    buffers_checkpoint,
    buffers_clean,
    buffers_backend
FROM pg_stat_bgwriter;
