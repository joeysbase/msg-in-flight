-- =============================================================
-- Chat-Flow Database Schema
-- PostgreSQL 15
-- =============================================================

-- Drop and recreate (idempotent setup)
DROP TABLE IF EXISTS messages CASCADE;
DROP MATERIALIZED VIEW IF EXISTS room_stats;

-- ---------------------------------------------------------------
-- Core messages table
-- ---------------------------------------------------------------
CREATE TABLE messages (
    message_id   VARCHAR(36)  PRIMARY KEY,        -- UUID from producer
    user_id      VARCHAR(20)  NOT NULL,
    room_id      VARCHAR(10)  NOT NULL,
    username     VARCHAR(20)  NOT NULL,
    message      TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,            -- timezone-aware
    message_type VARCHAR(10)  NOT NULL,            -- TEXT | JOIN | LEAVE
    server_id    VARCHAR(50),
    client_ip    VARCHAR(50)
);

-- ---------------------------------------------------------------
-- Indexes (see REPORT.md §2 for selectivity analysis)
-- ---------------------------------------------------------------

-- Query 1: messages in room for time range
--   SELECT ... WHERE room_id = ? AND created_at BETWEEN ? AND ?
--   Composite chosen: room_id has very high selectivity (1/20 rows),
--   created_at narrows further. Covers ORDER BY created_at with no sort.
CREATE INDEX idx_messages_room_time
    ON messages (room_id, created_at);

-- Query 2: user message history
--   SELECT ... WHERE user_id = ? AND created_at BETWEEN ? AND ?
--   user_id cardinality up to 100 000; composite avoids heap fetch for date filter.
CREATE INDEX idx_messages_user_time
    ON messages (user_id, created_at);

-- Query 3: active-user count in time window
--   SELECT COUNT(DISTINCT user_id) WHERE created_at BETWEEN ? AND ?
--   Single-column index lets the planner use index-only scan for the time filter.
CREATE INDEX idx_messages_time
    ON messages (created_at);

-- ---------------------------------------------------------------
-- Materialized view for fast room analytics
-- Refreshed CONCURRENTLY every 30 s by the stats-aggregator thread.
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW room_stats AS
SELECT
    room_id,
    COUNT(*)               AS message_count,
    MAX(created_at)        AS last_activity,
    COUNT(DISTINCT user_id) AS unique_users
FROM messages
GROUP BY room_id;

-- Unique index required for REFRESH CONCURRENTLY (no table lock)
CREATE UNIQUE INDEX idx_room_stats_room_id ON room_stats (room_id);
