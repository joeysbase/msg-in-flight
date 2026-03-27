package chatflow.db.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.sql.DataSource;

public class DbMetricsRepository {

  private static final String DB_NAME = "chatflow";

  private static final String SQL_DB_SNAPSHOT =
      """
      SELECT xact_commit,
             xact_rollback,
             blks_read,
             blks_hit,
             tup_inserted,
             tup_updated,
             tup_deleted,
             tup_fetched
      FROM   pg_stat_database
      WHERE  datname = ?
      """;

  private static final String SQL_CONNECTIONS =
      """
      SELECT count(*)                                              AS total,
             count(*) FILTER (WHERE state = 'active')             AS active,
             count(*) FILTER (WHERE state = 'idle')               AS idle,
             count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_txn,
             count(*) FILTER (WHERE wait_event_type = 'Lock')     AS waiting_on_lock
      FROM   pg_stat_activity
      WHERE  datname = ?
      """;

  private static final String SQL_LOCK_WAITS =
      """
      SELECT count(*)                                    AS waiting_lock_count,
             coalesce(max(extract(epoch FROM
                 now() - a.query_start)::int), 0)        AS max_lock_wait_sec,
             coalesce(avg(extract(epoch FROM
                 now() - a.query_start)::int), 0)        AS avg_lock_wait_sec
      FROM   pg_locks  l
      JOIN   pg_stat_activity a ON a.pid = l.pid
      WHERE  NOT l.granted
        AND  a.datname = ?
      """;

  private static final String SQL_BGWRITER =
      """
      SELECT buffers_checkpoint,
             buffers_clean,
             buffers_backend,
             checkpoint_write_time,
             checkpoint_sync_time,
             checkpoints_timed,
             checkpoints_req
      FROM   pg_stat_bgwriter
      """;

  private static final String SQL_TABLE_IO =
      """
      SELECT coalesce(sum(heap_blks_read),  0) AS heap_blks_read,
             coalesce(sum(heap_blks_hit),   0) AS heap_blks_hit,
             coalesce(sum(idx_blks_read),   0) AS idx_blks_read,
             coalesce(sum(idx_blks_hit),    0) AS idx_blks_hit,
             coalesce(sum(toast_blks_read), 0) AS toast_blks_read,
             coalesce(sum(toast_blks_hit),  0) AS toast_blks_hit
      FROM   pg_statio_user_tables
      """;

  private final DataSource ds;

  private long prevXactTotal = -1;
  private long prevTupTotal = -1;
  private long prevBlksRead = -1;
  private long prevBlksHit = -1;
  private long prevSnapshotMs = 0;

  public DbMetricsRepository(DataSource ds) {
    this.ds = ds;
  }

  public Map<String, Object> collect() {
    Map<String, Object> out = new LinkedHashMap<>();
    long now = System.currentTimeMillis();

    try {
      out.put("queriesPerSecond", collectQps(now));
    } catch (Exception e) {
      out.put("queriesPerSecond", error(e));
    }

    try {
      out.put("activeConnections", collectConnections());
    } catch (Exception e) {
      out.put("activeConnections", error(e));
    }

    try {
      out.put("lockWaits", collectLockWaits());
    } catch (Exception e) {
      out.put("lockWaits", error(e));
    }

    try {
      out.put("bufferPoolHitRatio", collectBufferHitRatio(now));
    } catch (Exception e) {
      out.put("bufferPoolHitRatio", error(e));
    }

    try {
      out.put("diskIO", collectDiskIO());
    } catch (Exception e) {
      out.put("diskIO", error(e));
    }

    prevSnapshotMs = now;
    return out;
  }

  private Map<String, Object> collectQps(long nowMs) throws SQLException {
    Map<String, Object> out = new LinkedHashMap<>();

    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(SQL_DB_SNAPSHOT)) {
      ps.setString(1, DB_NAME);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return out;

        long xactCommit = rs.getLong("xact_commit");
        long xactRollback = rs.getLong("xact_rollback");
        long tupInserted = rs.getLong("tup_inserted");
        long tupUpdated = rs.getLong("tup_updated");
        long tupDeleted = rs.getLong("tup_deleted");
        long tupFetched = rs.getLong("tup_fetched");

        long xactTotal = xactCommit + xactRollback;
        long tupWrite = tupInserted + tupUpdated + tupDeleted;

        double elapsedSec = prevSnapshotMs > 0 ? (nowMs - prevSnapshotMs) / 1000.0 : 0;

        if (elapsedSec > 0 && prevXactTotal >= 0) {
          out.put("transactionsPerSec", round2((xactTotal - prevXactTotal) / elapsedSec));
          out.put("tupleWritesPerSec", round2((tupWrite - prevTupTotal) / elapsedSec));
        } else {
          out.put("transactionsPerSec", 0);
          out.put("tupleWritesPerSec", 0);
          out.put("note", "first sample — rate available on next poll");
        }

        out.put("xactCommitTotal", xactCommit);
        out.put("xactRollbackTotal", xactRollback);
        out.put("tupInsertedTotal", tupInserted);
        out.put("tupFetchedTotal", tupFetched);

        prevXactTotal = xactTotal;
        prevTupTotal = tupWrite;
      }
    }
    return out;
  }

  private Map<String, Object> collectConnections() throws SQLException {
    Map<String, Object> out = new LinkedHashMap<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(SQL_CONNECTIONS)) {
      ps.setString(1, DB_NAME);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          out.put("total", rs.getInt("total"));
          out.put("active", rs.getInt("active"));
          out.put("idle", rs.getInt("idle"));
          out.put("idleInTxn", rs.getInt("idle_in_txn"));
          out.put("waitingOnLock", rs.getInt("waiting_on_lock"));
        }
      }
    }
    return out;
  }

  private Map<String, Object> collectLockWaits() throws SQLException {
    Map<String, Object> out = new LinkedHashMap<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(SQL_LOCK_WAITS)) {
      ps.setString(1, DB_NAME);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          out.put("waitingLocks", rs.getInt("waiting_lock_count"));
          out.put("maxWaitSec", rs.getInt("max_lock_wait_sec"));
          out.put("avgWaitSec", rs.getDouble("avg_lock_wait_sec"));
        }
      }
    }
    return out;
  }

  private Map<String, Object> collectBufferHitRatio(long nowMs) throws SQLException {
    Map<String, Object> out = new LinkedHashMap<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(SQL_DB_SNAPSHOT)) {
      ps.setString(1, DB_NAME);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return out;

        long blksRead = rs.getLong("blks_read");
        long blksHit = rs.getLong("blks_hit");
        long total = blksRead + blksHit;

        double cumRatio = total > 0 ? round2(100.0 * blksHit / total) : 100.0;
        out.put("cumulativeHitRatioPct", cumRatio);

        if (prevBlksRead >= 0 && prevBlksHit >= 0) {
          long dRead = blksRead - prevBlksRead;
          long dHit = blksHit - prevBlksHit;
          long dTotal = dRead + dHit;
          out.put("intervalHitRatioPct", dTotal > 0 ? round2(100.0 * dHit / dTotal) : 100.0);
        } else {
          out.put("intervalHitRatioPct", 0);
          out.put("note", "first sample — interval ratio available on next poll");
        }

        out.put("blksReadTotal", blksRead);
        out.put("blksHitTotal", blksHit);

        prevBlksRead = blksRead;
        prevBlksHit = blksHit;
      }
    }
    return out;
  }

  private Map<String, Object> collectDiskIO() throws SQLException {
    Map<String, Object> out = new LinkedHashMap<>();

    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(SQL_BGWRITER);
        ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        Map<String, Object> bgw = new LinkedHashMap<>();
        bgw.put("buffersCheckpoint", rs.getLong("buffers_checkpoint"));
        bgw.put("buffersClean", rs.getLong("buffers_clean"));
        bgw.put("buffersBackend", rs.getLong("buffers_backend"));
        bgw.put("checkpointWriteMs", rs.getLong("checkpoint_write_time"));
        bgw.put("checkpointSyncMs", rs.getLong("checkpoint_sync_time"));
        bgw.put("checkpointsTimed", rs.getLong("checkpoints_timed"));
        bgw.put("checkpointsRequired", rs.getLong("checkpoints_req"));
        out.put("bgwriter", bgw);
      }
    }

    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(SQL_TABLE_IO);
        ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        Map<String, Object> io = new LinkedHashMap<>();
        long heapRead = rs.getLong("heap_blks_read");
        long heapHit = rs.getLong("heap_blks_hit");
        long idxRead = rs.getLong("idx_blks_read");
        long idxHit = rs.getLong("idx_blks_hit");

        io.put("heapBlksRead", heapRead);
        io.put("heapBlksHit", heapHit);
        io.put("idxBlksRead", idxRead);
        io.put("idxBlksHit", idxHit);
        io.put("toastBlksRead", rs.getLong("toast_blks_read"));
        io.put("toastBlksHit", rs.getLong("toast_blks_hit"));

        long totalRead = heapRead + idxRead;
        long totalHit = heapHit + idxHit;
        long tot = totalRead + totalHit;
        io.put("combinedHitRatioPct", tot > 0 ? round2(100.0 * totalHit / tot) : 100.0);
        out.put("tableIO", io);
      }
    }

    return out;
  }

  private static double round2(double v) {
    return Math.round(v * 100.0) / 100.0;
  }

  private static Map<String, Object> error(Exception e) {
    return Map.of("error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
  }
}
