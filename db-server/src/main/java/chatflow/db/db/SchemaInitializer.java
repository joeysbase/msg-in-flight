package chatflow.db.db;

import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

public class SchemaInitializer {

  public static void init(DataSource ds) throws Exception {
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      // Main messages table
      stmt.execute(
          """
          CREATE TABLE IF NOT EXISTS messages (
              message_id   VARCHAR(36)  PRIMARY KEY,
              user_id      VARCHAR(20)  NOT NULL,
              room_id      VARCHAR(10)  NOT NULL,
              username     VARCHAR(20)  NOT NULL,
              message      TEXT         NOT NULL,
              created_at   TIMESTAMPTZ  NOT NULL,
              message_type VARCHAR(10)  NOT NULL,
              server_id    VARCHAR(50),
              client_ip    VARCHAR(50)
          )
          """);

      // Index: room timeline queries  (Query 1, most active rooms)
      stmt.execute(
          """
          CREATE INDEX IF NOT EXISTS idx_messages_room_time
              ON messages (room_id, created_at)
          """);

      // Index: user history queries  (Query 2, user participation)
      stmt.execute(
          """
          CREATE INDEX IF NOT EXISTS idx_messages_user_time
              ON messages (user_id, created_at)
          """);

      // Index: time-window active-user count  (Query 3)
      stmt.execute(
          """
          CREATE INDEX IF NOT EXISTS idx_messages_time
              ON messages (created_at)
          """);

      // Materialized view for per-room message counts (analytics)
      // Refreshed by the statistics aggregator thread.
      stmt.execute(
          """
          CREATE MATERIALIZED VIEW IF NOT EXISTS room_stats AS
          SELECT room_id,
                 COUNT(*)          AS message_count,
                 MAX(created_at)   AS last_activity,
                 COUNT(DISTINCT user_id) AS unique_users
          FROM messages
          GROUP BY room_id
          """);

      stmt.execute(
          """
          CREATE UNIQUE INDEX IF NOT EXISTS idx_room_stats_room_id
              ON room_stats (room_id)
          """);

      System.out.println("Schema initialized successfully.");
    }
  }

  public static void refreshRoomStats(DataSource ds) {
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY room_stats");
    } catch (Exception e) {
      System.err.println("Warning: could not refresh room_stats: " + e.getMessage());
    }
  }
}
