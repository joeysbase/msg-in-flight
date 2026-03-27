package chatflow.db.db;

import chatflow.utils.QueueMessage;
import java.sql.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import javax.sql.DataSource;

public class MessageRepository {

  private static final String INSERT_BATCH_SQL =
      """
      INSERT INTO messages
          (message_id, user_id, room_id, username, message, created_at, message_type, server_id, client_ip)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (message_id) DO NOTHING
      """;

  private static final String ROOM_MESSAGES_SQL =
      """
      SELECT message_id, user_id, room_id, username, message, created_at, message_type
      FROM messages
      WHERE room_id = ? AND created_at BETWEEN ? AND ?
      ORDER BY created_at
      LIMIT ?
      """;

  private static final String USER_MESSAGES_SQL =
      """
      SELECT message_id, user_id, room_id, username, message, created_at, message_type
      FROM messages
      WHERE user_id = ? AND created_at BETWEEN ? AND ?
      ORDER BY created_at
      LIMIT ?
      """;

  private static final String ACTIVE_USERS_SQL =
      """
      SELECT COUNT(DISTINCT user_id) AS cnt
      FROM messages
      WHERE created_at BETWEEN ? AND ?
      """;

  private static final String USER_ROOMS_SQL =
      """
      SELECT room_id, MAX(created_at) AS last_activity
      FROM messages
      WHERE user_id = ?
      GROUP BY room_id
      ORDER BY last_activity DESC
      """;

  private static final String MESSAGES_PER_SECOND_SQL =
      """
      SELECT DATE_TRUNC('second', created_at) AS ts, COUNT(*) AS cnt
      FROM messages
      WHERE created_at > NOW() - INTERVAL '10 minutes'
      GROUP BY ts
      ORDER BY ts
      """;

  private static final String MESSAGES_PER_MINUTE_SQL =
      """
      SELECT DATE_TRUNC('minute', created_at) AS ts, COUNT(*) AS cnt
      FROM messages
      WHERE created_at > NOW() - INTERVAL '1 hour'
      GROUP BY ts
      ORDER BY ts
      """;

  private static final String TOP_USERS_SQL =
      """
      SELECT user_id, username, COUNT(*) AS message_count
      FROM messages
      GROUP BY user_id, username
      ORDER BY message_count DESC
      LIMIT ?
      """;

  private static final String TOP_ROOMS_SQL =
      """
      SELECT room_id, COUNT(*) AS message_count, MAX(created_at) AS last_activity
      FROM messages
      GROUP BY room_id
      ORDER BY message_count DESC
      """;

  private static final String USER_PARTICIPATION_SQL =
      """
      SELECT user_id, room_id, COUNT(*) AS message_count,
             MIN(created_at) AS first_seen, MAX(created_at) AS last_seen
      FROM messages
      GROUP BY user_id, room_id
      ORDER BY message_count DESC
      LIMIT ?
      """;

  private static final String TOTAL_COUNT_SQL = "SELECT COUNT(*) FROM messages";

  private static final String FIRST_MESSAGE_TIME_SQL = "SELECT MIN(created_at) FROM messages";

  private final DataSource ds;

  public MessageRepository(DataSource ds) {
    this.ds = ds;
  }

  public int insertBatch(List<QueueMessage> messages) throws SQLException {
    if (messages.isEmpty()) return 0;
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(INSERT_BATCH_SQL)) {
      conn.setAutoCommit(false);
      for (QueueMessage msg : messages) {
        ps.setString(1, msg.messageId);
        ps.setString(2, msg.userId);
        ps.setString(3, msg.roomId);
        ps.setString(4, msg.username);
        ps.setString(5, msg.message);
        ps.setTimestamp(6, parseTimestamp(msg.timestamp));
        ps.setString(7, msg.messageType);
        ps.setString(8, msg.serverId);
        ps.setString(9, msg.clientIp);
        ps.addBatch();
      }
      int[] results = ps.executeBatch();
      conn.commit();
      int inserted = 0;
      for (int r : results) inserted += Math.max(r, 0);
      return inserted;
    }
  }

  /** Query 1: messages for a room in time range, ordered by time. */
  public List<Map<String, Object>> getMessagesForRoom(
      String roomId, Timestamp start, Timestamp end, int limit) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(ROOM_MESSAGES_SQL)) {
      ps.setString(1, roomId);
      ps.setTimestamp(2, start);
      ps.setTimestamp(3, end);
      ps.setInt(4, limit);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) rows.add(rowToMap(rs));
      }
    }
    return rows;
  }

  /** Query 2: user message history with optional time range. */
  public List<Map<String, Object>> getUserMessages(
      String userId, Timestamp start, Timestamp end, int limit) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(USER_MESSAGES_SQL)) {
      ps.setString(1, userId);
      ps.setTimestamp(2, start);
      ps.setTimestamp(3, end);
      ps.setInt(4, limit);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) rows.add(rowToMap(rs));
      }
    }
    return rows;
  }

  /** Query 3: count unique active users in time window. */
  public long countActiveUsers(Timestamp start, Timestamp end) throws SQLException {
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(ACTIVE_USERS_SQL)) {
      ps.setTimestamp(1, start);
      ps.setTimestamp(2, end);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong("cnt") : 0;
      }
    }
  }

  /** Query 4: rooms a user has participated in with last activity. */
  public List<Map<String, Object>> getRoomsForUser(String userId) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(USER_ROOMS_SQL)) {
      ps.setString(1, userId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("roomId", rs.getString("room_id"));
          row.put("lastActivity", rs.getTimestamp("last_activity").toString());
          rows.add(row);
        }
      }
    }
    return rows;
  }

  // ---- Analytics queries ----

  public List<Map<String, Object>> getMessagesPerSecond() throws SQLException {
    return queryTimeSeries(MESSAGES_PER_SECOND_SQL);
  }

  public List<Map<String, Object>> getMessagesPerMinute() throws SQLException {
    return queryTimeSeries(MESSAGES_PER_MINUTE_SQL);
  }

  public List<Map<String, Object>> getTopUsers(int topN) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(TOP_USERS_SQL)) {
      ps.setInt(1, topN);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("userId", rs.getString("user_id"));
          row.put("username", rs.getString("username"));
          row.put("messageCount", rs.getLong("message_count"));
          rows.add(row);
        }
      }
    }
    return rows;
  }

  public List<Map<String, Object>> getTopRooms() throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(TOP_ROOMS_SQL)) {
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("roomId", rs.getString("room_id"));
          row.put("messageCount", rs.getLong("message_count"));
          row.put("lastActivity", rs.getTimestamp("last_activity").toString());
          rows.add(row);
        }
      }
    }
    return rows;
  }

  public List<Map<String, Object>> getUserParticipationPatterns(int limit) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(USER_PARTICIPATION_SQL)) {
      ps.setInt(1, limit);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("userId", rs.getString("user_id"));
          row.put("roomId", rs.getString("room_id"));
          row.put("messageCount", rs.getLong("message_count"));
          row.put("firstSeen", rs.getTimestamp("first_seen").toString());
          row.put("lastSeen", rs.getTimestamp("last_seen").toString());
          rows.add(row);
        }
      }
    }
    return rows;
  }

  public long getTotalMessageCount() throws SQLException {
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(TOTAL_COUNT_SQL);
        ResultSet rs = ps.executeQuery()) {
      return rs.next() ? rs.getLong(1) : 0;
    }
  }

  public String getMostActiveUserId() {
    try (Connection conn = ds.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT user_id FROM messages GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 1");
        ResultSet rs = ps.executeQuery()) {
      return rs.next() ? rs.getString("user_id") : "1";
    } catch (Exception e) {
      return "1";
    }
  }

  private List<Map<String, Object>> queryTimeSeries(String sql) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ts", rs.getTimestamp("ts").toString());
        row.put("count", rs.getLong("cnt"));
        rows.add(row);
      }
    }
    return rows;
  }

  private Map<String, Object> rowToMap(ResultSet rs) throws SQLException {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("messageId", rs.getString("message_id"));
    row.put("userId", rs.getString("user_id"));
    row.put("roomId", rs.getString("room_id"));
    row.put("username", rs.getString("username"));
    row.put("message", rs.getString("message"));
    row.put("createdAt", rs.getTimestamp("created_at").toString());
    row.put("messageType", rs.getString("message_type"));
    return row;
  }

  private Timestamp parseTimestamp(String ts) {
    if (ts == null || ts.isBlank()) return Timestamp.from(Instant.now());
    try {
      return Timestamp.from(Instant.parse(ts));
    } catch (Exception e1) {
      try {
        return Timestamp.from(OffsetDateTime.parse(ts).toInstant());
      } catch (Exception e2) {
        return Timestamp.from(Instant.now());
      }
    }
  }
}
