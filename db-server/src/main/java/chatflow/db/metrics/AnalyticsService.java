package chatflow.db.metrics;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import chatflow.db.db.ConnectionPool;
import chatflow.db.db.DbMetricsRepository;
import chatflow.db.db.MessageRepository;
import chatflow.db.writer.BatchMessageWriter;
import chatflow.db.writer.CircuitBreaker;
import chatflow.db.writer.DeadLetterQueue;
import chatflow.db.writer.WriterStats;

public class AnalyticsService {

  private final MessageRepository repo;
  private final DbMetricsRepository dbMetrics;
  private final WriterStats writerStats;
  private final CircuitBreaker circuitBreaker;
  private final DeadLetterQueue dlq;
  private final BatchMessageWriter writer;

  private volatile Map<String, Object> cachedResult = null;
  private volatile long cacheExpiry = 0;
  private static final long CACHE_TTL_MS = 10_000;

  public AnalyticsService(
      MessageRepository repo,
      DbMetricsRepository dbMetrics,
      WriterStats writerStats,
      CircuitBreaker circuitBreaker,
      DeadLetterQueue dlq,
      BatchMessageWriter writer) {
    this.repo = repo;
    this.dbMetrics = dbMetrics;
    this.writerStats = writerStats;
    this.circuitBreaker = circuitBreaker;
    this.dlq = dlq;
    this.writer = writer;
  }

  public Map<String, Object> getMetrics() {
    long now = System.currentTimeMillis();
    if (cachedResult != null && now < cacheExpiry) return cachedResult;

    Map<String, Object> result = buildMetrics();
    cachedResult = result;
    cacheExpiry = now + CACHE_TTL_MS;
    return result;
  }

  private Map<String, Object> buildMetrics() {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("generatedAt", Instant.now().toString());
    out.put("coreQueries", buildCoreQueries());
    out.put("analytics", buildAnalytics());
    out.put("dbMetrics", buildDbMetrics());
    out.put("writerMetrics", buildWriterMetrics());
    return out;
  }

  private Map<String, Object> buildCoreQueries() {
    Map<String, Object> out = new LinkedHashMap<>();

    Instant now = Instant.now();
    Instant fiveMinAgo = now.minus(5, ChronoUnit.MINUTES);
    Timestamp tsStart = Timestamp.from(fiveMinAgo);
    Timestamp tsEnd = Timestamp.from(now);

    // Query 1: messages in room.1 in last 5 minutes
    try {
      List<Map<String, Object>> msgs = repo.getMessagesForRoom("room.1", tsStart, tsEnd, 1000);
      Map<String, Object> q1 = new LinkedHashMap<>();
      q1.put("description", "Messages in room.1 for last 5 minutes");
      q1.put("roomId", "room.1");
      q1.put("timeRange", fiveMinAgo + " → " + now);
      q1.put("count", msgs.size());
      q1.put("messages", msgs.size() <= 20 ? msgs : msgs.subList(0, 20));
      out.put("messagesInTimeRange", q1);
    } catch (Exception e) {
      out.put("messagesInTimeRange", Map.of("error", e.getMessage()));
    }

    // Query 2: most active user's history
    String sampleUserId = repo.getMostActiveUserId();
    try {
      Timestamp histStart = Timestamp.from(now.minus(1, ChronoUnit.HOURS));
      List<Map<String, Object>> userMsgs =
          repo.getUserMessages(sampleUserId, histStart, tsEnd, 1000);
      Map<String, Object> q2 = new LinkedHashMap<>();
      q2.put("description", "Message history for most active user (last 1h)");
      q2.put("userId", sampleUserId);
      q2.put("count", userMsgs.size());
      q2.put("messages", userMsgs.size() <= 20 ? userMsgs : userMsgs.subList(0, 20));
      out.put("userMessageHistory", q2);
    } catch (Exception e) {
      out.put("userMessageHistory", Map.of("error", e.getMessage()));
    }

    // Query 3: active users in last 5 minutes
    try {
      long activeUsers = repo.countActiveUsers(tsStart, tsEnd);
      Map<String, Object> q3 = new LinkedHashMap<>();
      q3.put("description", "Unique active users in last 5 minutes");
      q3.put("timeRange", fiveMinAgo + " → " + now);
      q3.put("activeUserCount", activeUsers);
      out.put("activeUsers", q3);
    } catch (Exception e) {
      out.put("activeUsers", Map.of("error", e.getMessage()));
    }

    // Query 4: rooms for sample user
    try {
      List<Map<String, Object>> rooms = repo.getRoomsForUser(sampleUserId);
      Map<String, Object> q4 = new LinkedHashMap<>();
      q4.put("description", "Rooms participated in by most active user");
      q4.put("userId", sampleUserId);
      q4.put("rooms", rooms);
      out.put("userRooms", q4);
    } catch (Exception e) {
      out.put("userRooms", Map.of("error", e.getMessage()));
    }

    return out;
  }

  private Map<String, Object> buildAnalytics() {
    Map<String, Object> out = new LinkedHashMap<>();

    try {
      out.put("messagesPerSecond", repo.getMessagesPerSecond());
    } catch (Exception e) {
      out.put("messagesPerSecond", Map.of("error", e.getMessage()));
    }

    try {
      out.put("messagesPerMinute", repo.getMessagesPerMinute());
    } catch (Exception e) {
      out.put("messagesPerMinute", Map.of("error", e.getMessage()));
    }

    try {
      out.put("mostActiveUsers", repo.getTopUsers(10));
    } catch (Exception e) {
      out.put("mostActiveUsers", Map.of("error", e.getMessage()));
    }

    try {
      out.put("mostActiveRooms", repo.getTopRooms());
    } catch (Exception e) {
      out.put("mostActiveRooms", Map.of("error", e.getMessage()));
    }

    try {
      out.put("userParticipationPatterns", repo.getUserParticipationPatterns(20));
    } catch (Exception e) {
      out.put("userParticipationPatterns", Map.of("error", e.getMessage()));
    }

    return out;
  }

  private Map<String, Object> buildDbMetrics() {
    Map<String, Object> out = new LinkedHashMap<>();

    // Total row count from the application table
    try {
      out.put("totalMessages", repo.getTotalMessageCount());
    } catch (Exception e) {
      out.put("totalMessages", "error: " + e.getMessage());
    }

    // HikariCP pool state
    out.put(
        "hikariPool",
        Map.of(
            "active", ConnectionPool.getActiveConnections(),
            "idle", ConnectionPool.getIdleConnections(),
            "total", ConnectionPool.getTotalConnections()));

    out.putAll(dbMetrics.collect());

    return out;
  }

  private Map<String, Object> buildWriterMetrics() {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("totalWritten", writerStats.getTotalWritten());
    out.put("totalFailed", writerStats.getTotalFailed());
    out.put("totalBatches", writerStats.getTotalBatches());
    out.put("dlqSize", dlq.size());
    out.put("dlqTotalAdded", writerStats.getTotalDlqAdded());
    out.put("writesPerSecond", Math.round(writerStats.getWritesPerSecond() * 10) / 10.0);
    out.put("avgWriteLatencyMs", Math.round(writerStats.getAvgLatencyMs() * 10) / 10.0);
    out.put("p50WriteLatencyMs", writerStats.getP50LatencyMs());
    out.put("p95WriteLatencyMs", writerStats.getP95LatencyMs());
    out.put("p99WriteLatencyMs", writerStats.getP99LatencyMs());
    out.put("bufferSize", writer.getBufferSize());
    out.put(
        "circuitBreaker",
        Map.of(
            "state", circuitBreaker.getState().name(),
            "failureCount", circuitBreaker.getFailureCount()));
    return out;
  }
}
