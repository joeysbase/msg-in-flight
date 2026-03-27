package chatflow.db.writer;

import chatflow.db.db.MessageRepository;
import chatflow.utils.QueueMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchMessageWriter {

  private final MessageRepository repository;
  private final CircuitBreaker circuitBreaker;
  private final DeadLetterQueue dlq;
  private final WriterStats stats;

  private final int batchSize;
  private final long flushIntervalMs;

  private final ConcurrentLinkedQueue<QueueMessage> buffer = new ConcurrentLinkedQueue<>();
  private final AtomicInteger bufferSize = new AtomicInteger(0);

  private final ExecutorService writerPool;

  private final ScheduledExecutorService scheduler;

  private final AtomicBoolean flushInProgress = new AtomicBoolean(false);
  private final AtomicBoolean running = new AtomicBoolean(false);

  public BatchMessageWriter(
      MessageRepository repository,
      CircuitBreaker circuitBreaker,
      DeadLetterQueue dlq,
      WriterStats stats,
      int batchSize,
      long flushIntervalMs,
      int writerPoolSize) {
    this.repository = repository;
    this.circuitBreaker = circuitBreaker;
    this.dlq = dlq;
    this.stats = stats;
    this.batchSize = batchSize;
    this.flushIntervalMs = flushIntervalMs;
    this.writerPool = Executors.newFixedThreadPool(writerPoolSize, r -> new Thread(r, "db-writer"));
    this.scheduler = Executors.newScheduledThreadPool(2, r -> new Thread(r, "db-scheduler"));
  }

  public void submit(QueueMessage message) {
    buffer.add(message);
    if (bufferSize.incrementAndGet() >= batchSize) {
      triggerFlush();
    }
  }

  public void submitAll(List<QueueMessage> messages) {
    for (QueueMessage m : messages) submit(m);
  }

  public void start() {
    running.set(true);
    scheduler.scheduleAtFixedRate(
        this::triggerFlush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    scheduler.scheduleAtFixedRate(this::refreshStats, 30, 30, TimeUnit.SECONDS);
    System.out.printf(
        "BatchMessageWriter started: batchSize=%d flushInterval=%dms writers=%d%n",
        batchSize, flushIntervalMs, ((ThreadPoolExecutor) writerPool).getCorePoolSize());
  }

  private void triggerFlush() {
    if (!flushInProgress.compareAndSet(false, true)) return;
    writerPool.submit(
        () -> {
          try {
            flush();
          } finally {
            flushInProgress.set(false);
          }
        });
  }

  private void flush() {
    if (buffer.isEmpty()) return;

    List<QueueMessage> batch = new ArrayList<>(batchSize);
    QueueMessage msg;
    while (batch.size() < batchSize && (msg = buffer.poll()) != null) {
      batch.add(msg);
      bufferSize.decrementAndGet();
    }
    if (batch.isEmpty()) return;

    if (!circuitBreaker.allowRequest()) {
      System.err.println("BatchWriter: circuit OPEN — routing " + batch.size() + " to DLQ");
      dlq.addAll(batch, "circuit breaker OPEN");
      stats.recordFailure(batch.size());
      return;
    }

    long start = System.currentTimeMillis();
    try {
      int written = repository.insertBatch(batch);
      long latency = System.currentTimeMillis() - start;
      stats.recordWrite(written, latency);
      circuitBreaker.recordSuccess();
    } catch (Exception e) {
      long latency = System.currentTimeMillis() - start;
      System.err.println("BatchWriter: flush failed (" + latency + "ms): " + e.getMessage());
      circuitBreaker.recordFailure();
      stats.recordFailure(batch.size());
      dlq.addAll(batch, e.getMessage());
    }
  }

  private void refreshStats() {
    try {
      chatflow.db.db.SchemaInitializer.refreshRoomStats(chatflow.db.db.ConnectionPool.get());
    } catch (Exception e) {
      // non-critical
    }
  }

  public int getBufferSize() {
    return bufferSize.get();
  }

  public void stop() {
    running.set(false);
    scheduler.shutdown();
    // Final flush
    flush();
    writerPool.shutdown();
    try {
      writerPool.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
