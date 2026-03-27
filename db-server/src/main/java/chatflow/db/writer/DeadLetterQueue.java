package chatflow.db.writer;

import chatflow.utils.QueueMessage;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class DeadLetterQueue implements Runnable {

  private static final int MAX_RETRIES = 5;
  private static final long BASE_DELAY = 100; // ms
  private static final long MAX_DELAY = 30_000; // ms

  public static class FailedMessage {
    public final QueueMessage message;
    public int retryCount;
    public long nextRetryAt;
    public String lastError;

    public FailedMessage(QueueMessage message, String error) {
      this.message = message;
      this.lastError = error;
      this.retryCount = 0;
      this.nextRetryAt = System.currentTimeMillis() + BASE_DELAY;
    }
  }

  private final ConcurrentLinkedQueue<FailedMessage> queue = new ConcurrentLinkedQueue<>();
  private final Consumer<List<QueueMessage>> retryCallback;
  private final WriterStats stats;
  private final AtomicBoolean running = new AtomicBoolean(true);

  public DeadLetterQueue(Consumer<List<QueueMessage>> retryCallback, WriterStats stats) {
    this.retryCallback = retryCallback;
    this.stats = stats;
  }

  public void add(QueueMessage message, String error) {
    queue.add(new FailedMessage(message, error));
    stats.recordDlqAdded(1);
  }

  public void addAll(List<QueueMessage> messages, String error) {
    for (QueueMessage m : messages) add(m, error);
  }

  public int size() {
    return queue.size();
  }

  public void stop() {
    running.set(false);
  }

  @Override
  public void run() {
    while (running.get()) {
      try {
        processQueue();
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void processQueue() {
    long now = System.currentTimeMillis();
    List<QueueMessage> readyForRetry = new ArrayList<>();
    List<FailedMessage> keepForLater = new ArrayList<>();

    FailedMessage fm;
    while ((fm = queue.poll()) != null) {
      if (fm.retryCount >= MAX_RETRIES) {
        permanentlyFail(fm);
      } else if (fm.nextRetryAt <= now) {
        readyForRetry.add(fm.message);
        fm.retryCount++;
        fm.nextRetryAt = now + Math.min(BASE_DELAY * (1L << fm.retryCount), MAX_DELAY);
        keepForLater.add(fm);
      } else {
        keepForLater.add(fm);
      }
    }

    queue.addAll(keepForLater);

    if (!readyForRetry.isEmpty()) {
      System.out.println("DLQ: retrying " + readyForRetry.size() + " messages");
      retryCallback.accept(readyForRetry);
    }
  }

  private void permanentlyFail(FailedMessage fm) {
    System.err.println(
        "DLQ: permanently failed messageId=" + fm.message.messageId + " lastError=" + fm.lastError);
    try (PrintWriter pw = new PrintWriter(new FileWriter("dead-letter.log", true))) {
      pw.println(
          Instant.now()
              + " | messageId="
              + fm.message.messageId
              + " | userId="
              + fm.message.userId
              + " | roomId="
              + fm.message.roomId
              + " | error="
              + fm.lastError);
    } catch (IOException e) {
      System.err.println("DLQ: could not write to dead-letter.log: " + e.getMessage());
    }
  }
}
