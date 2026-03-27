package chatflow.db.writer;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class WriterStats {

  private final AtomicLong totalWritten = new AtomicLong(0);
  private final AtomicLong totalFailed = new AtomicLong(0);
  private final AtomicLong totalBatches = new AtomicLong(0);
  private final AtomicLong totalDlqAdded = new AtomicLong(0);

  private final ConcurrentLinkedDeque<long[]> writeSamples = new ConcurrentLinkedDeque<>();
  private static final int MAX_SAMPLES = 120;

  private final ConcurrentLinkedDeque<Long> latencySamples = new ConcurrentLinkedDeque<>();
  private static final int MAX_LATENCY_SAMPLES = 1000;

  public void recordWrite(long count, long latencyMs) {
    totalWritten.addAndGet(count);
    totalBatches.incrementAndGet();

    long now = System.currentTimeMillis();
    writeSamples.addLast(new long[] {now, count});
    if (writeSamples.size() > MAX_SAMPLES) writeSamples.pollFirst();

    latencySamples.addLast(latencyMs);
    if (latencySamples.size() > MAX_LATENCY_SAMPLES) latencySamples.pollFirst();
  }

  public void recordFailure(long count) {
    totalFailed.addAndGet(count);
  }

  public void recordDlqAdded(long count) {
    totalDlqAdded.addAndGet(count);
  }

  public long getTotalWritten() {
    return totalWritten.get();
  }

  public long getTotalFailed() {
    return totalFailed.get();
  }

  public long getTotalBatches() {
    return totalBatches.get();
  }

  public long getTotalDlqAdded() {
    return totalDlqAdded.get();
  }

  public double getWritesPerSecond() {
    long windowMs = 10_000;
    long cutoff = System.currentTimeMillis() - windowMs;
    long sum = 0;
    for (long[] sample : writeSamples) {
      if (sample[0] >= cutoff) sum += sample[1];
    }
    return sum / (windowMs / 1000.0);
  }

  public double getAvgLatencyMs() {
    if (latencySamples.isEmpty()) return 0;
    long sum = 0;
    int count = 0;
    for (long l : latencySamples) {
      sum += l;
      count++;
    }
    return count == 0 ? 0 : (double) sum / count;
  }

  public long getP50LatencyMs() {
    return percentile(0.50);
  }

  public long getP95LatencyMs() {
    return percentile(0.95);
  }

  public long getP99LatencyMs() {
    return percentile(0.99);
  }

  private long percentile(double p) {
    if (latencySamples.isEmpty()) return 0;
    long[] sorted = latencySamples.stream().mapToLong(Long::longValue).sorted().toArray();
    int idx = Math.max(0, (int) Math.ceil(sorted.length * p) - 1);
    return sorted[idx];
  }
}
