package chatflow.db.writer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CircuitBreaker {

  public enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  private final int threshold;
  private final long recoveryTimeoutMs;

  private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final AtomicLong lastFailureTime = new AtomicLong(0);

  public CircuitBreaker(int threshold, long recoveryTimeoutMs) {
    this.threshold = threshold;
    this.recoveryTimeoutMs = recoveryTimeoutMs;
  }

  public boolean allowRequest() {
    State s = state.get();
    if (s == State.CLOSED) return true;
    if (s == State.HALF_OPEN) return true;
    // OPEN: check if recovery timeout has elapsed
    if (System.currentTimeMillis() - lastFailureTime.get() > recoveryTimeoutMs) {
      state.compareAndSet(State.OPEN, State.HALF_OPEN);
      System.out.println("CircuitBreaker: OPEN → HALF_OPEN (testing recovery)");
      return true;
    }
    return false;
  }

  public void recordSuccess() {
    State prev = state.getAndSet(State.CLOSED);
    if (prev != State.CLOSED) {
      System.out.println("CircuitBreaker: " + prev + " → CLOSED");
    }
    failureCount.set(0);
  }

  public void recordFailure() {
    lastFailureTime.set(System.currentTimeMillis());
    State s = state.get();
    if (s == State.HALF_OPEN) {
      state.set(State.OPEN);
      System.out.println("CircuitBreaker: HALF_OPEN → OPEN (probe failed)");
      return;
    }
    int count = failureCount.incrementAndGet();
    if (count >= threshold && state.compareAndSet(State.CLOSED, State.OPEN)) {
      System.out.println("CircuitBreaker: CLOSED → OPEN (failures=" + count + ")");
    }
  }

  public State getState() {
    return state.get();
  }

  public int getFailureCount() {
    return failureCount.get();
  }
}
