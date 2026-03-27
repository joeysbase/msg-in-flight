package chatflow.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import jakarta.websocket.Session;

public class AckManager {

  private static final ConcurrentHashMap<String, CountDownLatch> ackLatches     = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, CountDownLatch> sessionLatches = new ConcurrentHashMap<>();

  
  public static void addLatch(String messageId, CountDownLatch latch) {
    ackLatches.put(messageId, latch);
  }

  public static CountDownLatch getLatch(String messageId) {
    return ackLatches.get(messageId);
  }

  public static void removeLatch(String messageId) {
    ackLatches.remove(messageId);
  }

  private static String sessionKey(String messageId, Session session) {
    return messageId + ':' + session.getId();
  }

  public static void addSessionLatch(String messageId, Session session, CountDownLatch latch) {
    sessionLatches.put(sessionKey(messageId, session), latch);
  }

  public static CountDownLatch getSessionLatch(String messageId, Session session) {
    return sessionLatches.get(sessionKey(messageId, session));
  }

  public static void removeSessionLatch(String messageId, Session session) {
    sessionLatches.remove(sessionKey(messageId, session));
  }
}
