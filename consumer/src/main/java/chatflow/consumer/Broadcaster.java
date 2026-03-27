package chatflow.consumer;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import chatflow.utils.QueueMessage;
import jakarta.websocket.Session;

public class Broadcaster {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Shared scheduler for non-blocking delivery retries. Daemon so it does not
   *  prevent JVM shutdown. */
  private static final ScheduledExecutorService RETRY_SCHEDULER =
      Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "messenger-retry");
        t.setDaemon(true);
        return t;
      });

  private final ExecutorService messengerPool;

  private static final class Messenger implements Runnable {

    private static final int MAX_RETRIES  = 3;
    private static final long BASE_DELAY  = 50; // ms

    private final String  message;
    private final Session session;
    private final String  messageId;

    Messenger(String message, Session session, String messageId) {
      this.message   = message;
      this.session   = session;
      this.messageId = messageId;
    }

    @Override
    public void run() {
      attemptSend(0);
    }

    private void attemptSend(int attempt) {
      if (!session.isOpen()) return;

      session.getAsyncRemote().sendText(message, result -> {
        if (result.isOK()) {
          IdempotencyCache.add(messageId, session);

        } else if (session.isOpen() && attempt < MAX_RETRIES - 1) {
          long delayMs = BASE_DELAY * (1L << attempt);
          RETRY_SCHEDULER.schedule(
              () -> attemptSend(attempt + 1), delayMs, TimeUnit.MILLISECONDS);

        } else {
          if (attempt >= MAX_RETRIES - 1) {
            System.err.println("Broadcaster: failed to deliver " + messageId
                + " to session " + session.getId() + " after " + MAX_RETRIES + " attempts");
          }
        }
      });
    }
  }

  // ----------------------------------------------------------------------- //

  public Broadcaster(int messengerPoolSize) {
    messengerPool = Executors.newFixedThreadPool(messengerPoolSize);
  }

  public boolean send(String message, String routingKey) {
    String roomId = routingKey.substring(routingKey.lastIndexOf('.') + 1);
    Set<Session> sessions = RoomManager.getSessionsByRoomId(roomId);
    if (sessions == null || sessions.isEmpty()) return true;

    String messageId;
    try {
      messageId = MAPPER.readValue(message, QueueMessage.class).messageId;
    } catch (Exception e) {
      return true; // malformed message — ack and drop rather than requeue forever
    }
    if (messageId == null) return true;

    for (Session session : sessions) {
      if (!IdempotencyCache.isMessageSent(messageId, session)) {
        messengerPool.submit(new Messenger(message, session, messageId));
      }
    }
    return true;
  }
}
