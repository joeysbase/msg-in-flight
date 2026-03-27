package chatflow.consumer;

import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;

@ServerEndpoint("/receive/room/{roomId}")
public class ReceiveEndPoint {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @OnOpen
  public void onOpen(Session session, @PathParam("roomId") String roomId) {
    RoomManager.addSessionToRoom(roomId, session);
    session.getUserProperties().put("roomId", roomId);
  }

  @OnMessage
  public void onMessage(String message, Session session) {
    try {
      String messageId = MAPPER.readTree(message).path("messageId").asText(null);
      if (messageId == null) return;

      CountDownLatch latch = AckManager.getLatch(messageId);
      if (latch != null) latch.countDown();

      CountDownLatch sessionLatch = AckManager.getSessionLatch(messageId, session);
      if (sessionLatch != null) sessionLatch.countDown();
    } catch (Exception e) {
      
    }
  }

  @OnClose
  public void onClose(Session session) {
    RoomManager.removeSessionFromRoom(
        (String) session.getUserProperties().get("roomId"), session);
  }

  @OnError
  public void onError(Session session, Throwable throwable) {
    RoomManager.removeSessionFromRoom(
        (String) session.getUserProperties().get("roomId"), session);
  }
}
