package chatflow.consumer;

import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;

@ServerEndpoint("/receive-message/room/{roomId}")
public class ReceiveEndPoint {

  @OnOpen
  public void onOpen(Session session, @PathParam("roomId") String roomId) {
    RoomManager.addSessionToRoom(roomId, session);
    session.getUserProperties().put("roomId", roomId);
  }

  @OnMessage
  public void onMessage(String message, Session session) {
    // Handle incoming messages if needed
  }

  @OnClose
  public void onClose(Session session) {
    RoomManager.removeSessionFromRoom((String) session.getUserProperties().get("roomId"), session);
  }

  @OnError
  public void onError(Session session, Throwable throwable) {
    // Handle errors if needed
    RoomManager.removeSessionFromRoom((String) session.getUserProperties().get("roomId"), session);
  }
}
