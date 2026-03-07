package chatflow.consumer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.websocket.Session;

public class RoomManager {
  private static final ConcurrentHashMap<String, Set<Session>> roomSessions =
      new ConcurrentHashMap<>();

  // private static final RoomMonitor monitor = new RoomMonitor();

  // public static class RoomMonitor {

  //   public synchronized void waitIfNoUser() throws Exception {
  //     if (isEmpty()) {
  //       wait();
  //     }
  //   }

  //   public synchronized void notifyNewUserJoin() throws Exception {
  //     notifyAll();
  //   }
  // }

  public static void addSessionToRoom(String roomId, Session session) {
    roomSessions.computeIfAbsent(roomId, k -> ConcurrentHashMap.newKeySet()).add(session);
    try {
      // monitor.notifyNewUserJoin();
    } catch (Exception e) {
    }
  }

  public static void removeSessionFromRoom(String roomId, Session session) {
    Set<Session> sessions = roomSessions.get(roomId);
    if (session != null) {
      sessions.remove(session);
      if (sessions.isEmpty()) {
        roomSessions.remove(roomId);
      }
    }
  }

  public static Set<Session> getSessionsByRoomId(String roomId) {
    return roomSessions.getOrDefault(roomId, ConcurrentHashMap.newKeySet());
  }

  public static Set<String> getRoomSet() {
    return roomSessions.keySet();
  }

  public static boolean isEmpty() {
    return roomSessions.isEmpty();
  }

  
}
