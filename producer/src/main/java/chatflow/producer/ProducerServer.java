package chatflow.producer;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.Channel;

import chatflow.utils.ChannelPool;
import chatflow.utils.ChatMessage;
import chatflow.utils.MessageValidator;
import chatflow.utils.QueueMessage;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

@ServerEndpoint(value = "/send-message", configurator = IpAwareConfigurator.class)
public class ProducerServer {

  public static Set<Session> sessionSet = ConcurrentHashMap.newKeySet();
  public static Map<Session, String> sessionToIp = new ConcurrentHashMap<>();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ChannelPool POOL = new ChannelPool("", 20);
  private final String serverId;
  private static final String EXCHANGE_NAME = "chat.exchange";

  public ProducerServer(String id) {
    serverId = id;
  }

  private void sendError(String msg, Session session) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("status", "failed");
    node.put("msg", msg);
    node.put("timestamp", Instant.now().toString());
    session.getAsyncRemote().sendText(node.toString());
  }

  private QueueMessage buildQueueMessage(ChatMessage chatMsg, Session session) {
    QueueMessage queueMsg = new QueueMessage();
    queueMsg.messageId = UUID.randomUUID().toString();
    queueMsg.roomId = chatMsg.roomId;
    queueMsg.message = chatMsg.message;
    queueMsg.messageType = chatMsg.messageType;
    queueMsg.userId = chatMsg.userId;
    queueMsg.username = chatMsg.username;
    queueMsg.timestamp = chatMsg.timestamp;
    queueMsg.clientIp = sessionToIp.get(session);
    queueMsg.serverId = this.serverId;
    return queueMsg;
  }

  @OnOpen
  public void onOpen(Session session, EndpointConfig config) {
    sessionSet.add(session);
    sessionToIp.put(session, (String) config.getUserProperties().get("client-ip"));
    System.out.println("log: new user connected.");
  }

  @OnMessage
  public void onMessage(String message, Session session) {
    try {
      // Validate message
      ChatMessage chatMsg = MAPPER.readValue(message, ChatMessage.class);
      String errorMsg = MessageValidator.validate(chatMsg);
      if (errorMsg != null) {
        sendError(errorMsg, session);
        return;
      }

      // Publish to queue
      Channel channel = POOL.borrowChannel();
      QueueMessage queueMsg = buildQueueMessage(chatMsg, session);
      channel.queueDeclare("room." + queueMsg.roomId, true, false, true, null);
      channel.exchangeDeclare("chat.exchange", "direct");
      channel.queueBind("room." + queueMsg.roomId, EXCHANGE_NAME, "room." + queueMsg.roomId);
      channel.basicPublish(
          EXCHANGE_NAME,
          "room." + queueMsg.roomId,
          null,
          MAPPER.writeValueAsString(queueMsg).getBytes());

    } catch (JsonProcessingException e) {
      sendError("Invalid message format", session);
    } catch (InterruptedException | IOException e) {

    }
  }

  @OnClose
  public void onClose(Session session) {
    sessionSet.remove(session);
    System.out.println("Connection closed: " + sessionToIp.get(session));
  }

  @OnError
  public void onError(Session session, Throwable throwable) {}
}
