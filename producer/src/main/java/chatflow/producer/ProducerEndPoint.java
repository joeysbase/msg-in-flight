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
public class ProducerEndPoint {

  public static Set<Session> sessionSet = ConcurrentHashMap.newKeySet();
  public static Map<Session, String> sessionToIp = new ConcurrentHashMap<>();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ChannelPool POOL = new ChannelPool("", 20);
  private final String serverId;
  private static final String EXCHANGE_NAME = "chat.exchange";

  public ProducerEndPoint(String id) {
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

  private void sendAck(Session session){
    ObjectNode node = MAPPER.createObjectNode();
    node.put("status", "succeed");
    node.put("msg", "message received");
    node.put("timestamp", Instant.now().toString());
    session.getAsyncRemote().sendText(node.toString());
  }

  private void backoff(int attempt) {
    try {
      long sleep = (long) Math.pow(2, attempt) * 50;
      Thread.sleep(sleep);
    } catch (InterruptedException ignored) {
    }
  }

  private void publishWithRetry(
      Channel channel, String routingKey, byte[] messageBody, int maxRetries, Session session) {
    int attempt = 0;
    while (attempt < maxRetries) {
      try {
        channel.basicPublish(EXCHANGE_NAME, routingKey, null, messageBody);
        channel.waitForConfirmsOrDie(5000);
        sendAck(session);
        return; // Success
      } catch (Exception e) {
        attempt++;
        if (attempt >= maxRetries) {
          sendError("Failed to publish message after " + maxRetries + " attempts", session);
          return;
        }
        backoff(attempt);
      }
    }
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
      publishWithRetry(channel, "room." + queueMsg.roomId, MAPPER.writeValueAsBytes(queueMsg), 5, session);
      POOL.returnChannel(channel);
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
