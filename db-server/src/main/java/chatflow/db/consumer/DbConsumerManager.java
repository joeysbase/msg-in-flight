package chatflow.db.consumer;

import chatflow.db.writer.BatchMessageWriter;
import chatflow.utils.ChannelPool;
import chatflow.utils.QueueMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class DbConsumerManager implements Runnable {

  private static final String EXCHANGE = "chat.exchange";
  private static final int NUM_ROOMS = 20;
  private static final String QUEUE_PREFIX = "db.room.";

  private final ChannelPool pool;
  private final BatchMessageWriter writer;
  private final ExecutorService consumerPool;
  private final ObjectMapper mapper = new ObjectMapper();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final int prefetchCount;

  public DbConsumerManager(
      String mqHost,
      int channelPoolSize,
      int consumerPoolSize,
      int prefetchCount,
      BatchMessageWriter writer) {
    this.pool = new ChannelPool(mqHost, channelPoolSize);
    this.writer = writer;
    this.consumerPool =
        Executors.newFixedThreadPool(consumerPoolSize, r -> new Thread(r, "db-consumer"));
    this.prefetchCount = prefetchCount;
    pool.init();
  }

  @Override
  public void run() {
    int subscribed = 0;
    for (int i = 1; i <= NUM_ROOMS; i++) {
      String routingKey = "room." + i;
      String queueName = QUEUE_PREFIX + i;
      try {
        subscribe(routingKey, queueName);
        subscribed++;
      } catch (Exception e) {
        System.err.println(
            "DbConsumer: failed to subscribe to "
                + queueName
                + " ["
                + e.getClass().getSimpleName()
                + "]: "
                + e.getMessage());
      }
    }
    System.out.println(
        "DbConsumerManager: subscribed to " + subscribed + "/" + NUM_ROOMS + " rooms");

    while (running.get()) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void subscribe(String routingKey, String queueName) throws Exception {
    Channel channel = pool.borrowChannel();
    try {
      channel.basicQos(prefetchCount);

      channel.exchangeDeclarePassive(EXCHANGE);

      channel.queueDeclare(queueName, true, false, false, null);
      channel.queueBind(queueName, EXCHANGE, routingKey);

      channel.basicConsume(
          queueName,
          false,
          new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(
                String consumerTag,
                Envelope envelope,
                AMQP.BasicProperties properties,
                byte[] body) {
              consumerPool.submit(
                  () -> {
                    try {
                      String json = new String(body, StandardCharsets.UTF_8);
                      QueueMessage msg = mapper.readValue(json, QueueMessage.class);
                      writer.submit(msg);
                      channel.basicAck(envelope.getDeliveryTag(), false);
                    } catch (Exception e) {
                      try {
                        channel.basicNack(envelope.getDeliveryTag(), false, true);
                      } catch (IOException ex) {
                        // ignore
                      }
                    }
                  });
            }
          });
    } catch (Exception e) {

      try {
        pool.returnChannel(channel);
      } catch (Exception ignored) {
      }
      throw e;
    }
  }

  public void stop() {
    running.set(false);
    consumerPool.shutdown();
  }
}
