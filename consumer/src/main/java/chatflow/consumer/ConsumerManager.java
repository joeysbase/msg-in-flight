package chatflow.consumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import chatflow.utils.ChannelPool;

public class ConsumerManager implements Runnable {
  private final ChannelPool pool;
  private final ExecutorService workerPool;
  private final Broadcaster broadcaster;
  private final Map<String, Queue<MessageConsumer>> consumerAssigment = new HashMap<>();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final long monitorInterval;
  private final Set<DeclareOk> queueSet = new HashSet<>();
  private final int prefetchCount;

  public ConsumerManager(
      int consumerPoolSize,
      int messengerPoolSize,
      int workerPoolSize,
      long monitorInterval,
      int prefetchCount) {
    workerPool = Executors.newFixedThreadPool(workerPoolSize);
    pool = new ChannelPool("", consumerPoolSize);
    broadcaster = new Broadcaster(messengerPoolSize);
    this.monitorInterval = monitorInterval;
    this.prefetchCount = prefetchCount;
  }

  public class MessageConsumer extends DefaultConsumer {

    public MessageConsumer(Channel channel) {
      super(channel);
    }

    @Override
    public void handleCancel(String consumerTag) {
      try {
        pool.returnChannel(this.getChannel());
      } catch (InterruptedException e) {
      }
    }

    @Override
    public void handleCancelOk(String consumerTag) {
      try {
        pool.returnChannel(this.getChannel());
      } catch (InterruptedException e) {
      }
    }

    @Override
    public void handleDelivery(
        String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
      workerPool.submit(
          () -> {
            try {
              boolean succeed = broadcaster.send("", envelope.getRoutingKey());
              if (succeed) {
                this.getChannel().basicAck(envelope.getDeliveryTag(), false);
              }
            } catch (IOException e) {
            }
          });
    }
  }

  private void subscribe(String routingKey) throws Exception {
    Channel channel = pool.borrowChannel();
    MessageConsumer consumer = new MessageConsumer(channel);
    channel.basicQos(prefetchCount);
    channel.exchangeDeclare("chat.exchange", "direct");
    DeclareOk queue = channel.queueDeclare(routingKey, true, false, true, null);
    queueSet.add(queue);
    channel.queueBind(routingKey, "chat.exchange", routingKey);
    channel.basicConsume(routingKey, false, consumer);
    consumerAssigment.computeIfAbsent(routingKey, k -> new LinkedList<>()).add(consumer);
  }

  public void stop() {
    running.set(false);
  }

  @Override
  public void run() {
    while (running.get()) {
      try {
        for (DeclareOk queue : queueSet) {
          if (queue.getMessageCount() > 1000) {
            if (pool.isEmpty()) {
              pool.addXChannel(5);
            }
            subscribe(queue.getQueue());
          } else if (queue.getMessageCount() < 500
              && consumerAssigment.get(queue.getQueue()).size() > 1) {
            MessageConsumer consumer = consumerAssigment.get(queue.getQueue()).poll();
            consumer.getChannel().basicCancel(consumer.getConsumerTag());
          }
        }
        Thread.sleep(monitorInterval);
      } catch (Exception e) {
      }
    }
  }
}
