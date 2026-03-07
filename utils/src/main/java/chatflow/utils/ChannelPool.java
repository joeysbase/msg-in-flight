package chatflow.utils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ChannelPool {
  private final Queue<Channel> POOL;
  private final ConnectionFactory FACTORY;
  private Connection connection = null;
  private int poolSize;

  public ChannelPool(String host, int poolSize) {
    this.FACTORY = new ConnectionFactory();
    FACTORY.setHost(host);
    this.POOL = new LinkedList<>();
    this.poolSize = poolSize;
  }

  public boolean isEmpty() {
    return this.POOL.isEmpty();
  }

  public void addXChannel(int x) {
    this.poolSize += x;
    try {
      if (connection == null || !connection.isOpen()) {
        connection = this.FACTORY.newConnection();
      }
      for (int i = 0; i < x; i++) {
        Channel channel = connection.createChannel();
        channel.confirmSelect();
        this.POOL.add(channel);
      }
    } catch (Exception e) {
    }
  }

  public void init() {
    try {
      this.connection = this.FACTORY.newConnection();
      for (int i = 0; i < this.poolSize; i++) {
        Channel channel = connection.createChannel();
        channel.confirmSelect();
        this.POOL.add(channel);
      }
    } catch (IOException e) {
      System.err.println(
          "Error: initialization failed because connection to the queue is not available.");
      System.err.println(e);
    } catch (TimeoutException e) {
      System.err.println(
          "Error: initialization failed because connection to the queue is not available.");
      System.err.println(e);
    } catch (Exception e) {
      System.err.println(
          "Error: initialization failed because connection to the queue is not available.");
      System.err.println(e);
    }
  }

  public synchronized Channel borrowChannel() throws InterruptedException {
    while (this.POOL.isEmpty()) {
      wait();
    }
    Channel channel = this.POOL.poll();
    notifyAll();
    return channel;
  }

  public synchronized void returnChannel(Channel channel) throws InterruptedException {
    while (this.POOL.size() == this.poolSize) {
      wait();
    }
    this.POOL.add(channel);
    notifyAll();
  }

  public void cleanup() {
    for (Channel channel : this.POOL) {
      try {
        channel.close();
      } catch (IOException e) {
        System.err.println("Error: failed to close the channel.");
        System.err.println(e);
      } catch (TimeoutException e) {
        System.err.println("Error: failed to close the channel.");
        System.err.println(e);
      } catch (Exception e) {
        System.err.println("Error: failed to close the channel.");
        System.err.println(e);
      }
    }
  }
}
