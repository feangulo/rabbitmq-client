package com.squarespace.rabbitmq;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;

public class Recv {

  private final static String QUEUE_NAME = "myQueue";

  private final static Address[] ADDRESS_NAMES = new Address[] { new Address("rabbit01"),
          new Address("rabbit02"), new Address("rabbit03") };

  private static String CURRENT_HOST_NAME = null;

  public static void main(String args[]) throws IOException, InterruptedException {

    QueueingConsumer consumer = getConsumer();

    while (true) {

      try {

        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        String message = new String(delivery.getBody());
        System.out.println(" [x] @" + CURRENT_HOST_NAME + ": Received '" + message + "'");


      } catch (ConsumerCancelledException ex) {

        System.out.println("Received consumer cancelled exception: " + ex + " - reinitializing...");

        consumer = getConsumer();

      } catch (ShutdownSignalException ex) {

        System.out.println("Received shutdown signal exception: " + ex + " - reinitializing...");

        consumer = getConsumer();

      }

    }

  }

  private static QueueingConsumer getConsumer() throws IOException {

    ConnectionFactory factory = new ConnectionFactory();
    Connection connection = factory.newConnection(ADDRESS_NAMES);
    Channel channel = connection.createChannel();

    CURRENT_HOST_NAME = connection.getAddress().getHostName();
    System.out.println("Connected to: " + connection.getAddress());

    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume(QUEUE_NAME, true, consumer);

    return consumer;

  }

}
