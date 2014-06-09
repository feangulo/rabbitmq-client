package com.squarespace.rabbitmq;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.ArrayList;

public class Send {

  private static final String QUEUE_NAME = "myQueue";

  private final static Address[] ADDRESS_NAMES = new Address[] { new Address("rabbit01"),
          new Address("rabbit02"), new Address("rabbit03") };

  private static String CURRENT_HOST_NAME = null;

  public static void main(String args[]) throws IOException, InterruptedException {

    Channel channel = getChannel();

    // Delete the queue
    //channel.queueDelete(QUEUE_NAME);

    while (true) {

      try {

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        for (int outer_loop = 0; outer_loop < 100; outer_loop++) {

          for (int inner_loop = 0; inner_loop < 100000; inner_loop++) {
            String message = "Hello, World " + outer_loop + "/" + inner_loop + "!";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] " + CURRENT_HOST_NAME + "@" + System.currentTimeMillis() + ": Sent '" + message +
            "'");

          }

          Thread.sleep(2000);
        }

      } catch (Exception ex) {

        System.out.println("Received exception: " + ex + " - reinitializing...");

        Thread.sleep(10000);

        channel = getChannel();

        Thread.sleep(10000);


      }

    }

  }

  public static Channel getChannel() throws IOException {

    ConnectionFactory factory = new ConnectionFactory();
    Connection connection = factory.newConnection(ADDRESS_NAMES);
    Channel channel = connection.createChannel();

    CURRENT_HOST_NAME = connection.getAddress().getHostName();
    System.out.println("Connected to: " + connection.getAddress());

    channel.queueDeclare(QUEUE_NAME, false, false, false, null);

    return channel;

  }

}