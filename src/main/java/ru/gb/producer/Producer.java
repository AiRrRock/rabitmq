package ru.gb.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Producer {
    private static final String EXCHANGE_NAME = "topic_exchange";

    public static void main(String[] args) throws Exception {
        // Enter data using BufferReader
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            boolean exit = false;
            while (!exit) {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(System.in));

                String message = reader.readLine();
                if ("exit".equals(message)) {
                    exit = true;
                } else {
                    String[] splitedMessage = message.split(" ", 2);
                    if (splitedMessage.length == 2) {
                        String routingKey = splitedMessage[0];
                        String sentMessage = splitedMessage[1];

                        channel.basicPublish(EXCHANGE_NAME, routingKey, null, sentMessage.getBytes("UTF-8"));
                        System.out.println(" [x] Sent '" + routingKey + "':'" + sentMessage + "'");
                    } else {
                        System.out.println("Unable to send " + message);
                    }
                }
            }
        }
    }
}

