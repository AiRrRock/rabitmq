package ru.gb.consumer;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Consumer {
    private static final String EXCHANGE_NAME = "topic_exchange";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String queueName = channel.queueDeclare().getQueue();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [*] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });

        boolean exit = false;
        while (!exit) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));

            String message = reader.readLine();
            if ("exit".equals(message)) {
                exit = true;
            } else {
                String[] splitMessage = message.split(" ", 2);
                if(splitMessage.length == 2) {
                    String command = splitMessage[0];
                    String routingKey = splitMessage[1];
                    if("set_topic".equals(command)){
                        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                        System.out.println(" [*] Waiting for messages with routing key (" + routingKey + ")");
                    } else if("remove_topic".equals(command)){
                        channel.queueUnbind(queueName, EXCHANGE_NAME, routingKey);
                        System.out.println(" [*] Stopped waiting for messages with routing key (" + routingKey + ")");
                    } else {
                        System.out.println("Unknown command " + command);
                    }
                }

            }
        }
    }
}
