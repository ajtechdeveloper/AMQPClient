package com.aj.amqpclient;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


public class AMQPClient {

    private static final Logger logger = LoggerFactory.getLogger(AMQPClient.class);
    public static final String QUEUE_NAME = "mqttserver";

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        String message;
        String sendTopicName = "echo";
        String receiveTopicName = "thing";
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setVirtualHost("/");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME, "amq.topic", receiveTopicName);
        logger.info(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                logger.info(" [x] Received '" + message + "'");
            }
        };
        channel.basicConsume(QUEUE_NAME, true, consumer);
        Thread.sleep(15000);
        for (int i = 1; i < 6; i++) {
            message = "Message " + i + " from AMQP Client!";
            channel.basicPublish("amq.topic", sendTopicName, null, message.getBytes("UTF-8"));
            logger.info(" [x] Sent '" + message + "'");
        }
    }
}
