import com.rabbitmq.client.*;

import java.io.IOException;

public class RabbitMqReceiver {
    Connection connection;

    public RabbitMqReceiver() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try {
            connection = factory.newConnection();
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    public void receiveTopicMessages() throws Exception {

    }

    public void receiveMessages() throws IOException {
        Channel directChannel = connection.createChannel();
        setReceivingMessages(directChannel, "my_own_direct_queue");
        Channel headers0Channel = connection.createChannel();
        setReceivingMessages(headers0Channel, "my_own_headers0_queue");
        Channel headers1Channel = connection.createChannel();
        setReceivingMessages(headers1Channel, "my_own_headers1_queue");
        Channel topic0Channel = connection.createChannel();
        setReceivingMessages(topic0Channel, "my_own_topic0_queue");
        Channel topic1Channel = connection.createChannel();
        setReceivingMessages(topic1Channel, "my_own_topic1_queue");
    }

    private void setReceivingMessages(Channel channel, String queueName) throws IOException {
        channel.basicQos(1);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

            try {

            Thread.sleep(5000);
            } catch (Exception exc) {}

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        channel.basicConsume(queueName, false, deliverCallback, consumeTag -> { });
    }
}
