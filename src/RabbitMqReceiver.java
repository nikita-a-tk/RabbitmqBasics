import com.rabbitmq.client.*;

import java.io.IOException;

public class RabbitMqReceiver {
    private Connection connection;
    private Channel directChannel;
    private Channel headersChannel0;
    private Channel headersChannel1;
    private Channel topicChannel0;
    private Channel topicChannel1;
    private boolean verboseMode;

    public RabbitMqReceiver() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        verboseMode = false;
        try {
            connection = factory.newConnection();
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    public void receiveTopicMessages() throws Exception {

    }

    public void receiveMessages() throws IOException {
        directChannel = connection.createChannel();
        setReceivingMessages(directChannel, "my_own_direct_queue");
        headersChannel0 = connection.createChannel();
        setReceivingMessages(headersChannel0, "my_own_headers0_queue");
        headersChannel1 = connection.createChannel();
        setReceivingMessages(headersChannel1, "my_own_headers1_queue");
        topicChannel0 = connection.createChannel();
        setReceivingMessages(topicChannel0, "my_own_topic0_queue");
        topicChannel1 = connection.createChannel();
        setReceivingMessages(topicChannel1, "my_own_topic1_queue");
    }

    private void setReceivingMessages(Channel channel, String queueName) throws IOException {
        channel.basicQos(1);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            if (verboseMode) System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

            try {
                Thread.sleep(50000); // to have time to check queue content
            } catch (InterruptedException exc) {}

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        channel.basicConsume(queueName, false, deliverCallback, consumeTag -> { });
    }

    public void setVerboseMode(boolean verboseMode) {
        this.verboseMode = verboseMode;
    }

    public boolean isVerboseMode() {
        return verboseMode;
    }

    public void close() {
        try {
            directChannel.close();
            headersChannel0.close();
            headersChannel1.close();
            topicChannel0.close();
            topicChannel1.close();
            connection.close();
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }
}
