import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.*;

public class RabbitMqOperator {

    private Connection connection;
    private Channel channel;

    ArrayList<String[]> exchanges = new ArrayList<>();
    ArrayList<String> queues = new ArrayList<>();

    // TODO: don't forget to close connection in the end!!!

    public RabbitMqOperator() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();

            createQueues();
            createExchanges();
            bindQueuesExchanges();
        } catch (Exception exc) {
            exc.printStackTrace();
        }

        listExchanges();
        listQueues();
    }

    public void publishMessage(String exchangeName, String exchangeType, String routingKey, String message) throws IOException {
        channel.exchangeDeclare(exchangeName, exchangeType);
        channel.basicPublish(exchangeName, routingKey, null, message.getBytes());

        System.out.println(" [x] Sent '" + message + "'");
    }

    private void listExchanges() {
        String resultJson = makeGetRequest("http://localhost:15672/api/exchanges");

        if (resultJson != null) {
            String[] splittedJson = resultJson.split(":|,");
            String name = null;
            String type = null;
            for (int i = 0; i < splittedJson.length - 1; i++) {
                if (splittedJson[i].contains("name") && splittedJson[i + 1].contains("my_own")) {
                    name = splittedJson[i + 1].replace("\"", "");
                }
                if (splittedJson[i].contains("type") && name != null) {
                    type = splittedJson[i + 1].replace("\"", "");
                }

                String[] exchange = {name, type};
                if (name != null && type != null) {
                    exchanges.add(exchange);
                    name = null;
                    type = null;
                    i++;
                }
            }
        } else {
            throw new NullPointerException();
        }
    }

    private void createQueues() throws IOException {
        boolean durable = true;
        channel.queueDeclare("my_own_direct_queue", durable, false, false, null);
        channel.queueDeclare("my_own_headers0_queue", durable, false, false, null);
        channel.queueDeclare("my_own_headers1_queue", durable, false, false, null);
        channel.queueDeclare("my_own_topic0_queue", durable, false, false, null);
        channel.queueDeclare("my_own_topic1_queue", durable, false, false, null);
    }

    private void createExchanges() throws IOException {
        channel.exchangeDeclare("my_own_direct_exchange", BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare("my_own_headers_exchange", BuiltinExchangeType.HEADERS);
        channel.exchangeDeclare("my_own_topic_exchange", BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare("my_own_fanout_exchange", BuiltinExchangeType.FANOUT);
    }

    private void bindQueuesExchanges() throws IOException {
        channel.queueBind("my_own_direct_queue", "my_own_direct_exchange", "my_own_severity");
        channel.queueBind("my_own_direct_queue", "my_own_direct_exchange", "my_own_severity");
        channel.queueBind("my_own_headers0_queue", "my_own_headers_exchange", "*.own.*");
        channel.queueBind("my_own_headers0_queue", "my_own_headers_exchange", "my.#");
        channel.queueBind("my_own_topic0_queue", "my_own_topic_exchange", "info");
        channel.queueBind("my_own_topic1_queue", "my_own_topic_exchange", "info warning");
    }

    public ArrayList<String[]> getExchanges() {
        return exchanges;
    }

    public ArrayList<String> getQueues() {
        return queues;
    }

    private void listQueues() {

        String resultJson = makeGetRequest("http://localhost:15672/api/queues");

        if (resultJson != null) {
            String[] splittedJson = resultJson.split(":|,");
            String name = null;
            for (int i = 0; i < splittedJson.length - 1; i++) {
                if (splittedJson[i].contains("name") && splittedJson[i + 1].contains("my_own")) {
                    name = splittedJson[i + 1].replace("\"", "");
                }

                if (name != null) {
                    queues.add(name);
                    name = null;
                    i++;
                }
            }

        } else {
            throw new NullPointerException();
        }
    }

    private String makeGetRequest(String requestPath) {
        try {
            URL url = new URL(requestPath);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication("guest", "guest".toCharArray());
                }
            });

            connection.connect();
            int responseCode = connection.getResponseCode();

            if (responseCode != 200) {
                throw new RuntimeException("HttpResponseCode: " + responseCode);
            } else {
                Scanner scanner = new Scanner(url.openStream());
                String resultJson = new String();
                while (scanner.hasNext()) {
                    resultJson += scanner.nextLine();
                }
                scanner.close();

                return resultJson;
            }

        } catch (Exception exc) {
            exc.printStackTrace();
        }

        return null;
    }

    private String makePostRequest(String requestPath, String Body) {
        return null;
    }
}
