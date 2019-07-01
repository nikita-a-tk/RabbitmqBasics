import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
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

    private void publishMessage(String exchangeName, String exchangeType, String routingKey, String message) throws IOException {
        channel.exchangeDeclare(exchangeName, exchangeType);
        channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
        System.out.println(" [x] Sent '" + routingKey + ": " + message + "'");
    }

    public void sendMessage(String exchangeName, String exchangeType) throws IOException {
        channel.exchangeDeclare(exchangeName, exchangeType);

        String message;
        switch (exchangeType) {
            case "direct":
                message = new SimpleDateFormat("HH.mm.ss.S").format(new Timestamp(System.currentTimeMillis())) + " - Direct Message CATS";
                publishMessage(exchangeName, exchangeType, "cats_are_awesome", message);
                break;
            case "topic":
                message = new SimpleDateFormat("HH.mm.ss.S").format(new Timestamp(System.currentTimeMillis())) + " - Topic Message PUPPY";
                publishMessage(exchangeName, exchangeType, "puppy", message);
                message = new SimpleDateFormat("HH.mm.ss.S").format(new Timestamp(System.currentTimeMillis())) + " - Topic Message PUPPY CORGI";
                publishMessage(exchangeName, exchangeType, "puppy corgi", message);
                break;
            case "headers":
                message = new SimpleDateFormat("HH.mm.ss.S").format(new Timestamp(System.currentTimeMillis())) + " - Headers Message ALPACA";
                publishMessage(exchangeName, exchangeType, "brown.alpaca.runs", message);
                message = new SimpleDateFormat("HH.mm.ss.S").format(new Timestamp(System.currentTimeMillis())) + " - Headers Message BEAR";
                publishMessage(exchangeName, exchangeType, "brown.siberian.bear.roars", message);
                break;
        }
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

                if (name != null && type != null) {
                    String[] exchange = {name, type};
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
    }

    private void bindQueuesExchanges() throws IOException {
        channel.queueBind("my_own_direct_queue", "my_own_direct_exchange", "cats_are_awesome");
        channel.queueBind("my_own_headers0_queue", "my_own_headers_exchange", "*.alpaca.*");
        channel.queueBind("my_own_headers0_queue", "my_own_headers_exchange", "brown.#");
        channel.queueBind("my_own_topic0_queue", "my_own_topic_exchange", "puppy");
        channel.queueBind("my_own_topic1_queue", "my_own_topic_exchange", "puppy corgi");
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

    public ArrayList<String[]> getQueueContent(String queueName) {

        String resultJson = makePostRequest("http://localhost:15672/api/queues/%2F/" + queueName + "/get",
                "count=5&ackmode=ack_requeue_true&encoding=auto&truncate=50000");

        ArrayList<String[]> messages = parseQueuesJson(resultJson);

        return messages;
    }

    private String makePostRequest(String requestPath, String requestBody) {
        try {
            URL url = new URL(requestPath);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            connection.setDoOutput(true);
            connection.setRequestMethod("POST");

            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication("guest", "guest".toCharArray());
                }
            });

            byte[] out = "{\"count\":5,\"ackmode\":\"ack_requeue_true\",\"encoding\":\"auto\",\"truncate\":50000}" .getBytes(StandardCharsets.UTF_8);
            int length = out.length;

            connection.setFixedLengthStreamingMode(length);
            connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            connection.connect();
            try(OutputStream os = connection.getOutputStream()) {
                os.write(out);
            }

            connection.connect();
            int responseCode = connection.getResponseCode();

            if (responseCode != 200) {
                throw new RuntimeException("HttpResponseCode: " + responseCode);
            } else {
                Scanner scanner = new Scanner(connection.getInputStream());
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

    private ArrayList<String[]> parseQueuesJson(String json) {
        ArrayList<String[]> messages = new ArrayList<>();

        String[] splittedJson = json.split(":|,");
        String exchange = null;
        String routingKey = null;
        String text = null;
        for (int i = 0; i < splittedJson.length - 1; i++) {
            if (splittedJson[i].contains("exchange") && splittedJson[i + 1].contains("my_own")) {
                exchange = splittedJson[i + 1].replace("\"", "");
            } else if (splittedJson[i].contains("routing_key")) {
                routingKey = splittedJson[i + 1].replace("\"", "");
            } else if (splittedJson[i].contains("payload") && !splittedJson[i].contains("bytes") && !splittedJson[i].contains("encoding")) {
                text = splittedJson[i + 1].replace("\"", "");
            }

            if (exchange != null && routingKey != null && text != null) {
                String[] message = {exchange, routingKey, text};
                messages.add(message);
                exchange = null;
                routingKey = null;
                text = null;
                i++;
            }
        }

        return messages;
    }

    public void close() {
        try {
            channel.close();
            connection.close();
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }
}
