import java.io.IOException;
import java.util.ArrayList;

public class QueueViewer {
    private static RabbitMqOperator operator;
    private static RabbitMqReceiver receiver;

    public static void main(String[] argv) throws IOException {
        operator = new RabbitMqOperator();
        receiver = new RabbitMqReceiver();

        char choice = ' ';
        while (choice != '9') {
            System.out.println("---------------------------");
            System.out.println("Welcome to RabbitMQ queue viewer!");

            System.out.println("[1] List queues");
            System.out.println("[2] Print queue content");
            System.out.println("[3] Enable displaying received messages");
            System.out.println("[9] Exit");
            System.out.println("Select an option... ");

            try {
                receiveMessages();
            } catch (Exception exc) {
                exc.printStackTrace();
            }

            choice = (char) System.in.read();
            System.in.skip(System.in.available()); // to skip spare characters


            switch (choice) {
                case '1':
                    System.out.println("List queues");
                    printQueues();
                    break;
                case '2':
                    System.out.println("Print queue content");
                    break;
                case '9':
                    System.out.println("Exiting");
                    break;
                default:
                    System.out.println("Wrong option!");
                    break;
            }
        }
    }

    public static void printQueues() {
        ArrayList<String> queues = operator.getQueues();

        System.out.println("---------------------------");
        if (queues.size() > 0) {
            System.out.println("Please find the list of Queues below:");
            System.out.println("Name");
            for (int i = 0; i < queues.size(); i++) {
                String name = queues.get(i);
                System.out.println(String.format("[%d] %s", i, name));
            }
        } else {
            System.out.println("There are no queues");
        }
    }

    private static void receiveMessages() throws Exception {
        receiver.receiveMessages();
    }
}
