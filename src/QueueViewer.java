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
            if (receiver.isVerboseMode()) {
                System.out.println("[3] Disable displaying received messages");
            } else {
                System.out.println("[3] Enable displaying received messages");
            }
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
                    System.out.println("Print queues content");
                    printQueuesContent();
                    break;
                case '3':
                    if (receiver.isVerboseMode()) {
                        System.out.println("Disable displaying received messages");
                        receiver.setVerboseMode(false);
                    } else {
                        System.out.println("Enable displaying received messages");
                        receiver.setVerboseMode(true);
                    }
                    break;
                case '9':
                    System.out.println("Exiting");
                    operator.close();
                    receiver.close();
                    break;
                default:
                    System.out.println("Wrong option!");
                    break;
            }
        }
    }

    private static void printQueues() {
        ArrayList<String> queues = operator.getQueues();

        System.out.println("---------------------------");
        if (queues.size() > 0) {
            System.out.println("Please find the list of Queues below:");
            System.out.println("[num] Name");
            int i = 1;
            for (String queue : queues) {
                System.out.println(String.format("[%d] %s", i++, queue));
            }
        } else {
            System.out.println("There are no queues");
        }
    }

    private static void printQueuesContent() {

        for (String queue : operator.getQueues()) {

            ArrayList<String[]> messages = operator.getQueueContent(queue);

            if (messages.size() > 0) {
                System.out.println("----- " + queue + " -----");
                int i = 1;
                for (String[] message : messages) {
                    System.out.println(String.format("[%d] %s : %s : %s", i++, message[0], message[1], message[2]));
                }
            }
        }
    }

    private static void receiveMessages() throws Exception {
        receiver.receiveMessages();
    }
}
