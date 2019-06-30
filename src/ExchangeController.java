import java.io.IOException;
import java.util.ArrayList;

public class ExchangeController {
    private static RabbitMqOperator operator;

    public static void main(String[] argv) throws java.io.IOException {
         operator = new RabbitMqOperator();

        char choice = ' ';
        while (choice != '9') {
            System.out.println("---------------------------");
            System.out.println("Welcome to RabbitMQ control panel!");

            System.out.println("[1] List exchanges");
            System.out.println("[2] Post message");
            System.out.println("[9] Exit");
            System.out.println("Select an option...");

            choice = (char) System.in.read();
            System.in.skip(System.in.available()); // to skip spare characters

            switch (choice) {
                case '1':
                    System.out.println("List exchanges");
                    printExchanges();
                    break;
                case '2':
                    System.out.println("Post message");
                    postMessage();
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

    public static void printExchanges() {
        ArrayList<String[]> exchanges = operator.getExchanges();

        System.out.println("---------------------------");
        if (exchanges.size() > 0) {
            System.out.println("Please find the list of Exchanges below:");
            System.out.println("Name - Type");
            for (int i = 0; i < exchanges.size(); i++) {
                String name = exchanges.get(i)[0];
                String type = exchanges.get(i)[1];
                System.out.println(String.format("[%d] %s - %s", i, name, type));
            }
        } else {
            System.out.println("There are no exchanges");
        }
    }

    private static void postMessage() {
        printExchanges();
        System.out.println("Choose to which exchange you want to post a message");


        ArrayList<String[]> exchanges = operator.getExchanges();
        int choice = 0;
        String name = exchanges.get(choice)[0];
        String type = exchanges.get(choice)[1];
        try {
            operator.publishMessage(name, type,"my_own_severity", "Test message");

        } catch (IOException exc) {
            exc.printStackTrace();
        }



        choice = 3;
        name = exchanges.get(choice)[0];
        type = exchanges.get(choice)[1];
        try {
            operator.publishMessage(name, type,"info", "Test info message");

        } catch (IOException exc) {
            exc.printStackTrace();
        }

    }
}
