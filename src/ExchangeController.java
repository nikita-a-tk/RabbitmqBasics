import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
                    operator.close();
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
            System.out.println("[num] Name - Type");
            int i = 1;
            for (String[] exchange : exchanges) {
                System.out.println(String.format("[%d] %s - %s", i++, exchange[0], exchange[1]));
            }
        } else {
            System.out.println("There are no exchanges");
        }
    }

    private static void postMessage() throws IOException {

        int choice = -1;

        ArrayList<String[]> exchanges = operator.getExchanges();

        while (choice != 8) {
            printExchanges();
            System.out.println("[9] To Main Menu");
            System.out.println("Choose to which exchange you want to post a message");

            choice = System.in.read() - '1'; // choice begins from 1
            System.in.skip(System.in.available()); // to skip spare characters

            if (choice >= 0 && choice < exchanges.size()) {
                String[] exchange = exchanges.get(choice);
                System.out.println(Arrays.toString(exchange));

                operator.sendMessage(exchange[0], exchange[1]);
            } else if (choice != 8) {
                System.out.println("Please choose a valid exchange");
            }
        }
        System.out.println("To Main Menu");
    }
}
