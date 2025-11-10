import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Client {

    private static final String LOAD_BALANCER_HOST = "localhost";
    private static final int LOAD_BALANCER_PORT = 8080;

    public static void main(String[] args) {
        System.out.println("Uber Client Started...");

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("Enter your ride request (or 'exit' to quit): ");
            String userInput = scanner.nextLine();

            if ("exit".equalsIgnoreCase(userInput)) {
                break;
            }
            
            if (userInput.trim().isEmpty()) {
                continue;
            }

            try (Socket socket = new Socket(LOAD_BALANCER_HOST, LOAD_BALANCER_PORT);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                
                System.out.println("Sending: " + userInput);
                out.println(userInput);

            } catch (IOException e) {
                System.err.println("Could not connect or an I/O error occurred: " + e.getMessage());
            }
        }
        
        scanner.close();
        System.out.println("Client shutting down.");
    }
}