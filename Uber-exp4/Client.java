import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Client {
    private static final LamportClock clock = new LamportClock();

    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("localhost", 5000);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        Scanner sc = new Scanner(System.in);
        System.out.println("Connected. Enter commands: REQUEST <ride>, ACCEPT <ride>, PAY <ride>, STATUS");

        while (true) {
            String cmd = sc.nextLine();
            String[] parts = cmd.split(" ");
            int ts = clock.tick();
            if (parts[0].equalsIgnoreCase("STATUS")) {
                out.println("STATUS " + ts);
            } else if (parts.length >= 2) {
                out.println(parts[0].toUpperCase() + " " + parts[1] + " " + ts);
            } else {
                System.out.println("Invalid input");
                continue;
            }
            String response = in.readLine();
            System.out.println(response);
        }
    }
}
