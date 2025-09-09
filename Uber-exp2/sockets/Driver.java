import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Driver {
    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("localhost", 5000);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        Scanner sc = new Scanner(System.in);

        System.out.print("Enter Driver Name: ");
        String driverName = sc.nextLine();

        System.out.println("Enter GPS coordinates (lat,long). Type 'exit' to quit:");
        while (true) {
            String gps = sc.nextLine();
            if (gps.equalsIgnoreCase("exit")) break;

            out.println(driverName + " -> " + gps);
        }

        socket.close();
        sc.close();
    }
}
