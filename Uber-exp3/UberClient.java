import java.io.*;
import java.net.*;
import java.util.*;

public class UberClient {
    public static void main(String[] args) {
        try (Socket socket = new Socket("localhost", 12345);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             Scanner sc = new Scanner(System.in)) {

            while (true) {
                System.out.println("\nUBER CLIENT MENU");
                System.out.println("1. Request Ride");
                System.out.println("2. Update Driver Location");
                System.out.println("3. Process Payment");
                System.out.println("4. Exit");
                System.out.println("Choose option: ");

                int choice = Integer.parseInt(sc.nextLine());
                String request = "";

                switch (choice) {
                    case 1 -> {
                        System.out.print("Enter Passenger ID: ");
                        String pid = sc.nextLine();
                        System.out.print("Enter Driver ID: ");
                        String did = sc.nextLine();
                        request = "ride;" + pid + ";" + did;
                    }
                    case 2 -> {
                        System.out.print("Enter Driver ID: ");
                        String did = sc.nextLine();
                        System.out.print("Enter latitude: ");
                        double lat = Double.parseDouble(sc.nextLine());
                        System.out.print("Enter longitude: ");
                        double lng = Double.parseDouble(sc.nextLine());
                        request = "location;" + did + ";" + lat + ";" + lng;
                    }
                    case 3 -> {
                        System.out.print("Enter Ride ID: ");
                        String rid = sc.nextLine();
                        System.out.print("Enter Payment Method (UPI/Card/Wallet): ");
                        String method = sc.nextLine();
                        request = "payment;" + rid + ";" + method;
                    }
                    case 4 -> {
                        out.println("exit");
                        System.out.println("Exiting client.");
                        return;
                    }
                    default -> {
                        System.out.println("Invalid choice.");
                        continue;
                    }
                }

                out.println(request);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
