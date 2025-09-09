import java.rmi.*;
import java.util.Scanner;

public class RideClient {
    public static void main(String[] args) {
        try {
            RideService service = (RideService) Naming.lookup("rmi://localhost/RideService");
            Scanner sc = new Scanner(System.in);

            while (true) {
                System.out.print("\nEnter rider name (or type 'exit' to quit): ");
                String rider = sc.nextLine();
                if (rider.equalsIgnoreCase("exit")) {
                    System.out.println("Exiting Ride Client...");
                    break;
                }

                System.out.print("Enter distance (km): ");
                double distance = sc.nextDouble();

                System.out.print("Enter rate (per km): ");
                double rate = sc.nextDouble();
                sc.nextLine();

                String details = service.getFullTripDetails(rider, distance, rate);
                System.out.println("Location " + details);
            }

            sc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
