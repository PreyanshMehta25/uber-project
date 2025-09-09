import java.io.*;
import java.net.*;
import java.util.*;

interface UberRPC {
    void registerDriver(String driver);
    String requestRide(String rider);
    String assignDriver(String rider);
}

class UberRPCImpl implements UberRPC {
    List<String> drivers = new ArrayList<>();
    Map<String, String> rides = new HashMap<>();

    public void registerDriver(String driver) {
        drivers.add(driver);
        System.out.println("Driver registered: " + driver);
    }

    public String requestRide(String rider) {
        System.out.println(rider + " requested a ride");
        return "Ride request received";
    }

    public String assignDriver(String rider) {
        if (drivers.isEmpty()) return "No drivers available";
        String driver = drivers.remove(0);
        rides.put(rider, driver);

        try (Socket dispatcherSocket = new Socket("localhost", 7000)) {
            PrintWriter out = new PrintWriter(dispatcherSocket.getOutputStream(), true);
            out.println("Booking: Rider=" + rider + " -> Driver=" + driver);
        } catch (Exception e) {
            System.out.println("Dispatcher not available for booking update.");
        }

        return "Driver " + driver + " assigned to " + rider;
    }

    public Map<String, String> getAllBookings() {
        return rides;
    }
}

public class rpc {
    public static void main(String[] args) {
        UberRPCImpl service = new UberRPCImpl();
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.print("Enter 1 to register driver, 2 to request ride, 3 to exit: ");
            int choice = sc.nextInt(); sc.nextLine();

            if (choice == 1) {
                System.out.print("Enter driver name: ");
                service.registerDriver(sc.nextLine());
            } else if (choice == 2) {
                System.out.print("Enter rider name: ");
                String rider = sc.nextLine();
                System.out.println(service.requestRide(rider));
                System.out.println(service.assignDriver(rider));
            } else {
                break;
            }
        }
        sc.close();
    }
}
