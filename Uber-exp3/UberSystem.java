import java.util.*;
import java.util.concurrent.*;

public class UberSystem {
    private final ExecutorService pool = Executors.newFixedThreadPool(10);

    private final List<String> centralDatabase = Collections.synchronizedList(new ArrayList<>());

    public void handleRideRequest(String passengerId) {
        pool.submit(() -> {
            String msg = "Matching passenger " + passengerId + " with nearest driver...";
            System.out.println(msg);

            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            msg = "Ride confirmed for " + passengerId;
            System.out.println(msg);

            centralDatabase.add("Ride record: Passenger " + passengerId + " assigned to DriverX");
        });
    }

    public void updateLocation(String driverId, double lat, double lng) {
        pool.submit(() -> {
            String msg = "Driver " + driverId + " updated location → (" + lat + ", " + lng + ")";
            System.out.println(msg);

            centralDatabase.add("Location record: Driver " + driverId + " at (" + lat + "," + lng + ")");
        });
    }

    public void processPayment(String rideId, String method) {
        pool.submit(() -> {
            String msg = "Processing " + method + " payment for ride " + rideId;
            System.out.println(msg);

            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            msg = "Payment successful for ride " + rideId;
            System.out.println(msg);

            centralDatabase.add("Payment record: Ride " + rideId + " paid via " + method);
        });
    }

    public void startMenu() {
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.println("\nUBER SYSTEM MENU");
            System.out.println("1. Request Ride");
            System.out.println("2. Update Driver Location");
            System.out.println("3. Process Payment");
            System.out.println("4. Exit");
            System.out.println("Choose option: ");

            int choice = -1;
            try {
                choice = Integer.parseInt(sc.nextLine());
            } catch (Exception e) {
                System.out.println("Invalid input, try again.");
                continue;
            }

            switch (choice) {
                case 1 -> {
                    System.out.print("Enter Passenger ID: ");
                    String pid = sc.nextLine();
                    handleRideRequest(pid);
                }
                case 2 -> {
                    System.out.print("Enter Driver ID: ");
                    String did = sc.nextLine();
                    System.out.print("Enter latitude: ");
                    double lat = Double.parseDouble(sc.nextLine());
                    System.out.print("Enter longitude: ");
                    double lng = Double.parseDouble(sc.nextLine());
                    updateLocation(did, lat, lng);
                }
                case 3 -> {
                    System.out.print("Enter Ride ID: ");
                    String rid = sc.nextLine();
                    System.out.print("Enter Payment Method (UPI/Card/Wallet): ");
                    String method = sc.nextLine();
                    processPayment(rid, method);
                }
                case 4 -> {
                    shutdown();
                    return;
                }
                default -> System.out.println("Invalid choice, try again.");
            }
        }
    }

    private void shutdown() {
        System.out.println("\nShutting down... waiting for tasks to finish.");
        pool.shutdown();
        try {
            pool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {}

        System.out.println("\nCENTRAL DATABASE CONTENT");
        centralDatabase.forEach(System.out::println);
        System.out.println("END");
    }

    public static void main(String[] args) {
        new UberSystem().startMenu();
    }
}
