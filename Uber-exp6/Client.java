import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class Client {

    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 12345;

    public static void main(String[] args) {
        System.out.println("Uber Client");

        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("Connected to the Uber server.");

            List<models.Driver> drivers = setupDrivers(scanner);

            System.out.println("\nSetup complete. " + drivers.size() + " drivers are online and ready.");


            while (true) {
                System.out.println("\nMain Menu");
                System.out.println("1. Act as a User (Request a Ride)");
                System.out.println("2. Act as a Driver (View & Accept Rides)");
                System.out.println("3. System Administration");
                System.out.println("4. Exit");
                System.out.print("Enter your choice: ");

                String choice = scanner.nextLine();

                try {
                    switch (choice) {
                        case "1":
                            requestRide(scanner, out, in);
                            break;
                        case "2":
                            if (drivers.isEmpty()) {
                                System.out.println("No drivers are set up.");
                                break;
                            }
                            System.out.println("\nSelect a driver to act as:");
                            for (int i = 0; i < drivers.size(); i++) {
                                System.out.printf("%d: %s\n", i + 1, drivers.get(i).getName());
                            }
                            System.out.print("Enter your choice: ");
                            int driverChoice;
                            try {
                                driverChoice = Integer.parseInt(scanner.nextLine());
                                if (driverChoice > 0 && driverChoice <= drivers.size()) {
                                    models.Driver selectedDriver = drivers.get(driverChoice - 1);
                                    acceptRide(scanner, out, in, selectedDriver);
                                } else {
                                    System.out.println("Invalid driver selection.");
                                }
                            } catch (NumberFormatException e) {
                                System.out.println("Invalid input. Please enter a number.");
                            }
                            break;
                        case "3":
                            adminActions(scanner, out, in);
                            break;
                        case "4":
                            System.out.println("Exiting client. Goodbye!");
                            return;
                        default:
                            System.out.println("Invalid choice. Please try again.");
                    }
                } catch (Exception e) {
                    System.err.println("An error occurred: " + e.getMessage());
                    e.printStackTrace();
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Could not connect to the server: " + e.getMessage());
        }
    }

    private static List<models.Driver> setupDrivers(Scanner scanner) {
        List<models.Driver> drivers = new ArrayList<>();
        int driverCount = 0;

        while (true) {
            try {
                System.out.print("\nEnter the number of drivers for this session: ");
                driverCount = Integer.parseInt(scanner.nextLine());
                if (driverCount > 0) {
                    break;
                }
                System.out.println("Please enter a positive number for the driver count.");
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a number.");
            }
        }

        System.out.println("Please enter the name for each driver.");
        for (int i = 0; i < driverCount; i++) {
            String driverName;
            while (true) {
                System.out.print("Enter name for Driver " + (i + 1) + ": ");
                driverName = scanner.nextLine();
                if (!driverName.trim().isEmpty()) {
                    break;
                }
                System.out.println("Driver name cannot be empty.");
            }
            drivers.add(new models.Driver(driverName));
        }
        return drivers;
    }

    private static void adminActions(Scanner scanner, ObjectOutputStream out, ObjectInputStream in) throws IOException, ClassNotFoundException {
         while(true) {
            System.out.println("\nSystem Administration");
            System.out.println("1. View Status of All Servers");
            System.out.println("2. Simulate a Server Failure");
            System.out.println("3. Back to Main Menu");
            System.out.print("Enter your choice: ");
            String choice = scanner.nextLine();

            switch(choice) {
                case "1":
                    viewServerStatus(out, in);
                    break;
                case "2":
                    simulateFailure(scanner, out, in);
                    break;
                case "3":
                    return;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
         }
    }

    private static void requestRide(Scanner scanner, ObjectOutputStream out, ObjectInputStream in) throws IOException, ClassNotFoundException {
        System.out.print("\nEnter your name: ");
        String name = scanner.nextLine();
        System.out.print("Enter pickup location: ");
        String pickup = scanner.nextLine();
        System.out.print("Enter destination: ");
        String dest = scanner.nextLine();

        String command = String.format("REQUEST_RIDE;%s;%s;%s", name, pickup, dest);
        out.writeObject(command);
        out.flush();

        Object response = in.readObject();
        if (response != null && response.getClass().getName().endsWith("models$Ride")) {
            System.out.println("Ride requested successfully! Details: " + response);
        } else {
            System.err.println("Server returned an error: " + response);
        }
    }

    private static void acceptRide(Scanner scanner, ObjectOutputStream out, ObjectInputStream in, models.Driver driver) throws IOException, ClassNotFoundException {
        out.writeObject("GET_AVAILABLE_RIDES");
        out.flush();

        Object response = in.readObject();
        if (!(response instanceof List<?>)) {
            System.err.println("Unexpected response from server: " + response);
            return;
        }

        List<?> availableRides = (List<?>) response;

        if (availableRides.isEmpty()) {
            System.out.println("\nNo available rides at the moment for " + driver.getName() + ".");
            return;
        }

        System.out.println("\nAvailable rides for " + driver.getName() + ":");
        for (int i = 0; i < availableRides.size(); i++) {
            System.out.printf("%d: %s\n", i + 1, availableRides.get(i));
        }

        System.out.print("Enter the number of the ride to accept (0 to cancel): ");
        int rideChoice;
        try {
            rideChoice = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException e) {
            System.out.println("Invalid input.");
            return;
        }

        if (rideChoice > 0 && rideChoice <= availableRides.size()) {
            Object selectedRide = availableRides.get(rideChoice - 1);

            out.writeObject("ACCEPT_RIDE");
            out.writeObject(driver);
            out.writeObject(selectedRide);
            out.flush();

            boolean success = (boolean) in.readObject();
            if (success) {
                System.out.println("Ride accepted successfully!");
            } else {
                System.out.println("Failed to accept the ride. It may have been taken by another driver.");
            }
        }
    }

    private static void viewServerStatus(ObjectOutputStream out, ObjectInputStream in) throws IOException, ClassNotFoundException {
        out.writeObject("GET_SERVER_STATUS");
        out.flush();
        String statusReport = (String) in.readObject();
        System.out.print(statusReport);
    }

    private static void simulateFailure(Scanner scanner, ObjectOutputStream out, ObjectInputStream in) throws IOException, ClassNotFoundException {
        System.out.print("Enter the server ID to shut down: ");
        int serverId;
        try {
            serverId = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException e) {
            System.out.println("Invalid input.");
            return;
        }
        out.writeObject("SIMULATE_FAILURE;" + serverId);
        out.flush();
        String response = (String) in.readObject();
        System.out.println("Server response: " + response);
    }

    public static class models {
        public static abstract class Person implements Serializable {
            private static final long serialVersionUID = 1L;
            private final String id;
            private final String name;

            public Person(String name) {
                this.id = UUID.randomUUID().toString();
                this.name = name;
            }
            public String getId() { return id; }
            public String getName() { return name; }
        }

        public static class User extends Person {
            private static final long serialVersionUID = 1L;
            public User(String name) { super(name); }
        }

        public static class Driver extends Person {
            private static final long serialVersionUID = 1L;
            private boolean isAvailable;

            public Driver(String name) {
                super(name);
                this.isAvailable = true;
            }
            public boolean isAvailable() { return isAvailable; }
            public void setAvailable(boolean available) { isAvailable = available; }

            @Override
            public String toString() {
                return String.format("Driver[ID=%s, Name=%s, Available=%s]", getId().substring(0, 8), getName(), isAvailable);
            }
        }

        public static class Ride implements Serializable {
            private static final long serialVersionUID = 1L;
            private final String rideId;
            private final User user;
            private final String pickupLocation;
            private final String destination;
            private RideStatus status;
            private Driver assignedDriver;

            public enum RideStatus { REQUESTED, ACCEPTED, IN_PROGRESS, COMPLETED, CANCELLED }

            public Ride(User user, String pickupLocation, String destination) {
                this.rideId = UUID.randomUUID().toString();
                this.user = user;
                this.pickupLocation = pickupLocation;
                this.destination = destination;
                this.status = RideStatus.REQUESTED;
            }

            public String getRideId() { return rideId; }
            public User getUser() { return user; }
            public String getPickupLocation() { return pickupLocation; }
            public String getDestination() { return destination; }
            public RideStatus getStatus() { return status; }
            public Driver getAssignedDriver() { return assignedDriver; }
            public void setStatus(RideStatus status) { this.status = status; }
            public void setAssignedDriver(Driver driver) { this.assignedDriver = driver; }

            @Override
            public String toString() {
                String driverInfo = (assignedDriver != null) ? ", Driver=" + assignedDriver.getName() : "";
                return String.format("Ride[ID=%s, User=%s, From=%s, To=%s, Status=%s%s]",
                        rideId.substring(0, 8), user.getName(), pickupLocation, destination, status, driverInfo);
            }
        }
    }
}