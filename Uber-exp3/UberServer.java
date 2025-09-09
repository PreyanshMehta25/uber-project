import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class UberServer {
    private final ExecutorService pool = Executors.newFixedThreadPool(3);
    private final List<String> centralDatabase = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean running = true;
    private ServerSocket serverSocket;

    public void startServer(int port) {
        try (ServerSocket ss = new ServerSocket(port)) {
            serverSocket = ss;
            System.out.println("Uber Server started on port " + port);

            Thread consoleThread = new Thread(this::listenForConsoleCommands);
            consoleThread.start();

            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    pool.submit(() -> handleClient(clientSocket));
                } catch (SocketException e) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void listenForConsoleCommands() {
        Scanner scanner = new Scanner(System.in);
        while (running) {
            String command = scanner.nextLine();
            if (command.equalsIgnoreCase("quit")) {
                System.out.println("Quit command received. Shutting down server...");
                shutdown();
                break;
            }
        }
    }

    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            String request;
            while ((request = in.readLine()) != null) {
                if (request.equalsIgnoreCase("exit")) {
                    System.out.println("Client disconnected.");
                    break;
                }
                String[] parts = request.split(";");
                String action = parts[0];
                switch (action) {
                    case "ride" -> handleRideRequest(parts[1], parts[2]);
                    case "location" -> updateLocation(parts[1], Double.parseDouble(parts[2]), Double.parseDouble(parts[3]));
                    case "payment" -> processPayment(parts[1], parts[2]);
                    default -> System.out.println("Unknown request: " + request);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleRideRequest(String passengerId, String driverId) {
        pool.submit(() -> {
            String msg = "Matching passenger " + passengerId + " with driver " + driverId + "...";
            System.out.println(msg);
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            msg = "Ride confirmed: Passenger " + passengerId + " | Driver " + driverId;
            System.out.println(msg);
            System.out.println("API Call | RideService.assignRide");
            String record = "Ride record: Passenger " + passengerId + " assigned to Driver " + driverId;
            centralDatabase.add(record);
            System.out.println("Database updated with: " + record);
        });
    }

    private void updateLocation(String driverId, double lat, double lng) {
        pool.submit(() -> {
            String msg = "Driver " + driverId + " updated location | (" + lat + ", " + lng + ")";
            System.out.println(msg);
            System.out.println("API Call | LocationService.updateLocation");
            String record = "Location record: Driver " + driverId + " at (" + lat + "," + lng + ")";
            centralDatabase.add(record);
            System.out.println("Database updated with: " + record);
        });
    }

    private void processPayment(String rideId, String method) {
        pool.submit(() -> {
            String msg = "Processing " + method + " payment for ride " + rideId;
            System.out.println(msg);
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            msg = "Payment successful for ride " + rideId;
            System.out.println(msg);
            System.out.println("API Call | PaymentService.processPayment");
            String record = "Payment record: Ride " + rideId + " paid via " + method;
            centralDatabase.add(record);
            System.out.println("Database updated with: " + record);
        });
    }

    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        pool.shutdown();
        try {
            pool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {}
        System.out.println("\nCENTRAL DATABASE SUMMARY");
        centralDatabase.forEach(System.out::println);
        System.out.println("END");
    }

    public static void main(String[] args) {
        UberServer server = new UberServer();
        server.startServer(12345);
    }
}
