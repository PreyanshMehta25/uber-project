import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Server {

    private static final int PORT = 12345;
    private static final int SERVER_COUNT = 3;

    public static void main(String[] args) {
        System.out.println("Uber Server Cluster");

        ServerClusterManager clusterManager = new ServerClusterManager(SERVER_COUNT);
        clusterManager.startCluster();

        System.out.println("Server cluster is running. Waiting for client connections on port " + PORT + "...");

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("New client connected: " + clientSocket.getInetAddress());
                    new ClientHandler(clientSocket, clusterManager).start();
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Could not start server on port " + PORT + ": " + e.getMessage());
        }
    }

    static class ClientHandler extends Thread {
        private final Socket clientSocket;
        private final ServerClusterManager clusterManager;

        public ClientHandler(Socket socket, ServerClusterManager manager) {
            this.clientSocket = socket;
            this.clusterManager = manager;
        }

        @Override
        public void run() {
            try (ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream())) {

                Object input;
                while ((input = in.readObject()) != null) {
                    if (input instanceof String) {
                        String command = (String) input;
                        System.out.println("Received command from client: " + command);

                        String[] parts = command.split(";");

                        switch (parts[0]) {
                            case "REQUEST_RIDE":
                                if (parts.length == 4) {
                                    models.User user = new models.User(parts[1]);
                                    models.Ride newRide = new models.Ride(user, parts[2], parts[3]);
                                    clusterManager.requestRide(newRide);
                                    out.writeObject(newRide);
                                }
                                break;

                            case "GET_AVAILABLE_RIDES":
                                List<models.Ride> rides = clusterManager.getAvailableRides();
                                out.writeObject(rides);
                                break;

                            case "ACCEPT_RIDE":
                                try {
                                    Object driverObj = in.readObject();
                                    Object rideToAcceptObj = in.readObject();
                                    boolean success = clusterManager.acceptRide(driverObj, rideToAcceptObj);
                                    
                                    out.writeObject(success);

                                } catch (Exception e) {
                                    System.err.println("Error processing ACCEPT_RIDE command: " + e.getMessage());
                                    e.printStackTrace();
                                    out.writeObject(false);
                                }
                                break;

                            case "GET_SERVER_STATUS":
                                String status = clusterManager.getClusterStatus();
                                out.writeObject(status);
                                break;

                            case "SIMULATE_FAILURE":
                                if (parts.length == 2) {
                                    int serverId = Integer.parseInt(parts[1]);
                                    clusterManager.simulateServerFailure(serverId);
                                    out.writeObject("Server " + serverId + " shut down.");
                                }
                                break;
                        }
                        out.flush();
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("Client disconnected: " + clientSocket.getInetAddress());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                }
            }
        }
    }

    static class ServerClusterManager {
        private final List<RideBookingServer> servers;
        private int leaderId = 0;
        private int lastUsedServer = -1;

        public ServerClusterManager(int numberOfServers) {
            servers = new ArrayList<>();
            for (int i = 0; i < numberOfServers; i++) {
                servers.add(new RideBookingServer(i, this));
            }
        }

        public void startCluster() {
            servers.forEach(RideBookingServer::start);
            electNewLeader();
        }

        private synchronized RideBookingServer getLeader() {
            return servers.stream().filter(s -> s.serverId == leaderId && s.isRunning()).findFirst().orElse(null);
        }

        private synchronized RideBookingServer getNextServerForRequest() {
            lastUsedServer = (lastUsedServer + 1) % servers.size();
            int serversChecked = 0;
            while (!servers.get(lastUsedServer).isRunning()) {
                lastUsedServer = (lastUsedServer + 1) % servers.size();
                serversChecked++;
                if (serversChecked > servers.size()) return null;
            }
            return servers.get(lastUsedServer);
        }

        public synchronized void electNewLeader() {
            leaderId = servers.stream()
                .filter(RideBookingServer::isRunning)
                .mapToInt(s -> s.serverId)
                .findFirst()
                .orElse(-1);

            if (leaderId != -1) {
                System.out.println("\nLeader Election:");
                System.out.println("New Leader is Server " + leaderId);
            } else {
                System.err.println("CRITICAL: No available servers to elect as leader!");
            }
        }

        public synchronized void replicateData(List<models.Ride> rides) {
            String serializedRides = rides.stream().map(Object::toString).collect(Collectors.joining(", "));
            System.out.println("LEADER (Server " + leaderId + "): Replicating data to all followers. Data: [" + serializedRides + "]");
            for (RideBookingServer server : servers) {
                if (server.serverId != leaderId && server.isRunning()) {
                    server.updateRides(rides);
                }
            }
        }

        public void requestRide(models.Ride ride) {
            RideBookingServer leader = getLeader();
            if (leader != null) {
                leader.addRide(ride);
            } else {
                System.err.println("No leader available to handle ride request.");
            }
        }

        public boolean acceptRide(Object driverObj, Object rideToAcceptObj) {
            RideBookingServer leader = getLeader();
            if (leader != null) {
                models.Driver driver = getDriverFromObject(driverObj);
                String rideId = getRideIdFromObject(rideToAcceptObj);
                
                if (driver != null && rideId != null) {
                     return leader.assignDriverToRide(driver, rideId);
                }
                return false;
            } else {
                System.err.println("No leader available to handle ride acceptance.");
                return false;
            }
        }

        public List<models.Ride> getAvailableRides() {
            RideBookingServer server = getNextServerForRequest();
            if (server != null) {
                System.out.println("Routing GET_AVAILABLE_RIDES to Server " + server.serverId);
                return server.getAvailableRides();
            }
            return Collections.emptyList();
        }

        public synchronized String getClusterStatus() {
            StringBuilder status = new StringBuilder();
            status.append("\nCluster Status:\n");
            for (RideBookingServer server : servers) {
                status.append(String.format("Server %d: %s %s\n",
                    server.serverId,
                    server.isRunning() ? "ONLINE" : "OFFLINE",
                    server.serverId == leaderId ? "(LEADER)" : ""
                ));
            }
            return status.toString();
        }

        public synchronized void simulateServerFailure(int serverId) {
            if (serverId < 0 || serverId >= servers.size()) return;
            RideBookingServer serverToStop = servers.get(serverId);
            if(serverToStop.isRunning()) {
                serverToStop.shutdown();
                System.out.println("\nSIMULATING FAILURE: Server " + serverId + " has shut down.\n");
                if (serverId == leaderId) {
                    System.out.println("Leader has failed. Triggering new election...");
                    electNewLeader();
                }
            }
        }
        
        private String getRideIdFromObject(Object rideObj) {
            try {
                return (String) rideObj.getClass().getMethod("getRideId").invoke(rideObj);
            } catch (Exception e) {
                System.err.println("Could not reflectively get rideId: " + e.getMessage());
                return null;
            }
        }

        private models.Driver getDriverFromObject(Object driverObj) {
            try {
                String driverName = (String) driverObj.getClass().getMethod("getName").invoke(driverObj);
                return new models.Driver(driverName);
            } catch (Exception e) {
                System.err.println("Could not reflectively get driver name: " + e.getMessage());
                return null;
            }
        }
    }

    static class RideBookingServer extends Thread {
        private final int serverId;
        private final ServerClusterManager clusterManager;
        private final List<models.Ride> rides = new CopyOnWriteArrayList<>();
        private final ReentrantLock rideLock = new ReentrantLock();
        private volatile boolean running = true;

        public RideBookingServer(int id, ServerClusterManager manager) {
            this.serverId = id;
            this.clusterManager = manager;
        }

        public boolean isRunning() {
            return this.running;
        }

        public void addRide(models.Ride ride) {
            rides.add(ride);
            System.out.println("Server " + serverId + " (Leader): New ride added " + ride.getRideId().substring(0, 8) + ". Replicating...");
            clusterManager.replicateData(new ArrayList<>(this.rides));
        }
        
        public boolean assignDriverToRide(models.Driver driver, String rideId) {
            rideLock.lock();
            try {
                Optional<models.Ride> rideOpt = rides.stream()
                    .filter(r -> r.getRideId().equals(rideId) && r.getStatus() == models.Ride.RideStatus.REQUESTED)
                    .findFirst();

                if (rideOpt.isPresent()) {
                    models.Ride ride = rideOpt.get();
                    ride.setAssignedDriver(driver);
                    ride.setStatus(models.Ride.RideStatus.ACCEPTED);
                    System.out.println("Server " + serverId + " (Leader): Ride " + ride.getRideId().substring(0,8) + " assigned to " + driver.getName() + ". Replicating...");
                    clusterManager.replicateData(new ArrayList<>(this.rides));
                    return true;
                }
                return false; 
            } finally {
                rideLock.unlock();
            }
        }

        public List<models.Ride> getAvailableRides() {
            return rides.stream()
                .filter(ride -> ride.getStatus() == models.Ride.RideStatus.REQUESTED)
                .collect(Collectors.toList());
        }

        public void updateRides(List<models.Ride> newRides) {
            this.rides.clear();
            this.rides.addAll(newRides);
            System.out.println("Server " + serverId + " (Follower): Data replicated successfully.");
        }

        public void shutdown() {
            this.running = false;
        }

        @Override
        public void run() {
            System.out.println("Server " + serverId + " started.");
            while (running) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    this.running = false;
                }
            }
            System.out.println("Server " + serverId + " has stopped.");
        }
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
            public Driver(String name) { super(name); }

            @Override
            public String toString() {
                return String.format("Driver[ID=%s, Name=%s]", getId().substring(0, 8), getName());
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
            public RideStatus getStatus() { return status; }
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