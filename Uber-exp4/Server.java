import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;

public class Server {
    private static final LamportClock clock = new LamportClock();
    private static final LamportLock lock = new LamportLock();
    private static final ConcurrentHashMap<String, String> rides = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(5000);
        System.out.println("Server started on port 5000...");
        while (true) {
            Socket socket = serverSocket.accept();
            System.out.println("New client connected: " + socket.getInetAddress());
            new Thread(() -> handleClient(socket)).start();
        }
    }

    private static void handleClient(Socket socket) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String line;
            while ((line = in.readLine()) != null) {
                String[] parts = line.split(" ");
                String cmd = parts[0];
                int received = Integer.parseInt(parts[parts.length - 1]);
                int ts = clock.update(received);

                if (cmd.equals("REQUEST")) {
                    String ride = parts[1];
                    if (rides.containsKey(ride)) {
                        out.println("ERROR Ride already exists " + ts);
                        System.out.println("[" + ts + "] REQUEST " + ride + " -> ERROR (already exists)");
                    } else {
                        rides.put(ride, "PENDING");
                        out.println("ACK " + ts);
                        System.out.println("[" + ts + "] REQUEST " + ride + " -> PENDING");
                    }
                } else if (cmd.equals("ACCEPT")) {
                    String ride = parts[1];
                    if (!rides.containsKey(ride)) {
                        out.println("ERROR Ride not found " + ts);
                        System.out.println("[" + ts + "] ACCEPT " + ride + " -> ERROR (not found)");
                    } else if (!rides.get(ride).equals("PENDING")) {
                        out.println("ERROR Ride not pending " + ts);
                        System.out.println("[" + ts + "] ACCEPT " + ride + " -> ERROR (not pending)");
                    } else if (lock.acquire(ride, ts)) {
                        rides.put(ride, "ACCEPTED");
                        out.println("LOCKED " + ts);
                        System.out.println("[" + ts + "] ACCEPT " + ride + " -> ACCEPTED");
                    } else {
                        out.println("FAILED " + ts);
                        System.out.println("[" + ts + "] ACCEPT " + ride + " -> FAILED (lock held)");
                    }
                } else if (cmd.equals("PAY")) {
                    String ride = parts[1];
                    if (!rides.containsKey(ride)) {
                        out.println("ERROR Ride not found " + ts);
                        System.out.println("[" + ts + "] PAY " + ride + " -> ERROR (not found)");
                    } else if (!rides.get(ride).equals("ACCEPTED")) {
                        out.println("ERROR Ride not accepted " + ts);
                        System.out.println("[" + ts + "] PAY " + ride + " -> ERROR (not accepted)");
                    } else {
                        rides.put(ride, "PAID");
                        lock.release(ride);
                        out.println("DONE " + ts);
                        System.out.println("[" + ts + "] PAY " + ride + " -> PAID");
                    }
                } else if (cmd.equals("STATUS")) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("RIDES: ");
                    rides.forEach((r, state) -> sb.append(r).append("=").append(state).append(" "));
                    out.println(sb.toString().trim() + " " + ts);
                    System.out.println("[" + ts + "] STATUS requested -> " + sb);
                } else {
                    out.println("ERROR Unknown command " + ts);
                    System.out.println("[" + ts + "] UNKNOWN command: " + line);
                }
            }
        } catch (Exception e) {
            System.out.println("Client disconnected.");
        }
    }
}
