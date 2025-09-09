import java.io.*;
import java.net.*;

public class Dispatcher {
    public static void main(String[] args) throws Exception {
        ServerSocket gpsServer = new ServerSocket(5000);
        ServerSocket bookingServer = new ServerSocket(7000);

        System.out.println("Dispatcher running...");
        
        new Thread(() -> {
            while (true) {
                try {
                    Socket socket = bookingServer.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String booking = in.readLine();
                    System.out.println("New Booking: " + booking);
                    socket.close();
                } catch (Exception e) { e.printStackTrace(); }
            }
        }).start();

        while (true) {
            Socket driverSocket = gpsServer.accept();
            BufferedReader in = new BufferedReader(new InputStreamReader(driverSocket.getInputStream()));

            String gpsUpdate;
            while ((gpsUpdate = in.readLine()) != null) {
                System.out.println("GPS Update: " + gpsUpdate);

                try (Socket riderSocket = new Socket("localhost", 6000);
                     PrintWriter riderOut = new PrintWriter(riderSocket.getOutputStream(), true)) {
                    riderOut.println("Rider Update: " + gpsUpdate);
                }
            }
            driverSocket.close();
        }
    }
}
