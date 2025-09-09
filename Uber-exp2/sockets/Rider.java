import java.io.*;
import java.net.*;

public class Rider {
    public static void main(String[] args) throws Exception {
        ServerSocket riderServer = new ServerSocket(6000);
        System.out.println("Rider waiting for status updates...");

        while (true) {
            Socket socket = riderServer.accept();
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("Rider App: " + in.readLine());
            socket.close();
        }
    }
}
