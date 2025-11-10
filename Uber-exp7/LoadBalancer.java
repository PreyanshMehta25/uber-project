import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class LoadBalancer {

    private static final int LOAD_BALANCER_PORT = 8080;
    private static final List<WorkerServerInfo> WORKER_SERVERS = new ArrayList<>();
    private static BlockingQueue<WorkerServerInfo> freeServers;
    private static BlockingQueue<Socket> requestQueue;

    static {
        WORKER_SERVERS.add(new WorkerServerInfo("localhost", 9001));
        WORKER_SERVERS.add(new WorkerServerInfo("localhost", 9002));
        WORKER_SERVERS.add(new WorkerServerInfo("localhost", 9003));
        freeServers = new LinkedBlockingQueue<>(WORKER_SERVERS);
        requestQueue = new LinkedBlockingQueue<>();
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Starting worker servers...");
        for (WorkerServerInfo serverInfo : WORKER_SERVERS) {
            new Thread(new WorkerServer(serverInfo.getPort())).start();
        }
        
        try {
            Thread.sleep(1000); 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        new Thread(LoadBalancer::dispatchRequests).start();

        System.out.println("Load Balancer started on port " + LOAD_BALANCER_PORT);
        System.out.println("Ready to accept and queue client requests...\n");
        
        try (ServerSocket serverSocket = new ServerSocket(LOAD_BALANCER_PORT)) {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Request received and added to the queue. Queue size: " + (requestQueue.size() + 1));
                    requestQueue.put(clientSocket);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Main thread interrupted while queueing request.");
                    break;
                }
            }
        }
    }

    private static void dispatchRequests() {
        ExecutorService processingThreadPool = Executors.newCachedThreadPool();
        while (true) {
            try {
                WorkerServerInfo targetServer = freeServers.take();
                System.out.println("A server is free. Waiting for a request from the queue...");
                Socket clientSocket = requestQueue.take();
                System.out.println("Assigning request to server -> " + targetServer.getPort());
                processingThreadPool.submit(() -> processRequest(clientSocket, targetServer));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Dispatcher thread interrupted.");
                break;
            }
        }
    }

    private static void processRequest(Socket clientSocket, WorkerServerInfo targetServer) {
        try (Socket serverSocket = new Socket(targetServer.getHost(), targetServer.getPort());
             BufferedReader clientIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
             BufferedReader serverIn = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
             PrintWriter serverOut = new PrintWriter(serverSocket.getOutputStream(), true)) {
            
            String request = clientIn.readLine();
            serverOut.println(request);
            String response = serverIn.readLine();
            clientOut.println(response);

        } catch (IOException e) {
            System.err.println("Error processing request: " + e.getMessage());
        } finally {
            try {
                freeServers.put(targetServer);
                System.out.println("Server " + targetServer.getPort() + " is now free.");
                clientSocket.close();
            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class WorkerServerInfo {
        private final String host;
        private final int port;
        public WorkerServerInfo(String host, int port) { this.host = host; this.port = port; }
        public String getHost() { return host; }
        public int getPort() { return port; }
    }
}

class WorkerServer implements Runnable {
    private final int port;

    public WorkerServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Worker Server started and listening on port " + port);
            while (true) {
                try (Socket client = serverSocket.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                     PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {
                    
                    String request = in.readLine();
                    System.out.println("[Worker " + port + "] Starting to process: " + request);
                    Thread.sleep(2000);
                    String response = "Ride completed by Server@" + port + ".";
                    out.println(response);
                    System.out.println("[Worker " + port + "] Finished processing: " + request);
                }
            }
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}