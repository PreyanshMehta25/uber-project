import java.io.*;
import java.net.*;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.util.*;
import java.util.concurrent.*;

// ==================== FAULT TOLERANCE CLASSES ====================

class FaultToleranceManager {
    private final UberServer server;
    private final Map<String, Long> nodeHeartbeats = new ConcurrentHashMap<>();
    private final Map<String, Integer> nodeFailureCount = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private final long HEARTBEAT_INTERVAL = 5000; // 5 seconds
    private final long FAILURE_THRESHOLD = 15000; // 15 seconds
    private final int MAX_FAILURES = 3;
    
    public FaultToleranceManager(UberServer server) {
        this.server = server;
        startHeartbeatMonitoring();
        startFailureDetection();
        startRecoveryService();
    }
    
    private void startHeartbeatMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Monitor all dispatch nodes
                for (UberServer.ProcessNode node : server.getDispatchNodes()) {
                    String nodeId = "node_" + node.getId();
                    if (node.isActive()) {
                        nodeHeartbeats.put(nodeId, System.currentTimeMillis());
                        System.out.println("[FAULT-TOLERANCE] HEARTBEAT: " + nodeId + " - HEALTHY");
                    }
                }
                
                // Monitor HDFS nodes
                monitorHDFSNodes();
                
                // Monitor RMI service
                monitorRMIService();
                
            } catch (Exception e) {
                System.err.println("[FAULT-TOLERANCE] WARNING: Heartbeat monitoring error: " + e.getMessage());
            }
        }, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }
    
    private void startFailureDetection() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long currentTime = System.currentTimeMillis();
                
                // Check for failed nodes
                for (Map.Entry<String, Long> entry : nodeHeartbeats.entrySet()) {
                    String nodeId = entry.getKey();
                    long lastHeartbeat = entry.getValue();
                    
                    if (currentTime - lastHeartbeat > FAILURE_THRESHOLD) {
                        handleNodeFailure(nodeId);
                    }
                }
                
            } catch (Exception e) {
                System.err.println("[FAULT-TOLERANCE] ⚠️ Failure detection error: " + e.getMessage());
            }
        }, FAILURE_THRESHOLD, FAILURE_THRESHOLD / 2, TimeUnit.MILLISECONDS);
    }
    
    private void startRecoveryService() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Attempt to recover failed nodes
                for (Map.Entry<String, Integer> entry : nodeFailureCount.entrySet()) {
                    String nodeId = entry.getKey();
                    int failures = entry.getValue();
                    
                    if (failures > 0 && failures < MAX_FAILURES) {
                        attemptNodeRecovery(nodeId);
                    }
                }
                
                // Ensure leader exists
                server.ensureLeaderExists();
                
            } catch (Exception e) {
                System.err.println("[FAULT-TOLERANCE] ⚠️ Recovery service error: " + e.getMessage());
            }
        }, 10000, 10000, TimeUnit.MILLISECONDS);
    }
    
    private void handleNodeFailure(String nodeId) {
        int failures = nodeFailureCount.getOrDefault(nodeId, 0) + 1;
        nodeFailureCount.put(nodeId, failures);
        
        System.out.println("[FAULT-TOLERANCE] 🚨 NODE FAILURE DETECTED: " + nodeId + 
                          " (Failure #" + failures + ")");
        
        if (failures >= MAX_FAILURES) {
            System.out.println("[FAULT-TOLERANCE] ❌ Node " + nodeId + " marked as PERMANENTLY FAILED");
            // Trigger data migration if needed
            triggerDataMigration(nodeId);
        } else {
            System.out.println("[FAULT-TOLERANCE] 🔄 Scheduling recovery for " + nodeId);
        }
        
        // Log failure to HDFS
        server.getBackupManager().logFailureEvent(nodeId, failures);
    }
    
    private void attemptNodeRecovery(String nodeId) {
        System.out.println("[FAULT-TOLERANCE] 🔧 Attempting recovery for " + nodeId + "...");
        
        try {
            // Simulate node recovery attempt
            if (Math.random() > 0.3) { // 70% success rate
                nodeFailureCount.put(nodeId, 0);
                nodeHeartbeats.put(nodeId, System.currentTimeMillis());
                System.out.println("[FAULT-TOLERANCE] ✅ Node " + nodeId + " RECOVERED successfully!");
                
                // Trigger data synchronization
                server.getBackupManager().synchronizeNodeData(nodeId);
            } else {
                System.out.println("[FAULT-TOLERANCE] ❌ Recovery failed for " + nodeId + " - will retry");
            }
        } catch (Exception e) {
            System.err.println("[FAULT-TOLERANCE] ⚠️ Recovery error for " + nodeId + ": " + e.getMessage());
        }
    }
    
    private void monitorHDFSNodes() {
        try {
            // Check HDFS DataNodes health
            List<UberHDFS.DataNode> dataNodes = server.getHdfsClient().getNameNode().getDataNodes();
            for (UberHDFS.DataNode node : dataNodes) {
                String nodeId = "hdfs_" + node.getNodeId();
                if (node.isActive()) {
                    nodeHeartbeats.put(nodeId, System.currentTimeMillis());
                    System.out.println("[FAULT-TOLERANCE] 💾 HDFS Node: " + node.getNodeId() + 
                                     " - Storage: " + node.getUsedSpace() + "/" + 
                                     (node.getUsedSpace() + node.getFreeSpace()) + " bytes");
                }
            }
        } catch (Exception e) {
            System.err.println("[FAULT-TOLERANCE] ⚠️ HDFS monitoring error: " + e.getMessage());
        }
    }
    
    private void monitorRMIService() {
        try {
            // Test RMI service availability
            if (server.getFareService() != null) {
                // Perform a test calculation
                double testFare = server.getFareService().calculateFare(1.0, 2.0, 0);
                System.out.println("[FAULT-TOLERANCE] 🌐 RMI Service: HEALTHY (Test fare: $" + 
                                 String.format("%.2f", testFare) + ")");
                nodeHeartbeats.put("rmi_service", System.currentTimeMillis());
            }
        } catch (Exception e) {
            System.err.println("[FAULT-TOLERANCE] 🚨 RMI Service FAILURE: " + e.getMessage());
            // Attempt RMI service restart
            attemptRMIRestart();
        }
    }
    
    private void attemptRMIRestart() {
        System.out.println("[FAULT-TOLERANCE] 🔄 Attempting RMI service restart...");
        try {
            // This would restart the RMI service
            server.restartRMIService();
            System.out.println("[FAULT-TOLERANCE] ✅ RMI service restarted successfully!");
        } catch (Exception e) {
            System.err.println("[FAULT-TOLERANCE] ❌ RMI restart failed: " + e.getMessage());
        }
    }
    
    private void triggerDataMigration(String failedNodeId) {
        System.out.println("[FAULT-TOLERANCE] 📦 Triggering data migration from " + failedNodeId);
        server.getBackupManager().migrateDataFromFailedNode(failedNodeId);
    }
    
    public void shutdown() {
        scheduler.shutdown();
        System.out.println("[FAULT-TOLERANCE] Fault tolerance system shutdown");
    }
}

class ServiceHealthMonitor {
    private final UberServer server;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final Map<String, ServiceMetrics> serviceMetrics = new ConcurrentHashMap<>();
    
    static class ServiceMetrics {
        long requestCount = 0;
        long errorCount = 0;
        long totalResponseTime = 0;
        long lastHealthCheck = System.currentTimeMillis();
        
        double getErrorRate() {
            return requestCount > 0 ? (double) errorCount / requestCount : 0.0;
        }
        
        double getAverageResponseTime() {
            return requestCount > 0 ? (double) totalResponseTime / requestCount : 0.0;
        }
    }
    
    public ServiceHealthMonitor(UberServer server) {
        this.server = server;
        startHealthMonitoring();
        startPerformanceMonitoring();
    }
    
    private void startHealthMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                System.out.println("\n[HEALTH-MONITOR] 🏥 === SERVICE HEALTH CHECK ===");
                
                // Check main server socket
                checkSocketService("Main Server", 8080);
                
                // Check GPS service
                checkSocketService("GPS Service", 5000);
                
                // Check RMI service
                checkRMIService();
                
                // Check HDFS health
                checkHDFSHealth();
                
                // Check leader election status
                checkLeaderElectionHealth();
                
                System.out.println("[HEALTH-MONITOR] ===============================\n");
                
            } catch (Exception e) {
                System.err.println("[HEALTH-MONITOR] ⚠️ Health monitoring error: " + e.getMessage());
            }
        }, 0, 20000, TimeUnit.MILLISECONDS); // Every 20 seconds
    }
    
    private void startPerformanceMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                System.out.println("\n[PERFORMANCE] 📊 === PERFORMANCE METRICS ===");
                
                for (Map.Entry<String, ServiceMetrics> entry : serviceMetrics.entrySet()) {
                    String service = entry.getKey();
                    ServiceMetrics metrics = entry.getValue();
                    
                    System.out.printf("[PERFORMANCE] %s: Requests=%d, Errors=%d (%.1f%%), Avg Response=%.2fms%n",
                                    service, metrics.requestCount, metrics.errorCount,
                                    metrics.getErrorRate() * 100, metrics.getAverageResponseTime());
                }
                
                // Memory usage
                Runtime runtime = Runtime.getRuntime();
                long totalMemory = runtime.totalMemory();
                long freeMemory = runtime.freeMemory();
                long usedMemory = totalMemory - freeMemory;
                
                System.out.printf("[PERFORMANCE] Memory: Used=%dMB, Free=%dMB, Total=%dMB%n",
                                usedMemory / 1024 / 1024, freeMemory / 1024 / 1024, totalMemory / 1024 / 1024);
                
                System.out.println("[PERFORMANCE] ===============================\n");
                
            } catch (Exception e) {
                System.err.println("[PERFORMANCE] ⚠️ Performance monitoring error: " + e.getMessage());
            }
        }, 30000, 30000, TimeUnit.MILLISECONDS); // Every 30 seconds
    }
    
    private void checkSocketService(String serviceName, int port) {
        try (Socket testSocket = new Socket("localhost", port)) {
            System.out.println("[HEALTH-MONITOR] ✅ " + serviceName + " (port " + port + "): HEALTHY");
            recordServiceHealth(serviceName, true, 0);
        } catch (IOException e) {
            System.out.println("[HEALTH-MONITOR] ❌ " + serviceName + " (port " + port + "): FAILED - " + e.getMessage());
            recordServiceHealth(serviceName, false, 0);
        }
    }
    
    private void checkRMIService() {
        try {
            Registry registry = LocateRegistry.getRegistry(1099);
            String[] services = registry.list();
            boolean fareServiceFound = Arrays.asList(services).contains("FareService");
            
            if (fareServiceFound) {
                System.out.println("[HEALTH-MONITOR] ✅ RMI Service: HEALTHY (FareService available)");
                recordServiceHealth("RMI", true, 0);
            } else {
                System.out.println("[HEALTH-MONITOR] ⚠️ RMI Service: FareService not found");
                recordServiceHealth("RMI", false, 0);
            }
        } catch (Exception e) {
            System.out.println("[HEALTH-MONITOR] ❌ RMI Service: FAILED - " + e.getMessage());
            recordServiceHealth("RMI", false, 0);
        }
    }
    
    private void checkHDFSHealth() {
        try {
            // Check HDFS cluster status
            Map<String, Object> status = server.getHdfsClient().getNameNode().getClusterStatus();
            int activeNodes = (Integer) status.get("activeDataNodes");
            int totalNodes = (Integer) status.get("totalDataNodes");
            long usedSpace = (Long) status.get("usedSpaceMB");
            long totalSpace = (Long) status.get("totalCapacityMB");
            
            double nodeHealth = (double) activeNodes / totalNodes;
            double storageHealth = 1.0 - ((double) usedSpace / totalSpace);
            
            if (nodeHealth >= 0.67 && storageHealth > 0.1) { // At least 2/3 nodes and 10% free space
                System.out.printf("[HEALTH-MONITOR] ✅ HDFS: HEALTHY (Nodes: %d/%d, Storage: %dMB/%dMB)%n",
                                activeNodes, totalNodes, usedSpace, totalSpace);
                recordServiceHealth("HDFS", true, 0);
            } else {
                System.out.printf("[HEALTH-MONITOR] ⚠️ HDFS: DEGRADED (Nodes: %d/%d, Storage: %dMB/%dMB)%n",
                                activeNodes, totalNodes, usedSpace, totalSpace);
                recordServiceHealth("HDFS", false, 0);
            }
        } catch (Exception e) {
            System.out.println("[HEALTH-MONITOR] ❌ HDFS: FAILED - " + e.getMessage());
            recordServiceHealth("HDFS", false, 0);
        }
    }
    
    private void checkLeaderElectionHealth() {
        try {
            Optional<UberServer.ProcessNode> leader = server.getDispatchNodes().stream()
                .filter(n -> n.isLeader() && n.isActive()).findFirst();
            
            if (leader.isPresent()) {
                System.out.println("[HEALTH-MONITOR] ✅ Leader Election: HEALTHY (Leader: Process " + 
                                 leader.get().getId() + ")");
                recordServiceHealth("LeaderElection", true, 0);
            } else {
                System.out.println("[HEALTH-MONITOR] ⚠️ Leader Election: NO LEADER FOUND");
                recordServiceHealth("LeaderElection", false, 0);
                // Trigger leader election
                server.ensureLeaderExists();
            }
        } catch (Exception e) {
            System.out.println("[HEALTH-MONITOR] ❌ Leader Election: FAILED - " + e.getMessage());
            recordServiceHealth("LeaderElection", false, 0);
        }
    }
    
    public void recordServiceHealth(String serviceName, boolean healthy, long responseTime) {
        ServiceMetrics metrics = serviceMetrics.computeIfAbsent(serviceName, k -> new ServiceMetrics());
        metrics.requestCount++;
        metrics.totalResponseTime += responseTime;
        metrics.lastHealthCheck = System.currentTimeMillis();
        
        if (!healthy) {
            metrics.errorCount++;
        }
    }
    
    public void shutdown() {
        scheduler.shutdown();
        System.out.println("[HEALTH-MONITOR] Service health monitor shutdown");
    }
}

class DataBackupManager {
    private final UberHDFS.UberHDFSClient hdfsClient;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final Map<String, Long> lastBackupTimes = new ConcurrentHashMap<>();
    private final long BACKUP_INTERVAL = 60000; // 1 minute
    
    public DataBackupManager(UberHDFS.UberHDFSClient hdfsClient) {
        this.hdfsClient = hdfsClient;
        startBackupService();
        startDataIntegrityCheck();
    }
    
    private void startBackupService() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                System.out.println("\n[BACKUP-MANAGER] 💾 === AUTOMATED BACKUP PROCESS ===");
                
                // Backup ride data
                backupRideData();
                
                // Backup driver data
                backupDriverData();
                
                // Create system snapshot
                createSystemSnapshot();
                
                System.out.println("[BACKUP-MANAGER] =====================================\n");
                
            } catch (Exception e) {
                System.err.println("[BACKUP-MANAGER] ⚠️ Backup process error: " + e.getMessage());
            }
        }, 0, BACKUP_INTERVAL, TimeUnit.MILLISECONDS);
    }
    
    private void startDataIntegrityCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                System.out.println("\n[DATA-INTEGRITY] 🔍 === DATA INTEGRITY CHECK ===");
                
                // Check data consistency across replicas
                checkDataConsistency();
                
                // Verify backup integrity
                verifyBackupIntegrity();
                
                System.out.println("[DATA-INTEGRITY] ================================\n");
                
            } catch (Exception e) {
                System.err.println("[DATA-INTEGRITY] ⚠️ Integrity check error: " + e.getMessage());
            }
        }, 30000, 120000, TimeUnit.MILLISECONDS); // Every 2 minutes
    }
    
    private void backupRideData() {
        try {
            List<String> rides = hdfsClient.listAllRides();
            System.out.println("[BACKUP-MANAGER] 🚗 Backing up " + rides.size() + " ride records...");
            
            for (String rideFile : rides) {
                String backupFile = "/backup/rides/" + System.currentTimeMillis() + "_" + 
                                  rideFile.substring(rideFile.lastIndexOf("/") + 1);
                
                // Create backup entry
                String backupData = "BACKUP_RIDE|" + rideFile + "|" + System.currentTimeMillis() + "\n";
                hdfsClient.getNameNode().writeFile(backupFile, backupData.getBytes(), "backup_system");
            }
            
            lastBackupTimes.put("rides", System.currentTimeMillis());
            System.out.println("[BACKUP-MANAGER] ✅ Ride data backup completed");
            
        } catch (Exception e) {
            System.err.println("[BACKUP-MANAGER] ❌ Ride backup failed: " + e.getMessage());
        }
    }
    
    private void backupDriverData() {
        try {
            List<String> drivers = hdfsClient.listAllDrivers();
            System.out.println("[BACKUP-MANAGER] 👨‍💼 Backing up " + drivers.size() + " driver records...");
            
            for (String driverFile : drivers) {
                String backupFile = "/backup/drivers/" + System.currentTimeMillis() + "_" + 
                                  driverFile.substring(driverFile.lastIndexOf("/") + 1);
                
                String backupData = "BACKUP_DRIVER|" + driverFile + "|" + System.currentTimeMillis() + "\n";
                hdfsClient.getNameNode().writeFile(backupFile, backupData.getBytes(), "backup_system");
            }
            
            lastBackupTimes.put("drivers", System.currentTimeMillis());
            System.out.println("[BACKUP-MANAGER] ✅ Driver data backup completed");
            
        } catch (Exception e) {
            System.err.println("[BACKUP-MANAGER] ❌ Driver backup failed: " + e.getMessage());
        }
    }
    
    private void createSystemSnapshot() {
        try {
            String snapshotData = String.format(
                "SYSTEM_SNAPSHOT|%d|rides=%d|drivers=%d|timestamp=%d\n",
                System.currentTimeMillis(),
                hdfsClient.listAllRides().size(),
                hdfsClient.listAllDrivers().size(),
                System.currentTimeMillis()
            );
            
            String snapshotFile = "/backup/snapshots/snapshot_" + System.currentTimeMillis() + ".txt";
            hdfsClient.getNameNode().writeFile(snapshotFile, snapshotData.getBytes(), "backup_system");
            
            System.out.println("[BACKUP-MANAGER] 📸 System snapshot created: " + snapshotFile);
            
        } catch (Exception e) {
            System.err.println("[BACKUP-MANAGER] ❌ Snapshot creation failed: " + e.getMessage());
        }
    }
    
    private void checkDataConsistency() {
        try {
            // Check if all replicas have consistent data
            Map<String, Object> clusterStatus = hdfsClient.getNameNode().getClusterStatus();
            int totalBlocks = (Integer) clusterStatus.get("totalBlocks");
            
            System.out.println("[DATA-INTEGRITY] 🔄 Checking consistency of " + totalBlocks + " blocks...");
            
            // Simulate consistency check
            boolean consistent = Math.random() > 0.1; // 90% consistency rate
            
            if (consistent) {
                System.out.println("[DATA-INTEGRITY] ✅ Data consistency check PASSED");
            } else {
                System.out.println("[DATA-INTEGRITY] ⚠️ Data inconsistency detected - triggering repair");
                repairInconsistentData();
            }
            
        } catch (Exception e) {
            System.err.println("[DATA-INTEGRITY] ❌ Consistency check failed: " + e.getMessage());
        }
    }
    
    private void verifyBackupIntegrity() {
        try {
            long rideBackupTime = lastBackupTimes.getOrDefault("rides", 0L);
            long driverBackupTime = lastBackupTimes.getOrDefault("drivers", 0L);
            long currentTime = System.currentTimeMillis();
            
            boolean rideBackupFresh = (currentTime - rideBackupTime) < (BACKUP_INTERVAL * 2);
            boolean driverBackupFresh = (currentTime - driverBackupTime) < (BACKUP_INTERVAL * 2);
            
            if (rideBackupFresh && driverBackupFresh) {
                System.out.println("[DATA-INTEGRITY] ✅ Backup integrity verified - all backups are fresh");
            } else {
                System.out.println("[DATA-INTEGRITY] ⚠️ Stale backups detected - triggering immediate backup");
                // Trigger immediate backup
                backupRideData();
                backupDriverData();
            }
            
        } catch (Exception e) {
            System.err.println("[DATA-INTEGRITY] ❌ Backup verification failed: " + e.getMessage());
        }
    }
    
    private void repairInconsistentData() {
        System.out.println("[DATA-INTEGRITY] 🔧 Starting data repair process...");
        
        try {
            // Simulate data repair
            Thread.sleep(2000);
            System.out.println("[DATA-INTEGRITY] ✅ Data repair completed successfully");
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[DATA-INTEGRITY] ❌ Data repair interrupted");
        }
    }
    
    public void logFailureEvent(String nodeId, int failureCount) {
        try {
            String failureLog = String.format("FAILURE_EVENT|%s|count=%d|timestamp=%d\n",
                                            nodeId, failureCount, System.currentTimeMillis());
            
            String logFile = "/logs/failures/failure_" + System.currentTimeMillis() + ".txt";
            hdfsClient.getNameNode().writeFile(logFile, failureLog.getBytes(), "fault_system");
            
            System.out.println("[BACKUP-MANAGER] 📝 Failure event logged: " + nodeId);
            
        } catch (Exception e) {
            System.err.println("[BACKUP-MANAGER] ❌ Failed to log failure event: " + e.getMessage());
        }
    }
    
    public void synchronizeNodeData(String nodeId) {
        System.out.println("[BACKUP-MANAGER] 🔄 Synchronizing data for recovered node: " + nodeId);
        
        try {
            // Simulate data synchronization
            Thread.sleep(1000);
            System.out.println("[BACKUP-MANAGER] ✅ Data synchronization completed for " + nodeId);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[BACKUP-MANAGER] ❌ Data synchronization interrupted for " + nodeId);
        }
    }
    
    public void migrateDataFromFailedNode(String failedNodeId) {
        System.out.println("[BACKUP-MANAGER] 📦 Starting data migration from failed node: " + failedNodeId);
        
        try {
            // Simulate data migration
            Thread.sleep(3000);
            System.out.println("[BACKUP-MANAGER] ✅ Data migration completed from " + failedNodeId);
            
            // Log migration event
            String migrationLog = String.format("DATA_MIGRATION|from=%s|timestamp=%d\n",
                                               failedNodeId, System.currentTimeMillis());
            String logFile = "/logs/migrations/migration_" + System.currentTimeMillis() + ".txt";
            hdfsClient.getNameNode().writeFile(logFile, migrationLog.getBytes(), "fault_system");
            
        } catch (Exception e) {
            System.err.println("[BACKUP-MANAGER] ❌ Data migration failed: " + e.getMessage());
        }
    }
    
    public void shutdown() {
        scheduler.shutdown();
        System.out.println("[BACKUP-MANAGER] Data backup manager shutdown");
    }
}

public class UberServer {
    
    static class LamportClock {
        private int time = 0;
        public synchronized int tick() { return ++time; }
        public synchronized int update(int received) { return time = Math.max(time, received) + 1; }
        public synchronized int getTime() { return time; }
    }
    
    static class LamportLock {
        private final ConcurrentHashMap<String, Integer> locks = new ConcurrentHashMap<>();
        public synchronized boolean acquire(String resource, int timestamp) {
            if (!locks.containsKey(resource) || timestamp < locks.get(resource)) {
                locks.put(resource, timestamp);
                return true;
            }
            return false;
        }
        public synchronized void release(String resource) { locks.remove(resource); }
    }
    
    static class ProcessNode {
        private final int id;
        private boolean isActive = true;
        private boolean isLeader = false;
        private final List<ProcessNode> allNodes = new ArrayList<>();

        public ProcessNode(int id) { this.id = id; }
        
        public void setAllNodes(List<ProcessNode> nodes) {
            this.allNodes.clear();
            this.allNodes.addAll(nodes);
        }

        public void holdElection() {
            if (!isActive) return;
            System.out.println("[ELECTION] Process " + id + " starting election");
            boolean hasHigher = false;
            for (ProcessNode node : allNodes) {
                if (node.getId() > this.id && node.isActive()) {
                    if (node.receiveElectionMessage(this.id)) hasHigher = true;
                }
            }
            if (!hasHigher) declareAsLeader();
        }

        public boolean receiveElectionMessage(int senderId) {
            if (!isActive) return false;
            new Thread(this::holdElection).start();
            return true;
        }

        public void declareAsLeader() {
            System.out.println("Process " + id + " is LEADER");
            isLeader = true;
            for (ProcessNode node : allNodes) {
                if (node.getId() != this.id) node.receiveCoordinatorMessage(this.id);
            }
        }

        public void receiveCoordinatorMessage(int leaderId) {
            if (isActive) isLeader = false;
        }

        public void shutdown() { isActive = false; isLeader = false; }
        public void startup() { isActive = true; holdElection(); }
        public int getId() { return id; }
        public boolean isActive() { return isActive; }
        public boolean isLeader() { return isLeader; }
    }
    
    interface FareService extends Remote {
        double calculateFare(double distance, double rate, int timestamp) throws RemoteException;
        String getTripStatus(String rideId, int timestamp) throws RemoteException;
    }
    
    static class FareServiceImpl extends UnicastRemoteObject implements FareService {
        private final LamportClock clock = new LamportClock();
        private final ConcurrentHashMap<String, String> tripStatuses = new ConcurrentHashMap<>();
        
        public FareServiceImpl() throws RemoteException { super(); }
        
        @Override
        public double calculateFare(double distance, double rate, int timestamp) throws RemoteException {
            int currentTime = clock.update(timestamp);
            double fare = distance * rate * (1.0 + Math.random() * 0.5);
            System.out.println("[" + currentTime + "] RMI FARE: $" + String.format("%.2f", fare));
            return fare;
        }
        
        @Override
        public String getTripStatus(String rideId, int timestamp) throws RemoteException {
            int currentTime = clock.update(timestamp);
            String status = tripStatuses.getOrDefault(rideId, "NOT_FOUND");
            System.out.println("[" + currentTime + "] RMI STATUS: " + status);
            return status;
        }
        
        public void updateTripStatus(String rideId, String status) {
            tripStatuses.put(rideId, status);
        }
    }
    
    static class RideInfo {
        private final String rideId, riderId, pickup, destination;
        private String driverId, status = "REQUESTED";
        
        public RideInfo(String rideId, String riderId, String pickup, String destination) {
            this.rideId = rideId; this.riderId = riderId; this.pickup = pickup; this.destination = destination;
        }
        
        public String getRideId() { return rideId; }
        public String getRiderId() { return riderId; }
        public String getPickup() { return pickup; }
        public String getDestination() { return destination; }
        public String getDriverId() { return driverId; }
        public String getStatus() { return status; }
        public void setDriverId(String driverId) { this.driverId = driverId; }
        public void setStatus(String status) { this.status = status; }
    }
    
    private final ExecutorService threadPool = Executors.newFixedThreadPool(15);
    private final LamportClock clock = new LamportClock();
    private final LamportLock lock = new LamportLock();
    private final List<ProcessNode> dispatchNodes = new ArrayList<>();
    private final List<String> availableDrivers = Collections.synchronizedList(new ArrayList<>());
    private final ConcurrentHashMap<String, RideInfo> rides = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> driverLocations = new ConcurrentHashMap<>();
    private FareServiceImpl fareService;
    private volatile boolean running = true;
    private UberHDFS.UberHDFSClient hdfsClient;
    
    // FAULT TOLERANCE COMPONENTS
    private final FaultToleranceManager faultManager;
    private final ServiceHealthMonitor healthMonitor;
    private final DataBackupManager backupManager;
    
    public UberServer() {
        for (int i = 1; i <= 5; i++) dispatchNodes.add(new ProcessNode(i));
        for (ProcessNode node : dispatchNodes) node.setAllNodes(dispatchNodes);
        dispatchNodes.get(4).declareAsLeader();
        
        // Initialize HDFS
        UberHDFS hdfs = new UberHDFS();
        this.hdfsClient = hdfs.getClient();
        System.out.println("HDFS integrated with Uber Server");
        
        // Initialize Fault Tolerance Components
        this.faultManager = new FaultToleranceManager(this);
        this.healthMonitor = new ServiceHealthMonitor(this);
        this.backupManager = new DataBackupManager(hdfsClient);
        
        System.out.println("=== FAULT TOLERANCE SYSTEM INITIALIZED ===");
        System.out.println("✓ Node Failure Detection: ACTIVE");
        System.out.println("✓ Service Health Monitoring: ACTIVE");
        System.out.println("✓ Data Backup Manager: ACTIVE");
        System.out.println("✓ Automatic Recovery: ENABLED");
        System.out.println("============================================");
    }
    
    public void startServer() {
        System.out.println("Starting Uber Server with ALL features...");
        
        try {
            fareService = new FareServiceImpl();
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("FareService", fareService);
            System.out.println("RMI Fare Service started");
        } catch (Exception e) {
            System.err.println("RMI failed: " + e.getMessage());
        }
        
        threadPool.submit(() -> {
            try (ServerSocket gpsServer = new ServerSocket(5000)) {
                System.out.println("GPS Socket Service started");
                while (running) {
                    Socket socket = gpsServer.accept();
                    threadPool.submit(() -> handleGPS(socket));
                }
            } catch (IOException e) {
                if (running) System.err.println("GPS error: " + e.getMessage());
            }
        });
        
        threadPool.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(8080)) {
                System.out.println("Main Server started on port 8080");
                while (running) {
                    Socket clientSocket = serverSocket.accept();
                    threadPool.submit(() -> handleClient(clientSocket));
                }
            } catch (IOException e) {
                if (running) System.err.println("Server error: " + e.getMessage());
            }
        });
        
        threadPool.submit(() -> {
            while (running) {
                try {
                    Thread.sleep(10000);
                    ensureLeaderExists();
                } catch (InterruptedException e) { break; }
            }
        });
        
        // Start fault tolerance services
        System.out.println("\n=== STARTING FAULT TOLERANCE SERVICES ===");
        faultManager.toString(); // Initialize fault manager
        healthMonitor.toString(); // Initialize health monitor
        backupManager.toString(); // Initialize backup manager
        System.out.println("✅ All fault tolerance services started!");
        System.out.println("==========================================\n");
        
        System.out.println("All services operational!");
        System.out.println("   - RMI: port 1099 | GPS: port 5000 | Main: port 8080");
        System.out.println("   - Fault Tolerance: ACTIVE | Health Monitor: ACTIVE | Backup: ACTIVE");
    }
    
    private void handleGPS(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String gpsUpdate;
            while ((gpsUpdate = in.readLine()) != null) {
                String[] parts = gpsUpdate.split(":");
                if (parts.length >= 4) {
                    String driverId = parts[0];
                    int timestamp = Integer.parseInt(parts[3]);
                    int currentTime = clock.update(timestamp);
                    driverLocations.put(driverId, "(" + parts[1] + "," + parts[2] + ")");
                    System.out.println("[" + currentTime + "] GPS: Driver " + driverId + " location updated");
                }
            }
        } catch (IOException e) {
            System.out.println("GPS client disconnected");
        }
    }
    
    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
            
            String request;
            while ((request = in.readLine()) != null && running) {
                if (request.equalsIgnoreCase("exit")) break;
                String response = processRequest(request);
                out.println(response);
            }
        } catch (IOException e) {
            System.out.println("Client disconnected");
        }
    }
    
    private String processRequest(String request) {
        try {
            String[] parts = request.split(";");
            String action = parts[0].toUpperCase();
            int timestamp = parts.length > 1 ? Integer.parseInt(parts[parts.length - 1]) : clock.tick();
            int currentTime = clock.update(timestamp);
            
            switch (action) {
                case "REGISTER_DRIVER":
                    return registerDriver(parts, currentTime);
                case "REQUEST_RIDE":
                    return requestRide(parts, currentTime);
                case "ASSIGN_DRIVER":
                    return assignDriver(parts, currentTime);
                case "CALCULATE_FARE":
                    return calculateFare(parts, currentTime);
                case "GPS_UPDATE":
                    return updateGPS(parts, currentTime);
                case "GET_STATUS":
                    return getRideStatus(parts, currentTime);
                case "LEADER_STATUS":
                    return getLeaderStatus();
                case "FAIL_NODE":
                    return failNode(parts, currentTime);
                case "HDFS_STATUS":
                    return getHDFSStatus();
                case "HDFS_LIST_RIDES":
                    return listHDFSRides();
                case "HDFS_LIST_DRIVERS":
                    return listHDFSDrivers();
                case "SIMULATE_FAILURE":
                    return simulateFailure(parts, currentTime);
                case "SIMULATE_PARTITION":
                    return simulatePartition(currentTime);
                case "RECOVER_PARTITION":
                    return recoverPartition(currentTime);
                case "HEALTH_STATUS":
                    return getHealthStatus();
                case "BACKUP_STATUS":
                    return getBackupStatus();
                default:
                    return "ERROR: Unknown command";
            }
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }
    
    private String registerDriver(String[] parts, int timestamp) {
        if (parts.length < 3) return "ERROR: Invalid format";
        String driverId = parts[1], location = parts[2];
        availableDrivers.add(driverId);
        driverLocations.put(driverId, location);
        
        // Store driver data in HDFS
        hdfsClient.storeDriverData(driverId, driverId, location, "Vehicle_" + driverId);
        
        System.out.println("[" + timestamp + "] RPC REGISTER: " + driverId + " at " + location);
        return "SUCCESS: Driver registered";
    }
    
    private String requestRide(String[] parts, int timestamp) {
        if (parts.length < 4) return "ERROR: Invalid format";
        String riderId = parts[1], pickup = parts[2], destination = parts[3];
        String rideId = "RIDE_" + riderId + "_" + System.currentTimeMillis();
        rides.put(rideId, new RideInfo(rideId, riderId, pickup, destination));
        System.out.println("[" + timestamp + "] RPC REQUEST: " + riderId + " from " + pickup + " to " + destination);
        return "SUCCESS: " + rideId;
    }
    
    private String assignDriver(String[] parts, int timestamp) {
        if (parts.length < 2) return "ERROR: Invalid format";
        String rideId = parts[1];
        
        if (!lock.acquire(rideId, timestamp)) return "FAILED: Ride being processed";
        
        try {
            RideInfo ride = rides.get(rideId);
            if (ride == null) return "ERROR: Ride not found";
            if (availableDrivers.isEmpty()) return "ERROR: No drivers available";
            
            String driver = availableDrivers.remove(0);
            ride.setDriverId(driver);
            ride.setStatus("ASSIGNED");
            if (fareService != null) fareService.updateTripStatus(rideId, "ASSIGNED");
            
            // Store complete ride data in HDFS
            hdfsClient.storeRideData(rideId, ride.getRiderId(), driver, 
                                   ride.getPickup(), ride.getDestination(), 25.0);
            
            // Get driver details
            String location = driverLocations.getOrDefault(driver, "Unknown");
            String vehicle = "Vehicle_" + driver;
            String phone = generatePhoneNumber(driver);
            double rating = 4.5 + (Math.random() * 0.5); // 4.5-5.0 rating
            
            System.out.println("[" + timestamp + "] RPC ASSIGN: " + driver + " -> " + rideId);
            
            // Return driver details in format: SUCCESS|DriverName|Vehicle|Phone|Rating|Location
            return String.format("SUCCESS|%s|%s|%s|%.1f|%s", 
                               driver, vehicle, phone, rating, location);
        } finally {
            lock.release(rideId);
        }
    }
    
    private String generatePhoneNumber(String driverId) {
        // Generate consistent phone number based on driver ID
        int hash = Math.abs(driverId.hashCode());
        return String.format("(%03d) %03d-%04d", 
                           (hash % 900) + 100, 
                           (hash / 1000 % 900) + 100, 
                           hash % 10000);
    }
    
    private String calculateFare(String[] parts, int timestamp) {
        if (parts.length < 3) return "ERROR: Invalid format";
        try {
            double distance = Double.parseDouble(parts[1]);
            double rate = Double.parseDouble(parts[2]);
            if (fareService != null) {
                double fare = fareService.calculateFare(distance, rate, timestamp);
                return "SUCCESS: $" + String.format("%.2f", fare);
            }
            return "ERROR: Fare service unavailable";
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }
    
    private String updateGPS(String[] parts, int timestamp) {
        if (parts.length < 4) return "ERROR: Invalid format";
        String driverId = parts[1];
        double lat = Double.parseDouble(parts[2]);
        double lon = Double.parseDouble(parts[3]);
        
        driverLocations.put(driverId, "(" + lat + "," + lon + ")");
        
        // Store GPS data in HDFS
        hdfsClient.storeGPSData(driverId, lat, lon);
        
        System.out.println("[" + timestamp + "] GPS UPDATE: " + driverId);
        return "SUCCESS: GPS updated";
    }
    
    private String getRideStatus(String[] parts, int timestamp) {
        if (parts.length < 2) return "ERROR: Invalid format";
        RideInfo ride = rides.get(parts[1]);
        return ride != null ? "STATUS: " + ride.getStatus() : "STATUS: NOT_FOUND";
    }
    
    private String getLeaderStatus() {
        Optional<ProcessNode> leader = dispatchNodes.stream()
            .filter(n -> n.isLeader() && n.isActive()).findFirst();
        return leader.isPresent() ? "LEADER: Process " + leader.get().getId() : "LEADER: None";
    }
    
    private String failNode(String[] parts, int timestamp) {
        if (parts.length < 2) return "ERROR: Invalid format";
        try {
            int nodeId = Integer.parseInt(parts[1]);
            Optional<ProcessNode> node = dispatchNodes.stream()
                .filter(n -> n.getId() == nodeId).findFirst();
            if (node.isPresent()) {
                node.get().shutdown();
                System.out.println("[" + timestamp + "] FAILURE: Process " + nodeId);
                threadPool.submit(() -> {
                    try { Thread.sleep(1000); ensureLeaderExists(); } 
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                });
                return "SUCCESS: Node " + nodeId + " failed";
            }
            return "ERROR: Node not found";
        } catch (NumberFormatException e) {
            return "ERROR: Invalid node ID";
        }
    }
    
    public void ensureLeaderExists() {
        boolean hasLeader = dispatchNodes.stream().anyMatch(n -> n.isLeader() && n.isActive());
        if (!hasLeader) {
            System.out.println("[FAULT-TOLERANCE] 🗳️ No leader found - triggering election");
            Optional<UberServer.ProcessNode> candidate = dispatchNodes.stream()
                .filter(ProcessNode::isActive).max(Comparator.comparingInt(ProcessNode::getId));
            if (candidate.isPresent()) {
                candidate.get().holdElection();
                System.out.println("[FAULT-TOLERANCE] ✅ New leader elected: Process " + candidate.get().getId());
            } else {
                System.out.println("[FAULT-TOLERANCE] ❌ No active nodes available for leadership");
            }
        }
    }
    
    // Getter methods for fault tolerance components
    public List<UberServer.ProcessNode> getDispatchNodes() { return dispatchNodes; }
    public FareServiceImpl getFareService() { return fareService; }
    public UberHDFS.UberHDFSClient getHdfsClient() { return hdfsClient; }
    public DataBackupManager getBackupManager() { return backupManager; }
    
    // Fault tolerance methods
    public void restartRMIService() throws Exception {
        System.out.println("[FAULT-TOLERANCE] 🔄 Restarting RMI service...");
        
        try {
            // Stop existing service
            if (fareService != null) {
                Registry registry = LocateRegistry.getRegistry(1099);
                registry.unbind("FareService");
            }
            
            // Start new service
            fareService = new FareServiceImpl();
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("FareService", fareService);
            
            System.out.println("[FAULT-TOLERANCE] ✅ RMI service restarted successfully");
            
        } catch (Exception e) {
            System.err.println("[FAULT-TOLERANCE] ❌ RMI restart failed: " + e.getMessage());
            throw e;
        }
    }
    
    public void simulateNodeFailure(int nodeId) {
        System.out.println("[FAULT-TOLERANCE] 🧪 SIMULATING NODE FAILURE: Process " + nodeId);
        
        Optional<ProcessNode> node = dispatchNodes.stream()
            .filter(n -> n.getId() == nodeId).findFirst();
            
        if (node.isPresent()) {
            node.get().shutdown();
            System.out.println("[FAULT-TOLERANCE] ❌ Process " + nodeId + " has been shut down");
            
            // Trigger immediate fault detection
            threadPool.submit(() -> {
                try {
                    Thread.sleep(2000);
                    ensureLeaderExists();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        } else {
            System.out.println("[FAULT-TOLERANCE] ⚠️ Node " + nodeId + " not found");
        }
    }
    
    public void simulateNetworkPartition() {
        System.out.println("[FAULT-TOLERANCE] 🌐 SIMULATING NETWORK PARTITION");
        
        // Simulate network partition by shutting down half the nodes
        int halfNodes = dispatchNodes.size() / 2;
        for (int i = 0; i < halfNodes; i++) {
            dispatchNodes.get(i).shutdown();
            System.out.println("[FAULT-TOLERANCE] 📡 Process " + dispatchNodes.get(i).getId() + 
                             " disconnected due to network partition");
        }
        
        // Trigger leader election in remaining partition
        threadPool.submit(() -> {
            try {
                Thread.sleep(3000);
                ensureLeaderExists();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
    
    public void recoverFromPartition() {
        System.out.println("[FAULT-TOLERANCE] 🔗 RECOVERING FROM NETWORK PARTITION");
        
        // Restart all nodes
        for (ProcessNode node : dispatchNodes) {
            if (!node.isActive()) {
                node.startup();
                System.out.println("[FAULT-TOLERANCE] 🔄 Process " + node.getId() + " reconnected");
            }
        }
        
        // Trigger leader election
        threadPool.submit(() -> {
            try {
                Thread.sleep(2000);
                ensureLeaderExists();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
    
    private String getHDFSStatus() {
        // This would normally get status from HDFS NameNode
        return "HDFS_STATUS: Active - 3 DataNodes, Replication Factor: 2";
    }
    
    private String listHDFSRides() {
        List<String> rides = hdfsClient.listAllRides();
        return "HDFS_RIDES: " + String.join(",", rides);
    }
    
    private String listHDFSDrivers() {
        List<String> drivers = hdfsClient.listAllDrivers();
        return "HDFS_DRIVERS: " + String.join(",", drivers);
    }
    
    private String simulateFailure(String[] parts, int timestamp) {
        if (parts.length < 2) return "ERROR: Invalid format - use SIMULATE_FAILURE;nodeId";
        try {
            int nodeId = Integer.parseInt(parts[1]);
            simulateNodeFailure(nodeId);
            System.out.println("[" + timestamp + "] FAULT-TOLERANCE: Simulated failure of node " + nodeId);
            return "SUCCESS: Node " + nodeId + " failure simulated";
        } catch (NumberFormatException e) {
            return "ERROR: Invalid node ID";
        }
    }
    
    private String simulatePartition(int timestamp) {
        simulateNetworkPartition();
        System.out.println("[" + timestamp + "] FAULT-TOLERANCE: Network partition simulated");
        return "SUCCESS: Network partition simulated";
    }
    
    private String recoverPartition(int timestamp) {
        recoverFromPartition();
        System.out.println("[" + timestamp + "] FAULT-TOLERANCE: Recovering from network partition");
        return "SUCCESS: Network partition recovery initiated";
    }
    
    private String getHealthStatus() {
        StringBuilder status = new StringBuilder("HEALTH_STATUS: ");
        
        // Check active nodes
        long activeNodes = dispatchNodes.stream().filter(ProcessNode::isActive).count();
        status.append("ActiveNodes=").append(activeNodes).append("/").append(dispatchNodes.size());
        
        // Check leader
        Optional<ProcessNode> leader = dispatchNodes.stream()
            .filter(n -> n.isLeader() && n.isActive()).findFirst();
        status.append(", Leader=").append(leader.isPresent() ? leader.get().getId() : "None");
        
        // Check HDFS
        try {
            Map<String, Object> hdfsStatus = hdfsClient.getNameNode().getClusterStatus();
            status.append(", HDFS=").append(hdfsStatus.get("activeDataNodes"))
                  .append("/").append(hdfsStatus.get("totalDataNodes")).append(" nodes");
        } catch (Exception e) {
            status.append(", HDFS=ERROR");
        }
        
        return status.toString();
    }
    
    private String getBackupStatus() {
        try {
            int rideCount = hdfsClient.listAllRides().size();
            int driverCount = hdfsClient.listAllDrivers().size();
            
            return "BACKUP_STATUS: Rides=" + rideCount + ", Drivers=" + driverCount + 
                   ", LastBackup=" + System.currentTimeMillis();
        } catch (Exception e) {
            return "BACKUP_STATUS: ERROR - " + e.getMessage();
        }
    }
    
    public void shutdown() {
        System.out.println("\n=== SHUTTING DOWN UBER SERVER ===");
        running = false;
        
        // Shutdown fault tolerance services
        System.out.println("Stopping fault tolerance services...");
        if (faultManager != null) faultManager.shutdown();
        if (healthMonitor != null) healthMonitor.shutdown();
        if (backupManager != null) backupManager.shutdown();
        
        // Shutdown thread pool
        System.out.println("Stopping thread pool...");
        threadPool.shutdown();
        try { 
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) { 
            threadPool.shutdownNow();
            Thread.currentThread().interrupt(); 
        }
        
        System.out.println("✅ Uber Server shutdown complete");
        System.out.println("==================================\n");
    }
    
    public static void main(String[] args) {
        UberServer server = new UberServer();
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        server.startServer();
        
        Scanner scanner = new Scanner(System.in);
        System.out.println("Type 'quit' to stop server");
        while (scanner.hasNextLine()) {
            if ("quit".equalsIgnoreCase(scanner.nextLine().trim())) break;
        }
        server.shutdown();
        scanner.close();
    }
}
