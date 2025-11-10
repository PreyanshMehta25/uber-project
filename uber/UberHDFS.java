import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;


public class UberHDFS {
    
    static class HDFSBlock {
        private final String blockId;
        private final byte[] data;
        private final int size;
        private final List<String> replicas;
        
        public HDFSBlock(String blockId, byte[] data) {
            this.blockId = blockId;
            this.data = data.clone();
            this.size = data.length;
            this.replicas = new ArrayList<>();
        }
        
        public String getBlockId() { return blockId; }
        public byte[] getData() { return data.clone(); }
        public int getSize() { return size; }
        public List<String> getReplicas() { return new ArrayList<>(replicas); }
        public void addReplica(String dataNodeId) { replicas.add(dataNodeId); }
    }
    
    static class FileMetadata {
        private final String fileName;
        private final long fileSize;
        private final List<String> blockIds;
        private final long timestamp;
        private final String owner;
        
        public FileMetadata(String fileName, long fileSize, String owner) {
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.owner = owner;
            this.blockIds = new ArrayList<>();
            this.timestamp = System.currentTimeMillis();
        }
        
        public String getFileName() { return fileName; }
        public long getFileSize() { return fileSize; }
        public List<String> getBlockIds() { return new ArrayList<>(blockIds); }
        public long getTimestamp() { return timestamp; }
        public String getOwner() { return owner; }
        public void addBlock(String blockId) { blockIds.add(blockId); }
    }
    
    static class DataNode {
        private final String nodeId;
        private final String address;
        private final int port;
        private final File storageDir;
        private final Map<String, HDFSBlock> blocks;
        private final long capacity;
        private long usedSpace;
        private boolean isActive;

        public DataNode(String nodeId, String address, int port, long capacity, File storageDir) {
            this.nodeId = nodeId;
            this.address = address;
            this.port = port;
            this.capacity = capacity;
            this.blocks = new ConcurrentHashMap<>();
            this.usedSpace = 0;
            this.isActive = true;
            this.storageDir = storageDir;

            // Ensure storage directories exist
            File blocksDir = new File(storageDir, "blocks");
            if (!blocksDir.exists()) blocksDir.mkdirs();
        }
        
        public boolean storeBlock(HDFSBlock block) {
            if (usedSpace + block.getSize() > capacity) {
                return false;
            }

            try {
                File blocksDir = new File(storageDir, "blocks");
                if (!blocksDir.exists()) blocksDir.mkdirs();

                File blockFile = new File(blocksDir, block.getBlockId() + ".blk");
                // ensure parent dirs exist (blockId may contain path-like segments)
                File parent = blockFile.getParentFile();
                if (parent != null && !parent.exists()) parent.mkdirs();
                try (FileOutputStream fos = new FileOutputStream(blockFile)) {
                    fos.write(block.getData());
                }

                // Keep in-memory reference for quick reads
                blocks.put(block.getBlockId(), block);
                usedSpace += block.getSize();
                block.addReplica(nodeId);

                System.out.println("DataNode " + nodeId + ": Stored block " + block.getBlockId() + 
                                 " (" + block.getSize() + " bytes) -> " + blockFile.getAbsolutePath());
                return true;
            } catch (IOException e) {
                System.err.println("DataNode " + nodeId + ": Failed to store block " + block.getBlockId() + " - " + e.getMessage());
                return false;
            }
        }
        
        public HDFSBlock getBlock(String blockId) {
            HDFSBlock block = blocks.get(blockId);
            if (block != null) return block;

            // Try to load from disk
            File blockFile = new File(new File(storageDir, "blocks"), blockId + ".blk");
            if (blockFile.exists()) {
                try {
                    byte[] data = Files.readAllBytes(blockFile.toPath());
                    HDFSBlock loaded = new HDFSBlock(blockId, data);
                    blocks.put(blockId, loaded);
                    return loaded;
                } catch (IOException e) {
                    System.err.println("DataNode " + nodeId + ": Failed to read block file " + blockFile.getAbsolutePath());
                }
            }

            return null;
        }
        
        public boolean deleteBlock(String blockId) {
            HDFSBlock block = blocks.remove(blockId);
            if (block != null) {
                usedSpace -= block.getSize();
            }

            File blockFile = new File(new File(storageDir, "blocks"), blockId + ".blk");
            if (blockFile.exists()) {
                if (blockFile.delete()) {
                    System.out.println("DataNode " + nodeId + ": Deleted block file " + blockFile.getAbsolutePath());
                } else {
                    System.err.println("DataNode " + nodeId + ": Failed to delete block file " + blockFile.getAbsolutePath());
                }
            }

            return true;
        }
        
        public String getNodeId() { return nodeId; }
        public String getAddress() { return address; }
        public int getPort() { return port; }
        public long getUsedSpace() { return usedSpace; }
        public long getFreeSpace() { return capacity - usedSpace; }
        public boolean isActive() { return isActive; }
        public void setActive(boolean active) { this.isActive = active; }
        
        public Map<String, Integer> getBlockSummary() {
            Map<String, Integer> summary = new HashMap<>();
            summary.put("totalBlocks", blocks.size());
            summary.put("usedSpaceMB", (int)(usedSpace / (1024 * 1024)));
            summary.put("freeSpaceMB", (int)(getFreeSpace() / (1024 * 1024)));
            return summary;
        }
    }
    
    static class NameNode {
        private final Map<String, FileMetadata> fileSystem;
        private final Map<String, DataNode> dataNodes;
        private final Map<String, HDFSBlock> blockLocations;
        private final int replicationFactor;
        private final int blockSize;
        private final File metaDir;
        
        public NameNode(File metaDir, int replicationFactor, int blockSize) {
            this.fileSystem = new ConcurrentHashMap<>();
            this.dataNodes = new ConcurrentHashMap<>();
            this.blockLocations = new ConcurrentHashMap<>();
            this.replicationFactor = replicationFactor;
            this.blockSize = blockSize;
            this.metaDir = metaDir;

            if (this.metaDir != null && !this.metaDir.exists()) this.metaDir.mkdirs();

            System.out.println("NameNode initialized with replication factor: " + replicationFactor + 
                             ", block size: " + blockSize + " bytes");
            if (this.metaDir != null) System.out.println("NameNode metadata: " + this.metaDir.getAbsolutePath());
        }
        
        public void registerDataNode(DataNode dataNode) {
            dataNodes.put(dataNode.getNodeId(), dataNode);
            System.out.println("DataNode registered: " + dataNode.getNodeId() + 
                             " at " + dataNode.getAddress() + ":" + dataNode.getPort());
        }
        
        public boolean writeFile(String fileName, byte[] data, String owner) {
            if (fileSystem.containsKey(fileName)) {
                System.out.println("File already exists: " + fileName);
                return false;
            }
            
            FileMetadata metadata = new FileMetadata(fileName, data.length, owner);
            List<HDFSBlock> blocks = createBlocks(data, fileName);
            
            // Distribute blocks to DataNodes
            for (HDFSBlock block : blocks) {
                List<DataNode> targetNodes = selectDataNodes(replicationFactor);
                if (targetNodes.size() < replicationFactor) {
                    System.out.println("Not enough DataNodes for replication");
                    return false;
                }
                
                boolean stored = false;
                for (DataNode node : targetNodes) {
                    if (node.storeBlock(block)) {
                        stored = true;
                    }
                }
                
                if (stored) {
                    blockLocations.put(block.getBlockId(), block);
                    metadata.addBlock(block.getBlockId());
                }
            }
            
            if (metadata.getBlockIds().isEmpty()) {
                System.err.println("NameNode: No blocks were stored for " + fileName + "; write failed");
                return false;
            }

            fileSystem.put(fileName, metadata);
            System.out.println("File written successfully: " + fileName + " (" + data.length + " bytes)");

            // Persist metadata to disk
            try {
                persistFileMetadata(metadata);
            } catch (Exception e) {
                System.err.println("NameNode: Failed to persist metadata for " + fileName + " - " + e.getMessage());
            }
            return true;
        }
        
        public byte[] readFile(String fileName) {
            FileMetadata metadata = fileSystem.get(fileName);
            if (metadata == null) {
                System.out.println("File not found: " + fileName);
                return null;
            }
            
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            
            for (String blockId : metadata.getBlockIds()) {
                HDFSBlock block = blockLocations.get(blockId);
                if (block != null) {
                    // Find a DataNode that has this block
                    for (String replica : block.getReplicas()) {
                        DataNode node = dataNodes.get(replica);
                        if (node != null && node.isActive()) {
                            HDFSBlock retrievedBlock = node.getBlock(blockId);
                            if (retrievedBlock != null) {
                                try {
                                    result.write(retrievedBlock.getData());
                                    break;
                                } catch (IOException e) {
                                    System.err.println("Error reading block: " + blockId);
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("File read successfully: " + fileName);
            return result.toByteArray();
        }
        
        public boolean deleteFile(String fileName) {
            FileMetadata metadata = fileSystem.remove(fileName);
            if (metadata == null) {
                return false;
            }
            
            // Delete all blocks
            for (String blockId : metadata.getBlockIds()) {
                HDFSBlock block = blockLocations.remove(blockId);
                if (block != null) {
                    for (String replica : block.getReplicas()) {
                        DataNode node = dataNodes.get(replica);
                        if (node != null) {
                            node.deleteBlock(blockId);
                        }
                    }
                }
            }
            
            System.out.println("File deleted successfully: " + fileName);

            // Remove metadata file
            try {
                removeFileMetadata(fileName);
            } catch (Exception e) {
                System.err.println("NameNode: Failed to remove metadata for " + fileName + " - " + e.getMessage());
            }
            return true;
        }

        private String sanitizeName(String fileName) {
            return fileName.replaceAll("[^a-zA-Z0-9._-]", "_");
        }

        private void persistFileMetadata(FileMetadata metadata) throws IOException {
            if (metaDir == null) return;
            String safe = sanitizeName(metadata.getFileName());
            File metaFile = new File(metaDir, safe + ".meta");
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(metaFile), StandardCharsets.UTF_8))) {
                bw.write("fileName:" + metadata.getFileName() + "\n");
                bw.write("owner:" + metadata.getOwner() + "\n");
                bw.write("size:" + metadata.getFileSize() + "\n");
                bw.write("timestamp:" + metadata.getTimestamp() + "\n");
                bw.write("blocks:\n");
                for (String b : metadata.getBlockIds()) bw.write(b + "\n");
            }
            System.out.println("NameNode: Persisted metadata -> " + metaFile.getAbsolutePath());
        }

        private void removeFileMetadata(String fileName) {
            if (metaDir == null) return;
            String safe = sanitizeName(fileName);
            File metaFile = new File(metaDir, safe + ".meta");
            if (metaFile.exists()) {
                if (metaFile.delete()) {
                    System.out.println("NameNode: Removed metadata file " + metaFile.getAbsolutePath());
                } else {
                    System.err.println("NameNode: Failed to delete metadata file " + metaFile.getAbsolutePath());
                }
            }
        }
        
        public List<String> listFiles() {
            return new ArrayList<>(fileSystem.keySet());
        }
        
        public FileMetadata getFileInfo(String fileName) {
            return fileSystem.get(fileName);
        }
        
        private List<HDFSBlock> createBlocks(byte[] data, String fileName) {
            List<HDFSBlock> blocks = new ArrayList<>();
            int offset = 0;
            int blockIndex = 0;
            
            while (offset < data.length) {
                int currentBlockSize = Math.min(blockSize, data.length - offset);
                byte[] blockData = Arrays.copyOfRange(data, offset, offset + currentBlockSize);
                
                String blockId = fileName + "_block_" + blockIndex + "_" + System.currentTimeMillis();
                HDFSBlock block = new HDFSBlock(blockId, blockData);
                blocks.add(block);
                
                offset += currentBlockSize;
                blockIndex++;
            }
            
            return blocks;
        }
        
        private List<DataNode> selectDataNodes(int count) {
            List<DataNode> activeNodes = dataNodes.values().stream()
                .filter(DataNode::isActive)
                .sorted((a, b) -> Long.compare(b.getFreeSpace(), a.getFreeSpace()))
                .limit(count)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            
            return activeNodes;
        }
        
        public Map<String, Object> getClusterStatus() {
            Map<String, Object> status = new HashMap<>();
            status.put("totalFiles", fileSystem.size());
            status.put("totalDataNodes", dataNodes.size());
            status.put("activeDataNodes", dataNodes.values().stream().mapToInt(n -> n.isActive() ? 1 : 0).sum());
            status.put("totalBlocks", blockLocations.size());
            
            long totalCapacity = dataNodes.values().stream().mapToLong(n -> n.getFreeSpace() + n.getUsedSpace()).sum();
            long usedSpace = dataNodes.values().stream().mapToLong(DataNode::getUsedSpace).sum();
            
            status.put("totalCapacityMB", totalCapacity / (1024 * 1024));
            status.put("usedSpaceMB", usedSpace / (1024 * 1024));
            status.put("freeSpaceMB", (totalCapacity - usedSpace) / (1024 * 1024));
            
            return status;
        }
        
        public List<DataNode> getDataNodes() {
            return new ArrayList<>(dataNodes.values());
        }
    }
    
    // ==================== UBER HDFS CLIENT ====================
    static class UberHDFSClient {
        private final NameNode nameNode;
        
        public UberHDFSClient(NameNode nameNode) {
            this.nameNode = nameNode;
        }
        
        public boolean storeRideData(String rideId, String riderName, String driverName, 
                                   String pickup, String destination, double fare) {
            String rideData = String.format("RIDE_DATA|%s|%s|%s|%s|%s|%.2f|%d\n", 
                                          rideId, riderName, driverName, pickup, destination, fare, System.currentTimeMillis());
            
            String fileName = "/uber/rides/" + rideId + ".txt";
            return nameNode.writeFile(fileName, rideData.getBytes(), "uber_system");
        }
        
        public boolean storeDriverData(String driverId, String name, String location, String vehicle) {
            String driverData = String.format("DRIVER_DATA|%s|%s|%s|%s|%d\n", 
                                            driverId, name, location, vehicle, System.currentTimeMillis());
            
            String fileName = "/uber/drivers/" + driverId + ".txt";
            return nameNode.writeFile(fileName, driverData.getBytes(), "uber_system");
        }
        
        public boolean storeGPSData(String driverId, double lat, double lon) {
            String gpsData = String.format("GPS_DATA|%s|%.6f|%.6f|%d\n", 
                                         driverId, lat, lon, System.currentTimeMillis());
            
            String fileName = "/uber/gps/" + driverId + "_" + System.currentTimeMillis() + ".txt";
            return nameNode.writeFile(fileName, gpsData.getBytes(), "uber_system");
        }
        
        public String getRideData(String rideId) {
            String fileName = "/uber/rides/" + rideId + ".txt";
            byte[] data = nameNode.readFile(fileName);
            return data != null ? new String(data) : null;
        }
        
        public String getDriverData(String driverId) {
            String fileName = "/uber/drivers/" + driverId + ".txt";
            byte[] data = nameNode.readFile(fileName);
            return data != null ? new String(data) : null;
        }
        
        public List<String> listAllRides() {
            return nameNode.listFiles().stream()
                .filter(f -> f.startsWith("/uber/rides/"))
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        }
        
        public List<String> listAllDrivers() {
            return nameNode.listFiles().stream()
                .filter(f -> f.startsWith("/uber/drivers/"))
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        }
        
        public NameNode getNameNode() {
            return nameNode;
        }
    }
    
    // ==================== MAIN HDFS SYSTEM ====================
    private final NameNode nameNode;
    private final List<DataNode> dataNodes;
    private final UberHDFSClient client;
    private final File basePath;
    
    public UberHDFS() {
        // Base path for persisted HDFS data
        this.basePath = new File("UberSystem" + File.separator + "hdfs_data");
        if (!basePath.exists()) basePath.mkdirs();

        // Prepare namenode metadata dir
        File nameNodeDir = new File(basePath, "namenode");
        if (!nameNodeDir.exists()) nameNodeDir.mkdirs();

        // Initialize HDFS with replication factor and block size (and meta dir)
        this.nameNode = new NameNode(nameNodeDir, 2, 1024);
        this.dataNodes = new ArrayList<>();

        // Create 3 DataNodes with on-disk storage
        File dn1 = new File(basePath, "datanode1"); dn1.mkdirs();
        File dn2 = new File(basePath, "datanode2"); dn2.mkdirs();
        File dn3 = new File(basePath, "datanode3"); dn3.mkdirs();

        dataNodes.add(new DataNode("datanode1", "localhost", 9001, 10 * 1024 * 1024, dn1)); // 10MB
        dataNodes.add(new DataNode("datanode2", "localhost", 9002, 10 * 1024 * 1024, dn2)); // 10MB
        dataNodes.add(new DataNode("datanode3", "localhost", 9003, 10 * 1024 * 1024, dn3)); // 10MB

        // Register DataNodes with NameNode
        for (DataNode node : dataNodes) {
            nameNode.registerDataNode(node);
        }

        this.client = new UberHDFSClient(nameNode);

        System.out.println("Uber HDFS initialized successfully! Data stored under: " + basePath.getAbsolutePath());
    }
    
    public UberHDFSClient getClient() {
        return client;
    }
    
    public NameNode getNameNode() {
        return nameNode;
    }
    
    public List<DataNode> getDataNodes() {
        return new ArrayList<>(dataNodes);
    }
    
    public void printClusterStatus() {
        System.out.println("\n=== UBER HDFS CLUSTER STATUS ===");
        Map<String, Object> status = nameNode.getClusterStatus();
        
        System.out.println("Files: " + status.get("totalFiles"));
        System.out.println("DataNodes: " + status.get("activeDataNodes") + "/" + status.get("totalDataNodes"));
        System.out.println("Blocks: " + status.get("totalBlocks"));
        System.out.println("Storage: " + status.get("usedSpaceMB") + "MB used / " + 
                         status.get("totalCapacityMB") + "MB total");
        
        System.out.println("\nDataNode Details:");
        for (DataNode node : dataNodes) {
            Map<String, Integer> summary = node.getBlockSummary();
            System.out.println("  " + node.getNodeId() + ": " + 
                             summary.get("totalBlocks") + " blocks, " +
                             summary.get("usedSpaceMB") + "MB used, " +
                             summary.get("freeSpaceMB") + "MB free");
        }
        System.out.println("================================\n");
    }
    
    // Test method
    public static void main(String[] args) {
        UberHDFS hdfs = new UberHDFS();
        UberHDFSClient client = hdfs.getClient();
        
        // Test storing Uber data
        System.out.println("Testing Uber HDFS...");
        
        // Store some ride data
        client.storeRideData("RIDE001", "John Doe", "Driver Smith", "Airport", "Hotel", 25.50);
        client.storeRideData("RIDE002", "Jane Smith", "Driver Johnson", "Mall", "Home", 15.75);
        
        // Store driver data
        client.storeDriverData("DRIVER001", "Smith", "Downtown", "Toyota Camry");
        client.storeDriverData("DRIVER002", "Johnson", "Airport", "Honda Civic");
        
        // Store GPS data
        client.storeGPSData("DRIVER001", 40.7128, -74.0060);
        client.storeGPSData("DRIVER002", 40.7589, -73.9851);
        
        // Print cluster status
        hdfs.printClusterStatus();
        
        // Read some data
        System.out.println("Reading ride data:");
        System.out.println(client.getRideData("RIDE001"));
        
        System.out.println("All rides: " + client.listAllRides());
        System.out.println("All drivers: " + client.listAllDrivers());
    }
}