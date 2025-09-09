import java.util.concurrent.ConcurrentHashMap;

public class LamportLock {
    private final ConcurrentHashMap<String, Integer> locks = new ConcurrentHashMap<>();
    public synchronized boolean acquire(String resource, int timestamp) {
        if (!locks.containsKey(resource) || timestamp < locks.get(resource)) {
            locks.put(resource, timestamp);
            return true;
        }
        return false;
    }
    public synchronized void release(String resource) {
        locks.remove(resource);
    }
}
