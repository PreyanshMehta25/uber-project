public class LamportClock {
    private int time = 0;
    public synchronized int tick() {
        time++;
        return time;
    }
    public synchronized int update(int received) {
        time = Math.max(time, received) + 1;
        return time;
    }
    public synchronized int getTime() {
        return time;
    }
}
