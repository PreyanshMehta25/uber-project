import java.util.ArrayList;
import java.util.List;

public class ProcessNode {
    private final int id;
    private boolean isActive;
    private boolean isLeader;
    private final List<ProcessNode> allNodes;

    public ProcessNode(int id) {
        this.id = id;
        this.isActive = true;
        this.isLeader = false;
        this.allNodes = new ArrayList<>();
    }

    public void setAllNodes(List<ProcessNode> nodes) {
        this.allNodes.addAll(nodes);
    }

    public void holdElection() {
        if (!this.isActive) {
            System.out.println("Process " + id + " is down and cannot start an election.");
            return;
        }

        System.out.println("Process " + id + " is holding an election.");
        boolean hasHigherActiveNodes = false;

        for (ProcessNode node : allNodes) {
            if (node.getId() > this.id) {
                System.out.println("  -> [ELECTION] Message sent from Process " + id + " to Process " + node.getId());
                if (node.receiveElectionMessage(this.id)) {
                    hasHigherActiveNodes = true;
                }
            }
        }

        if (!hasHigherActiveNodes) {
            System.out.println("\n** Process " + id + " received no replies from higher nodes. **");
            declareAsLeader();
        }
    }

    public boolean receiveElectionMessage(int senderId) {
        if (!this.isActive) {
            System.out.println("  <- [NO RESPONSE] Process " + id + " is down.");
            return false;
        }

        System.out.println("  <- [OK] Message sent from Process " + id + " to Process " + senderId);

        new Thread(this::holdElection).start();

        return true;
    }

    public void declareAsLeader() {
        System.out.println(">Process " + id + " is now the new LEADER (Active Dispatcher)");
        this.isLeader = true;

        for (ProcessNode node : allNodes) {
            if (node.getId() != this.id) {
                node.receiveCoordinatorMessage(this.id);
            }
        }
    }

    public void receiveCoordinatorMessage(int leaderId) {
        if (!this.isActive) {
            return;
        }
        this.isLeader = false;
        System.out.println("    [COORDINATOR] Process " + id + " acknowledges new leader is Process " + leaderId);
    }

    public void checkForLeader() {
        if (!this.isActive) return;

        ProcessNode leader = findLeader();
        if (leader == null || !leader.isActive()) {
            System.out.println("\nProcess " + id + " detected that the Leader is down!");
            holdElection();
        } else {
            System.out.println("Process " + id + " confirms Leader " + leader.getId() + " is still active.");
        }
    }

    private ProcessNode findLeader() {
        for (ProcessNode node : allNodes) {
            if (node.isLeader()) {
                return node;
            }
        }
        return null;
    }

    public void shutdown() {
        System.out.println("Process " + id + " is shutting down.");
        this.isActive = false;
        this.isLeader = false;
    }

    public void startup() {
        System.out.println("Process " + id + " is starting up.");
        this.isActive = true;
        holdElection();
    }

    public int getId() {
        return id;
    }

    public boolean isActive() {
        return isActive;
    }

    public boolean isLeader() {
        return isLeader;
    }

    @Override
    public String toString() {
        return "Process " + id + " [Active: " + isActive + ", Leader: " + isLeader + "]";
    }
}