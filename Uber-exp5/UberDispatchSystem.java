import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;

public class UberDispatchSystem {

    private final List<ProcessNode> nodes;

    public UberDispatchSystem(int numberOfNodes) {
        nodes = new ArrayList<>();
        for (int i = 1; i <= numberOfNodes; i++) {
            nodes.add(new ProcessNode(i));
        }

        for (ProcessNode node : nodes) {
            node.setAllNodes(nodes);
        }

        System.out.println(numberOfNodes + " dispatch servers created.");
        System.out.println("Setting initial leader...");
        nodes.get(numberOfNodes - 1).declareAsLeader();
        printSystemStatus();
    }

    public void printSystemStatus() {
        System.out.println("\nCurrent System Status");
        for (ProcessNode node : nodes) {
            System.out.println("  " + node);
        }
    }

    public Optional<ProcessNode> getNodeById(int id) {
        return nodes.stream().filter(n -> n.getId() == id).findFirst();
    }

    public void ensureLeaderExists() throws InterruptedException {
        Optional<ProcessNode> currentLeader = nodes.stream().filter(ProcessNode::isLeader).findFirst();

        if (currentLeader.isEmpty() || !currentLeader.get().isActive()) {
            System.out.println("\nSystem detected leader has failed. Initiating automatic re-election...");
            Thread.sleep(500);

            Optional<ProcessNode> newCandidate = nodes.stream()
                .filter(ProcessNode::isActive)
                .max(Comparator.comparingInt(ProcessNode::getId));
            
            if (newCandidate.isPresent()) {
                newCandidate.get().holdElection();
            } else {
                System.out.println("No active processes available to become leader.");
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        UberDispatchSystem uberSystem = new UberDispatchSystem(5);
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\nEnter a command:");
            System.out.println("  status");
            System.out.println("  down <id>");
            System.out.println("  up <id>");
            System.out.println("  check <id>");
            System.out.println("  exit");
            System.out.print("> ");

            String[] input = scanner.nextLine().trim().split("\\s+");
            String command = input[0].toLowerCase();

            try {
                switch (command) {
                    case "status":
                        uberSystem.printSystemStatus();
                        break;
                    case "down":
                        int downId = Integer.parseInt(input[1]);
                        uberSystem.getNodeById(downId).ifPresent(ProcessNode::shutdown);
                        break;
                    case "up":
                        int upId = Integer.parseInt(input[1]);
                        uberSystem.getNodeById(upId).ifPresent(ProcessNode::startup);
                        break;
                    case "check":
                        int checkId = Integer.parseInt(input[1]);
                        uberSystem.getNodeById(checkId).ifPresent(ProcessNode::checkForLeader);
                        break;
                    case "exit":
                        System.out.println("Exiting simulation.");
                        scanner.close();
                        System.exit(0);
                    default:
                        System.out.println("Invalid command.");
                }

                Thread.sleep(1000);
                uberSystem.ensureLeaderExists();

            } catch (Exception e) {
                System.out.println("Error processing command. Please try again. Usage: command <id>");
            }
        }
    }
}