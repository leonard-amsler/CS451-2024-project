package cs451;

import java.util.List;
import java.util.Set;
import cs451.parsers.Parser;

public class Main {

    private static void handleSignal() {
        // immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        // write/flush output file if necessary
        System.out.println("Writing output.");
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host : parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config_path() + "\n");

        // Load config values
        int p = parser.config_p();
        int vs = parser.config_vs();
        int ds = parser.config_ds();
        List<Set<Integer>> proposals = parser.config_proposals();
        int myId = parser.myId();
        Host myHost = parser.hosts().get(myId - 1);

        Latice latice = new Latice(myHost, parser.hosts(), parser.output(), parser.config_p());
        latice.start();

        for (int i = 0; i < p; i++) {
            latice.propose(proposals.get(i), i);
        }

        while (true) {
            Thread.sleep(1000);
        }
    }
}
