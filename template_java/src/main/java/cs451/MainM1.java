package cs451;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;

import cs451.message.Message;
import cs451.parsers.Parser;

public class MainM1 {

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
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

    public static void MainM1(String[] args) throws InterruptedException {
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
        for (Host host: parser.hosts()) {
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
        int m = parser.config_m();              // Number of messages to send
        int myId = parser.myId();               // Current process ID

        Host myHost = parser.hosts().get(myId - 1);
        PerfectLinks perfectLinks = new PerfectLinks(parser.output(), myHost, parser.hosts());
        perfectLinks.start();

        Host receiverHost = parser.hosts().get(0);
        
        if (myHost.getId() != 1) {
            for (int i = 1; i <= m; i++) {
                perfectLinks.pl_broadcast(new Message(myHost, Integer.toString(i), i), receiverHost);
            }
        }
    }
}
