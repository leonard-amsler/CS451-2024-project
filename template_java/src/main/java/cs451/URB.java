package cs451;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import cs451.message.Message;

public class URB {

    // Private variables from the template
    private final String outputFilePath;
    private final Host myHost;
    private final List<Host> hosts;

    private static final int MAX_PENDING_SIZE = (int) Math.pow(2, 11); // (2^11 = 2048)

    // Maps to keep track of the messages
    private Map<Integer, Integer> lastDelivered; // Last delivered message per initial host Id: <InitialHostId, Timestamp>
    private Map<Message, Set<Integer>> ack; // Set of hosts that we have received an ack from for a message: <Message, Set of HostIds>

    // PerfectLinks object
    private PerfectLinks perfectLinks;

    // Thread to check for deliveries
    private Thread checkDeliveryThread;
    
    public URB(String outputFilePath, Host myHost, List<Host> hosts) {
        this.outputFilePath = outputFilePath;
        this.myHost = myHost;
        this.hosts = hosts;

        this.lastDelivered = new ConcurrentHashMap<>();
        for (Host host : hosts) {
            lastDelivered.put(host.getId(), 0);
        }

        this.ack = new ConcurrentHashMap<>();

        this.perfectLinks = new PerfectLinks(outputFilePath, myHost, hosts, this);

        this.checkDeliveryThread = new Thread(() -> checkForDeliveries());
    }

    public void start() {
        System.out.println("Starting URB");

        // Start the perfect links
        perfectLinks.start();
        checkDeliveryThread.start();
    }

    public void stop() {
        System.out.println("Stopping URB");

        // Stop the perfect links
        perfectLinks.stop();
        checkDeliveryThread.interrupt();
    }

    // ------------------------- Best effort broadcast -------------------------

    public void beb_broadcast(Message message) {    

        // Send the message to all the hosts
        for (Host host : hosts) {
            if (host.getId() != myHost.getId()) {
                perfectLinks.pl_broadcast(message, host);
            }
        }
        
    }

    public void beb_deliver(Message message, Host senderHost) {

        //System.out.println("\nBEB Delivering message " + message.toString() + " from host " + senderHost.getId());

        // Extract message data
        int initialSenderHostId = message.getInitialSenderHostId();

        // Add to the ack set for the current host if we haven't already delivered the message
        if (lastDelivered.get(initialSenderHostId) < message.getTimestamp()) {
            // Add to ack
            ack.putIfAbsent(message, new HashSet<>());
            ack.get(message).add(senderHost.getId());
        }

        // Broadcast the ack
        beb_broadcast(message);
    }

    //------------------------ Uniform reliable broadcast ------------------------

    public void urb_broadcast(Message message) {

        while (ack.size() > MAX_PENDING_SIZE) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Initialize the ack set for the current message
        ack.put(message, new HashSet<>(List.of(myHost.getId())));

        // Send the message to all the hosts
        beb_broadcast(message);

        // Log the message
        try {
            log_broadcast(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ----------------------------- Helper functions -----------------------------

    private boolean canDeliver(Message message) {

        // Check that we have already delivered the message with the previous timestamp
        int currentTimestamp = message.getTimestamp();
        if (currentTimestamp != lastDelivered.get(message.getInitialSenderHostId()) + 1) {
            return false;
        }

        // Check that we have received acks from more than half of the hosts
        if (ack.get(message).size() < hosts.size() / 2) {
            return false;
        }

        return true;
    }

    private void checkForDeliveries() {

        while (true) {
            // Print useful information
            System.out.println("\nChecking for deliveries");
            System.out.println("Last delivered: " + lastDelivered.toString());
            System.out.println("Number of messages waiting for ack: " + ack.size());

            // Check for all the messages
            List <Message> messagesCopy = new ArrayList<>(ack.keySet());

            // Group the messages by initial sender host id
            Map <Integer, List<Message>> messagesByHost = new ConcurrentHashMap<>();
            messagesCopy.forEach(message -> {
                messagesByHost.putIfAbsent(message.getInitialSenderHostId(), new ArrayList<>());
                messagesByHost.get(message.getInitialSenderHostId()).add(message);
            });

            // Sort the messages by timestamp
            // The goal is to be able to deliver also the messages that were received out of order, after delivering the missing ones
            messagesByHost.forEach((hostId, messages) -> {
                messages.sort((m1, m2) -> m1.getTimestamp() - m2.getTimestamp());
            });

            for (int hostId : messagesByHost.keySet()) {
                for (Message message : messagesByHost.get(hostId)) {
                    if (canDeliver(message)) {
                        // Deliver the message
                        lastDelivered.put(message.getInitialSenderHostId(), message.getTimestamp());

                        // Remove the message from the ack set
                        ack.remove(message);

                        // Log the message
                        try {
                            log_deliver(message);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        // The messages are sorted, so if we can't deliver the first one, we can't deliver the rest
                        break;
                    }
                }
            }

            // Sleep for a bit
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // ------------------------------- Log messages -------------------------------

    public synchronized void log_broadcast(Message message) throws Exception {
        StringBuilder log = new StringBuilder();
        log.append("b").append(" ");
        log.append(message.getContent()).append("\n");
        Files.write(Paths.get(outputFilePath), log.toString().getBytes(), StandardOpenOption.APPEND);
    }

    public synchronized void log_deliver(Message message) throws Exception {
        StringBuilder log = new StringBuilder();
        log.append("d").append(" ");
        log.append(message.getInitialSenderHostId()).append(" ");
        log.append(message.getContent()).append("\n");
        Files.write(Paths.get(outputFilePath), log.toString().getBytes(), StandardOpenOption.APPEND);
    }

}
