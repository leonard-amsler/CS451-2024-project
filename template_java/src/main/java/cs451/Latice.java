package cs451;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import cs451.message.LaticeMessageType;
import cs451.message.Message;

public class Latice {

    // Private variables from the template
    private final Host myHost;
    private final List<Host> hosts;
    private final int F;
    private PerfectLinks perfectLinks;
    private final String outputFilePath;

    // Timestamp
    private AtomicInteger current_timestamp;

    private AtomicInteger current_round;
    private Integer nb_rounds;

    // Algorithm variables
    private Map<Integer, AtomicBoolean> active = new ConcurrentHashMap<>(); // <round, active>
    private Map<Integer, AtomicInteger> ack_count = new ConcurrentHashMap<>(); // <round, ack_count>
    private Map<Integer, AtomicInteger> nack_count = new ConcurrentHashMap<>(); // <round, nack_count>
    private Map<Integer, AtomicInteger> active_proposal_number = new ConcurrentHashMap<>(); // <round, active_proposal_number>
    private Map<Integer, Set<Integer>> proposed_value = new ConcurrentHashMap<>(); // <round, proposed_value>
    private Map<Integer, Set<Integer>> accepted_value = new ConcurrentHashMap<>(); // <round, accepted_value>

    // Threads
    private Thread verifyThread1;
    private Thread verifyThread2;

    public Latice(Host myHost, List<Host> hosts, String outputFilePath, int nb_rounds) {
        // Init the variables from the template
        this.myHost = myHost;
        this.hosts = hosts;
        this.F = (int) Math.floor((double) hosts.size() / 2);
        this.nb_rounds = nb_rounds;

        // PerfectLinks object
        this.outputFilePath = outputFilePath;
        this.perfectLinks = new PerfectLinks(outputFilePath, myHost, hosts, this);

        // Init the variables for the algorithm for the proposer
        this.current_round = new AtomicInteger(0);
        this.active = new ConcurrentHashMap<>();
        this.ack_count = new ConcurrentHashMap<>();
        this.nack_count = new ConcurrentHashMap<>();
        this.active_proposal_number = new ConcurrentHashMap<>();
        this.proposed_value = new ConcurrentHashMap<>();
        this.accepted_value = new ConcurrentHashMap<>();

        // Add the default values for all the rounds
        for (int i = 0; i < nb_rounds; i++) {
            this.active.put(i, new AtomicBoolean(false));
            this.ack_count.put(i, new AtomicInteger(0));
            this.nack_count.put(i, new AtomicInteger(0));
            this.active_proposal_number.put(i, new AtomicInteger(0));
            this.proposed_value.put(i, new HashSet<>());
            this.accepted_value.put(i, new HashSet<>());
        }

        // Timestamp
        this.current_timestamp = new AtomicInteger(0);

        // Init the threads
        this.verifyThread1 = new Thread(() -> verifyThread1());
        this.verifyThread2 = new Thread(() -> verifyThread2());

    }

    public void start() {
        // Start the perfect links
        perfectLinks.start();

        // Start the threads
        this.verifyThread1.start();
        this.verifyThread2.start();
    }

    public void stop() {
        // Stop the perfect links
        perfectLinks.stop();

        // Stop the threads
        this.verifyThread1.interrupt();
        this.verifyThread2.interrupt();
    }

    // ----------------------------- BEB -----------------------------

    /**
     * Broadcast a message to all the hosts, except the current host
     * 
     * @param message The message to broadcast
     */
    public void beb_broadcast(Message message) {
        System.out.println("BEB Broadcast: " + message.getContent());
        for (Host host : hosts) {
            if (host.getId() != myHost.getId()) {
                perfectLinks.pl_broadcast(message, host);
            }
        }
    }

    /**
     * Deliver a message to the algorithm
     * 
     * @param message    The message to deliver
     * @param senderHost The host that sent the message
     */
    public void beb_deliver(Message message, Host senderHost) {
        System.out.println("BEB Deliver: " + message.getContent());

        // Extract the content of the message
        String content = message.getContent();
        String[] parts = content.split("&");

        // Extract the message type
        String type = parts[0];
        LaticeMessageType messageType = LaticeMessageType.fromString(type);

        // Round
        int round = Integer.parseInt(parts[1]);
        int proposal_nb;
        Set<Integer> value;
        switch (messageType) {
            case PROPOSE:
                // <proposal, Set proposed_value, Integer proposal_number>
                proposal_nb = Integer.parseInt(parts[3]);
                value = stringToSet(parts[2]);
                handle_proposal(senderHost, round, proposal_nb, value);
                break;
            case ACK:
                // <ack, Integer proposal_number>
                proposal_nb = Integer.parseInt(parts[2]);
                handle_ack(senderHost, round, proposal_nb);
                break;
            case NACK:
                // <nack, Integer proposal_number, Set value>
                proposal_nb = Integer.parseInt(parts[2]);
                value = stringToSet(parts[3]);
                handle_nack(senderHost, round, proposal_nb, value);
                break;
            default:
                throw new IllegalArgumentException("Unknown message type: " + messageType);
        }
    }

    /**
     * Handle the reception of a propose message
     * 
     * @param senderHost      The host that sent the message
     * @param proposal_number The proposal number
     */
    public void handle_ack(Host senderHost, int round, int proposal_number) {
        // upon reception of ⟨ack, Integer proposal_number⟩ such that proposal_number = active_proposal_numberi:
        // ack_counti ← ack_counti + 1
        if (proposal_number == active_proposal_number.get(round).get()) {
            ack_count.get(round).incrementAndGet();
        }
    }

    /**
     * Handle the reception of a propose message
     * 
     * @param senderHost      The host that sent the message
     * @param proposal_number The proposal number
     * @param value           The value proposed
     */
    public void handle_nack(Host senderHost, int round, int proposal_number, Set<Integer> value) {
        // upon reception of ⟨nack, Integer proposal_number, Set value⟩ such that proposal_number = active_proposal_numberi:
        // proposed_value ← proposed_value ∪ value
        // nack_counti ← nack_counti + 1
        if (proposal_number == active_proposal_number.get(round).get()) {
            proposed_value.get(round).addAll(value);
            nack_count.get(round).incrementAndGet();
        }
    }

    public void handle_proposal(Host senderHost, int round, int proposal_number, Set<Integer> value) {
        // upon reception of ⟨proposal, Set proposed_value, Integer proposal_number⟩ from proposer Pj such that accepted_valuei ⊆ proposed_value:
        // accepted_valuei ← proposed_value send ⟨ack, proposal_number⟩ to Pj
        // upon reception of ⟨proposal, Set proposed_value, Integer proposal_number⟩ from proposer Pj such that accepted_valuei ̸⊆ proposed_value:
        // accepted_valuei ← accepted_valuei ∪ proposed_value send ⟨nack, proposal_number, accepted_valuei⟩ to Pj

        // Check if the proposed value is included in the accepted value
        boolean included = true;
        for (Integer val : accepted_value.get(round)) {
            if (!value.contains(val)) {
                included = false;
                break;
            }
        }

        Message message;
        if (included) {
            // accepted_valuei ← proposed_value
            accepted_value.put(round, value);

            // Create the message
            LaticeMessageType type = LaticeMessageType.ACK;
            String content = type.toString() + "&" + round + "&" + proposal_number;
            message = new Message(myHost, content, current_timestamp.incrementAndGet());
        } else {
            // accepted_valuei ← accepted_valuei ∪ proposed_value
            accepted_value.get(round).addAll(value);

            // Create the message
            LaticeMessageType type = LaticeMessageType.NACK;
            String content = type.toString() + "&" + round + "&" + proposal_number + "&" + setToString(accepted_value.get(round));
            message = new Message(myHost, content, current_timestamp.incrementAndGet());
        }

        // Send the message to the sender
        perfectLinks.pl_broadcast(message, senderHost);
    }

    // ----------------------------- Latice -----------------------------

    /**
     * Propose a new value
     * 
     * @param proposal The proposed value
     * @param round    The round number
     */
    public void propose(Set<Integer> proposal, int round) {
        //  Verify that there is no active rounds
        boolean active_round = true;
        while (active_round) {
            active_round = false;
            for (int r = 0; r < nb_rounds; r++) {
                if (active.get(r).get()) {
                    active_round = true;
                    break;
                }
            }
            if (active_round) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // Set the proposed value for the round
        this.proposed_value.put(round, proposal);
        this.active.get(round).set(true);
        this.active_proposal_number.get(round).getAndIncrement();
        this.ack_count.get(round).set(0);
        this.nack_count.get(round).set(0);

        // Create the message
        LaticeMessageType type = LaticeMessageType.PROPOSE;
        String content = type.toString() + "&" + round + "&" + setToString(proposal) + "&" + active_proposal_number.get(round).toString();
        Message message = new Message(myHost, content, current_timestamp.incrementAndGet());

        // Broadcast the message
        beb_broadcast(message);
    }

    /**
     * Verify thread that checks if a new proposal should be sent
     */
    public void verifyThread1() {
        // upon nack_counti > 0 and ack_counti + nack_counti ≥ f + 1 and activei = true:
        // active_proposal_numberi ← active_proposal_numberi + 1
        // ack_counti ← 0
        // nack_counti ← 0
        // trigger beb.broadcast(⟨proposal, proposed_valuei, active_proposal_numberi⟩)
        while (true) {
            // Sleep for a while
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }

            for (int r = 0; r < nb_rounds; r++) {
                if (nack_count.get(r).get() > 0 && ack_count.get(r).get() + nack_count.get(r).get() >= F + 1 && active.get(r).get()) {
                    active_proposal_number.get(r).incrementAndGet();
                    ack_count.get(r).set(0);
                    nack_count.get(r).set(0);

                    // Create the message
                    LaticeMessageType type = LaticeMessageType.PROPOSE;
                    String content = type.toString() + "&" + r + "&" + setToString(proposed_value.get(r)) + "&" + active_proposal_number.get(r).toString();
                    Message message = new Message(myHost, content, current_timestamp.incrementAndGet());

                    // Broadcast the message
                    beb_broadcast(message);
                }
            }
        }
    }

    /**
     * Verify thread that checks if the proposal should be decided
     */
    public void verifyThread2() {
        // upon ack_counti ≥ f + 1 and activei = true:
        // trigger decide(proposed_valuei) activei ← false
        while (true) {
            // Sleep for a while
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }

            for (int r = 0; r < nb_rounds; r++) {
                if (ack_count.get(r).get() >= F + 1 && active.get(r).get()) {
                    active.get(r).set(false);
                    try {
                        decide(proposed_value.get(r));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    // ----------------------------- UTILS -----------------------------

    /**
     * Convert a set of integers to a string
     * 
     * @param set The set of integers
     * @return The string representation of the set
     */
    private String setToString(Set<Integer> set) {
        Character delimiter = ',';
        return setToString(set, delimiter);
    }

    /**
     * Convert a set of integers to a string
     * 
     * @param set       The set of integers
     * @param delimiter The delimiter to use
     * @return The string representation of the set
     */
    private String setToString(Set<Integer> set, Character delimiter) {
        Set<String> stringSet = new HashSet<>();
        for (Integer value : set) {
            stringSet.add(value.toString());
        }
        return String.join(delimiter.toString(), stringSet);
    }

    /**
     * Convert a string to a set of integers
     * 
     * @param string The string to convert
     * @return The set of integers
     */
    private Set<Integer> stringToSet(String string) {
        Set<Integer> set = new HashSet<>();
        String[] values = string.split(",");
        for (String value : values) {
            set.add(Integer.parseInt(value));
        }
        return set;
    }

    /**
     * Print the decided proposal
     * 
     * @param proposal The decided proposal
     */
    private void decide(Set<Integer> proposal) throws Exception {
        Character delimiter = ' ';
        Files.write(Paths.get(this.outputFilePath), (setToString(proposal, delimiter) + "\n").getBytes(), StandardOpenOption.APPEND);
    }

}
