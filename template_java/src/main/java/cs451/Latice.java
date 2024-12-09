package cs451;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    // Algorithm variables for the proposer
    private AtomicBoolean active;
    private AtomicInteger ack_count;
    private AtomicInteger nack_count;
    private AtomicInteger active_proposal_number;
    private Set<Integer> proposed_value;

    // Algorithm variables for the acceptor
    private Set<Integer> accepted_value;

    public Latice(Host myHost, List<Host> hosts, PerfectLinks perfectLinks, String outputFilePath) {
        // Init the variables from the template
        this.myHost = myHost;
        this.hosts = hosts;
        this.F = (int) Math.floor((double) hosts.size() / 2);
        this.perfectLinks = perfectLinks;
        this.outputFilePath = outputFilePath;

        // Init the variables for the algorithm for the proposer
        this.active = new AtomicBoolean(false);
        this.ack_count = new AtomicInteger(0);
        this.nack_count = new AtomicInteger(0);
        this.active_proposal_number = new AtomicInteger(0);
        this.proposed_value = new HashSet<>();

        // Init the variables for the algorithm for the acceptor
        this.accepted_value = new HashSet<>();

    }

    // ----------------------------- BEB -----------------------------

    /**
     * Broadcast a message to all the hosts, except the current host
     * 
     * @param message The message to broadcast
     */
    public void beb_broadcast(Message message) {
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
        // Extract the content of the message
        String content = message.getContent();
        String[] parts = content.split(":");

        // Extract the message type
        String type = parts[0];
        LaticeMessageType messageType = LaticeMessageType.fromString(type);

        int proposal_nb;
        Set<Integer> value;
        switch (messageType) {
            case PROPOSE:
                // <proposal, Set proposed_value, Integer proposal_number>
                proposal_nb = Integer.parseInt(parts[2]);
                value = stringToSet(parts[1]);
                handle_proposal(senderHost, proposal_nb, value);
                break;
            case ACK:
                // <ack, Integer proposal_number>
                proposal_nb = Integer.parseInt(parts[1]);
                handle_ack(senderHost, proposal_nb);
                break;
            case NACK:
                // <nack, Integer proposal_number, Set value>
                proposal_nb = Integer.parseInt(parts[1]);
                value = stringToSet(parts[2]);
                handle_nack(senderHost, proposal_nb, value);
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
    public void handle_ack(Host senderHost, int proposal_number) {
        // upon reception of ⟨ack, Integer proposal_number⟩ such that proposal_number = active_proposal_numberi:
        // ack_counti ← ack_counti + 1
        if (proposal_number == active_proposal_number.get()) {
            ack_count.incrementAndGet();
        }
    }

    /**
     * Handle the reception of a propose message
     * 
     * @param senderHost      The host that sent the message
     * @param proposal_number The proposal number
     * @param value           The value proposed
     */
    public void handle_nack(Host senderHost, int proposal_number, Set<Integer> value) {
        // upon reception of ⟨nack, Integer proposal_number, Set value⟩ such that proposal_number = active_proposal_numberi:
        // proposed_value ← proposed_value ∪ value
        // nack_counti ← nack_counti + 1
        if (proposal_number == active_proposal_number.get()) {
            proposed_value.addAll(value);
            nack_count.incrementAndGet();
        }
    }

    public void handle_proposal(Host senderHost, int proposal_number, Set<Integer> value) {
        // upon reception of ⟨proposal, Set proposed_value, Integer proposal_number⟩ from proposer Pj such that accepted_valuei ⊆ proposed_value:
        // accepted_valuei ← proposed_value send ⟨ack, proposal_number⟩ to Pj
        // upon reception of ⟨proposal, Set proposed_value, Integer proposal_number⟩ from proposer Pj such that accepted_valuei ̸⊆ proposed_value:
        // accepted_valuei ← accepted_valuei ∪ proposed_value send ⟨nack, proposal_number, accepted_valuei⟩ to Pj

        // Check if the proposed value is included in the accepted value
        boolean included = true;
        for (Integer val : accepted_value) {
            if (!value.contains(val)) {
                included = false;
                break;
            }
        }

        Message message;
        if (included) {
            // accepted_valuei ← proposed_value
            accepted_value = value;

            // Create the message
            LaticeMessageType type = LaticeMessageType.ACK;
            String content = type.toString() + ":" + proposal_number;
            message = new Message(myHost, content, current_timestamp.incrementAndGet());
        } else {
            // accepted_valuei ← accepted_valuei ∪ proposed_value
            accepted_value.addAll(value);

            // Create the message
            LaticeMessageType type = LaticeMessageType.NACK;
            String content = type.toString() + ":" + proposal_number + ":" + setToString(accepted_value);
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
     */
    public void propose(Set<Integer> proposal) {
        this.proposed_value = proposal;
        this.active.set(true);
        this.active_proposal_number.incrementAndGet();
        this.ack_count.set(0);
        this.nack_count.set(0);

        // Create the message
        LaticeMessageType type = LaticeMessageType.PROPOSE;
        String content = type.toString() + ":" + setToString(proposal) + ":" + active_proposal_number.toString();
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
        if (nack_count.get() > 0 && ack_count.get() + nack_count.get() >= F + 1 && active.get()) {
            active_proposal_number.incrementAndGet();
            ack_count.set(0);
            nack_count.set(0);

            // Create the message
            LaticeMessageType type = LaticeMessageType.PROPOSE;
            String content = type.toString() + ":" + setToString(proposed_value) + ":" + active_proposal_number.toString();
            Message message = new Message(myHost, content, current_timestamp.incrementAndGet());

            // Broadcast the message
            beb_broadcast(message);
        }
    }

    /**
     * Verify thread that checks if the proposal should be decided
     */
    public void verifyThread2() {
        // upon ack_counti ≥ f + 1 and activei = true:
        // trigger decide(proposed_valuei) activei ← false
        if (ack_count.get() >= F + 1 && active.get()) {
            active.set(false);
            try {
                decide(proposed_value);
            } catch (Exception e) {
                e.printStackTrace();
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
        Files.write(Paths.get(this.outputFilePath), setToString(proposal, delimiter).getBytes(), StandardOpenOption.APPEND);
    }

}
