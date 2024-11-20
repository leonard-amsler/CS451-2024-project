package cs451;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Peer {

    private final Host myHost;
    private final String outputPath;
    private final List<Host> hosts;

    private DatagramSocket socket;
    private BufferedWriter logWriter;

    // Sequence number for messages this process broadcasts
    private int seqNum = 0;

    // Expected sequence numbers per sender
    private final ConcurrentHashMap<Integer, Integer> expectedSeqNums = new ConcurrentHashMap<>();

    // Buffers for out-of-order messages per sender
    private final ConcurrentHashMap<Integer, Map<Integer, String>> pendingMessages = new ConcurrentHashMap<>();

    // Set of messages that have been delivered
    private final Set<String> deliveredMessages = ConcurrentHashMap.newKeySet();

    // Set of messages that have been seen (to avoid re-broadcasting)
    private final Set<String> seenMessages = ConcurrentHashMap.newKeySet();

    public Peer(Host myHost, String outputPath) {
        this.myHost = myHost;
        this.outputPath = outputPath;
        this.hosts = new ArrayList<>();
    }

    public void start(int m, List<Host> hosts) {
        this.hosts.addAll(hosts);

        // Initialize expected sequence numbers for all processes
        for (Host host : hosts) {
            expectedSeqNums.put(host.getId(), 1);
        }

        try {
            // Bind socket to my port
            socket = new DatagramSocket(myHost.getPort());

            // Initialize log writer
            logWriter = new BufferedWriter(new FileWriter(outputPath));

            // Start receiver thread
            Thread receiverThread = new Thread(this::receiveMessages);
            receiverThread.start();

            // Start broadcasting messages
            Thread senderThread = new Thread(() -> broadcastMessages(m));
            senderThread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void broadcastMessages(int m) {
        for (int i = 1; i <= m; i++) {
            seqNum = i;
            // Create message: "senderId seqNum"
            String message = myHost.getId() + " " + seqNum;

            // Log broadcast event
            logEvent("b " + seqNum);

            // URB broadcast: send to all processes (including self)
            for (Host host : hosts) {
                sendMessage(host, message);
            }
        }
    }

    private void sendMessage(Host host, String message) {
        byte[] buffer = message.getBytes();
        try {
            InetAddress address = InetAddress.getByName(host.getIp());
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, host.getPort());
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void receiveMessages() {
        byte[] buffer = new byte[1024];
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);

                String received = new String(packet.getData(), 0, packet.getLength());
                handleReceivedMessage(received);
            } catch (SocketException e) {
                // Socket closed, exit the loop
                break;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleReceivedMessage(String message) {
        // Message format: "senderId seqNum"
        String[] parts = message.trim().split(" ");
        if (parts.length != 2) {
            // Invalid message format
            return;
        }

        int senderId;
        int seqNum;
        try {
            senderId = Integer.parseInt(parts[0]);
            seqNum = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            // Invalid numbers
            return;
        }

        String messageId = senderId + ":" + seqNum;

        // Check if we've already seen this message
        if (seenMessages.add(messageId)) {
            // First time seeing this message, re-broadcast it
            for (Host host : hosts) {
                if (host.getId() != myHost.getId()) {
                    sendMessage(host, message);
                }
            }
        }

        // Check if we've already delivered this message
        if (deliveredMessages.contains(messageId)) {
            // Already delivered, ignore
            return;
        }

        // FIFO ordering
        int expectedSeqNum = expectedSeqNums.get(senderId);

        if (seqNum == expectedSeqNum) {
            // Deliver the message
            deliverMessage(senderId, seqNum);

            // Increment expected sequence number
            expectedSeqNums.put(senderId, expectedSeqNum + 1);

            // Check if we can deliver any buffered messages
            checkPendingMessages(senderId);
        } else if (seqNum > expectedSeqNum) {
            // Message is ahead of expected, buffer it
            pendingMessages
                .computeIfAbsent(senderId, k -> new ConcurrentHashMap<>())
                .put(seqNum, message);
        }
        // If seqNum < expectedSeqNum, it's a duplicate or old message; ignore
    }

    private void deliverMessage(int senderId, int seqNum) {
        String messageId = senderId + ":" + seqNum;
        deliveredMessages.add(messageId);

        // Log delivery event
        logEvent("d " + senderId + " " + seqNum);
    }

    private void checkPendingMessages(int senderId) {
        Map<Integer, String> senderPendingMessages = pendingMessages.get(senderId);
        if (senderPendingMessages == null) {
            return;
        }

        int expectedSeqNum = expectedSeqNums.get(senderId);

        while (senderPendingMessages.containsKey(expectedSeqNum)) {
            // Deliver the buffered message
            deliverMessage(senderId, expectedSeqNum);

            // Remove it from the buffer
            senderPendingMessages.remove(expectedSeqNum);

            // Increment expected sequence number
            expectedSeqNum++;
            expectedSeqNums.put(senderId, expectedSeqNum);
        }
    }

    private synchronized void logEvent(String event) {
        try {
            logWriter.write(event);
            logWriter.newLine();
            logWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Call this method to stop the peer and clean up resources
    public void stop() {
        // Close the socket to stop receiving messages
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }

        // Close the log writer
        if (logWriter != null) {
            try {
                logWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
