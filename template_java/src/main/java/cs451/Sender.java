package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Sender {

    private static final int TIMEOUT_MS = 100; // Timeout in milliseconds (0.1 seconds)

    // Map to keep track of which messages have been acknowledged
    private Map<Integer, Boolean> ackReceived = new ConcurrentHashMap<>();

    /**
     * Send m messages to the receiver with the given receiverId
     * @param receiverId the id of the receiver
     * @param m the number of messages to send
     * @param hosts the list of hosts
     * @param outputFilePath the path to the output file
     * @param myId the id of the sender
     */
    public void sendMessages(int receiverId, int m, List<Host> hosts, String outputFilePath, int myId) {
        try (DatagramSocket socket = new DatagramSocket()) {
            Host receiver = hosts.get(receiverId - 1); // Get the receiver host
            InetAddress receiverAddress = InetAddress.getByName(receiver.getIp());

            // Initialize the map for all messages as not acknowledged yet
            for (int seqNum = 1; seqNum <= m; seqNum++) {
                ackReceived.put(seqNum, false);
            }

            // Start two threads:
            // 1. One for receiving ACKs
            Thread ackListenerThread = new Thread(() -> listenForAcks(socket, receiverAddress, receiver.getPort()));
            ackListenerThread.start();

            // 2. One for resending unacknowledged messages periodically
            Thread resendThread = new Thread(() -> resendUnacknowledgedMessages(socket, receiverAddress, receiver.getPort(), m, myId, outputFilePath));
            resendThread.start();

            // Send the messages initially
            for (int seqNum = 1; seqNum <= m; seqNum++) {
                sendMessage(socket, receiverAddress, receiver.getPort(), myId, seqNum, outputFilePath, true);
            }

            // Wait for both threads to finish
            ackListenerThread.join();
            resendThread.join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Send a message with the given sequence number to the receiver
     * @param socket the socket to send the message on
     * @param receiverAddress the address of the receiver
     * @param port the port to send the message on
     * @param myId the id of the sender
     * @param seqNum the sequence number of the message
     * @param outputFilePath the path to the output file
     * @param shouldWrite whether to write the send event to the output file
     * @throws Exception if an error occurs while sending the message
     */
    private void sendMessage(DatagramSocket socket, InetAddress receiverAddress, int port, int myId, int seqNum, String outputFilePath, Boolean shouldWrite) throws Exception {
        String message = myId + " " + seqNum;
        byte[] buf = message.getBytes();

        DatagramPacket packet = new DatagramPacket(buf, buf.length, receiverAddress, port);
        socket.send(packet);

        // Log the send event
        if (shouldWrite) {
            String log = "b " + seqNum + "\n";
            if (!Files.exists(Paths.get(outputFilePath))) {
                Files.createFile(Paths.get(outputFilePath));
            }
            Files.write(Paths.get(outputFilePath), log.getBytes(), StandardOpenOption.APPEND);
        }
    }

    /**
     * Thread 1: Listen for ACKs from the receiver.
     * This thread continuously listens for ACKs and marks the messages as acknowledged.
     * @param socket the socket to listen on
     * @param receiverAddress the address of the receiver
     * @param port the port to listen on
     */
    private void listenForAcks(DatagramSocket socket, InetAddress receiverAddress, int port) {
        try {
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            while (ackReceived.containsValue(false)) { // Keep running until all messages are acknowledged
                int remainingAcks = ackReceived.values().stream().mapToInt(value -> value ? 0 : 1).sum();
                System.out.println("[Sender] Still waiting for " + remainingAcks + " ACKs...");
                try {
                    // Listen for an ACK
                    socket.receive(packet);
                    String receivedMessage = new String(packet.getData(), 0, packet.getLength());

                    // Check if the message is an ACK
                    if (receivedMessage.startsWith("ack")) {
                        String[] parts = receivedMessage.split(" ");
                        int seqNum = Integer.parseInt(parts[2]);

                        // Mark the message as acknowledged
                        ackReceived.put(seqNum, true);
                    }
                } catch (Exception e) {
                    // Handle exception (could be timeout or other network issues)
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Thread 2: Resend unacknowledged messages periodically.
     * This thread resends all messages that haven't been acknowledged every TIMEOUT_MS.
     * @param socket the socket to resend messages on
     * @param receiverAddress the address of the receiver
     * @param port the port to resend messages on
     * @param m the number of messages to send
     * @param myId the id of the sender
     * @param outputFilePath the path to the output file
     */
    private void resendUnacknowledgedMessages(DatagramSocket socket, InetAddress receiverAddress, int port, int m, int myId, String outputFilePath) {
        try {
            while (ackReceived.containsValue(false)) { // Keep running until all messages are acknowledged
                // Check for unacknowledged messages and resend them
                for (int seqNum = 1; seqNum <= m; seqNum++) {
                    if (!ackReceived.get(seqNum)) {
                        // Resend the message if it hasn't been acknowledged
                        System.out.println("[Sender] Resending message " + seqNum + " to receiver...");
                        sendMessage(socket, receiverAddress, port, myId, seqNum, outputFilePath, false);
                    }
                }

                // Sleep for the timeout duration before checking again
                Thread.sleep(TIMEOUT_MS);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
