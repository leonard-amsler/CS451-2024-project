package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class Sender {

    // Set to keep track of which messages must be acknolwedged
    private Map<Integer, Boolean> messageState = new ConcurrentHashMap<>();

    /**
     * Send m messages to the receiver with the given receiverId
     * @param receiverId the id of the receiver
     * @param m the number of messages to send
     * @param hosts the list of hosts
     * @param outputFilePath the path to the output file
     * @param myId the id of the sender
     */
    public void sendMessages(int receiverId, int m, List<Host> hosts, String outputFilePath, int myId) {
        // Delete & recreate the output file
        try {
            Files.deleteIfExists(Paths.get(outputFilePath));
            Files.createFile(Paths.get(outputFilePath));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (DatagramSocket socket = new DatagramSocket()) {
            Host receiver = hosts.get(receiverId - 1); // Get the receiver host
            InetAddress receiverAddress = InetAddress.getByName(receiver.getIp());

            // Initialize the map for all messages as not acknowledged yet
            for (int seqNum = 1; seqNum <= m; seqNum += Constants.BATCH_SIZE) {
                int batchNum = seqNum/Constants.BATCH_SIZE; // Batch num is zero-indexed
                messageState.put(batchNum, false); // One entry per batch
            }

            // 1. Thread for receiving ACKs
            Thread ackListenerThread = new Thread(() -> listenForAcks(socket, receiverAddress, receiver.getPort()));
            ackListenerThread.start();

            // 2. Thread for resending unacknowledged messages periodically
            Thread resendThread = new Thread(() -> resendUnacknowledgedMessages(socket, receiverAddress, receiver.getPort(), m, myId, outputFilePath));
            resendThread.start();

            // Send the messages initially
            for (int seqNum = 1; seqNum <= m; seqNum += Constants.BATCH_SIZE) {
                List<Integer> seqNums = new ArrayList<>();
                int max = Math.min(seqNum + Constants.BATCH_SIZE - 1 , m);
                for (int i = seqNum; i <= max; i++) {
                    seqNums.add(i);
                }
                if (seqNums.size() != 8) {
                    System.out.println("Sending batch with " + seqNums.size() + " messages");
                    System.out.println("Batch num: " + seqNum/Constants.BATCH_SIZE);
                    System.out.println("Seqnums: " + seqNums.toString());
                }
                int batchNum = seqNum/Constants.BATCH_SIZE;
                sendBatchMessage(socket, receiverAddress, receiver.getPort(), myId, seqNums, batchNum, outputFilePath, true);
            }

            // Wait for both threads to finish
            ackListenerThread.join();
            resendThread.join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends a batch of messages via a DatagramSocket to a specified receiver address and port.
     * The message format is "senderId batchNum seqNum1;seqNum2;seqNum3;..."
     * Optionally logs the send event to a specified output file.
     *
     * @param socket           the DatagramSocket used to send the message
     * @param receiverAddress  the InetAddress of the receiver
     * @param port             the port number of the receiver
     * @param myId             the sender's ID
     * @param seqNums          the list of sequence numbers to send
     * @param batchNum         the batch number of the message
     * @param outputFilePath   the file path where the log should be written
     * @param shouldWrite      a boolean flag indicating whether to log the send event
     * @throws Exception       if an I/O error occurs
     */
    private void sendBatchMessage(DatagramSocket socket, InetAddress receiverAddress, int port, int myId, List<Integer> seqNums, Integer batchNum, String outputFilePath, Boolean shouldWrite) throws Exception {
        
        StringBuilder message = new StringBuilder(); 
        message.append(myId).append(" ");
        message.append(batchNum).append(" ");

        for (int seqNum : seqNums) {
            message.append(seqNum).append(";");
        }

        // Remove last ";"
        message.deleteCharAt(message.length() - 1);

        byte[] buf = message.toString().getBytes();

        DatagramPacket packet = new DatagramPacket(buf, buf.length, receiverAddress, port);
        socket.send(packet);

        // Log the send event
        if (shouldWrite) {
            StringBuilder log = new StringBuilder();
            for (int seqNum : seqNums) {
                log.append("b ").append(seqNum).append("\n");
            }
            Files.write(Paths.get(outputFilePath), log.toString().getBytes(), StandardOpenOption.APPEND);
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

            while (messageState.containsValue(false)) { // Keep running until all batches are acknowledged
                try {
                    // Listen for an ACK
                    socket.receive(packet);
                    String receivedMessage = new String(packet.getData(), 0, packet.getLength());
                    // Format: "ack senderId batchNum"

                    // Check if the message is an ACK
                    if (receivedMessage.startsWith("ack")) {
                        
                        String[] parts = receivedMessage.split(" ");
                        int batchNum = Integer.parseInt(parts[2]);

                        // Mark the batch as acknowledged
                        messageState.put(batchNum, true);
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
            while (messageState.containsValue(false)) { // Keep running until all batches are acknowledged
                // Check for unacknowledged messages and resend them
                for (int batchNum : messageState.keySet()) {
                    if (!messageState.get(batchNum)) {
                        Integer max = Math.min((batchNum + 1) * Constants.BATCH_SIZE, m); // batch 0 => max = 8
                        List<Integer> seqNums = new ArrayList<>();
                        for (int i = batchNum * Constants.BATCH_SIZE + 1; i <= max; i++) {
                            seqNums.add(i);
                        }
                        if (seqNums.size() != 8) {
                            System.out.println("Received batch with " + seqNums.size() + " messages");
                            System.out.println("Resending batch " + batchNum);
                        }
                        sendBatchMessage(socket, receiverAddress, port, myId, seqNums, batchNum, outputFilePath, false);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
