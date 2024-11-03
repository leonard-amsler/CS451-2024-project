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
import java.util.concurrent.ConcurrentHashMap;

public class Sender {

    // Map to keep track of which message batches are awaiting acknowledgment
    private Map<Integer, Long> messageState = new ConcurrentHashMap<>();

    // Congestion control parameters
    private static final double MAX_WINDOW_SIZE = Math.pow(2, 14); // Maximum window size (16384)
    private static final double MIN_WINDOW_SIZE = Math.pow(2, 0); // Minimum window size (1)
    private double windowSize = MIN_WINDOW_SIZE; // Initial window size

    private static final int MAX_TIMEOUT_MS = 1024; // Maximum timeout duration
    private static final int MIN_TIMEOUT_MS = 128; // Minimum timeout duration
    private int timeout_ms = MIN_TIMEOUT_MS; // Initial timeout duration

    // Variables for tracking acknowledgments and timeouts
    private volatile int ackCount = 0;
    private volatile int timeoutCount = 0;
    private volatile int nb_consec_min = 0;
    private volatile boolean running = true;

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

            // Thread for receiving ACKs
            Thread ackListenerThread = new Thread(() -> listenForAcks(socket));
            ackListenerThread.start();

            // Thread for resending unacknowledged messages periodically
            Thread resendThread = new Thread(() -> resendUnacknowledgedMessages(socket, receiverAddress, receiver.getPort(), myId, outputFilePath, m));
            resendThread.start();

            // Thread for adjusting the window size periodically
            Thread windowAdjustmentThread = new Thread(() -> adjustWindowSizePeriodically());
            windowAdjustmentThread.start();

            // Sliding window variables
            int seqNum = 1;
            while (seqNum <= m || !messageState.isEmpty()) {
                // Check if we can send more batches
                if (messageState.size() < windowSize && seqNum <= m) {
                    int max = Math.min(seqNum + Constants.BATCH_SIZE - 1, m);
                    List<Integer> seqNums = new ArrayList<>();
                    for (int i = seqNum; i <= max; i++) {
                        seqNums.add(i);
                    }
                    int batchNum = seqNum / Constants.BATCH_SIZE; // Batch num is zero-indexed
                    messageState.put(batchNum, System.currentTimeMillis()); // Add to messageState when sending
                    //System.out.println("Sending batch " + batchNum + " with seqnums " + seqNums.toString());
                    sendBatchMessage(socket, receiverAddress, receiver.getPort(), myId, seqNums, batchNum, outputFilePath, true);
                    seqNum += Constants.BATCH_SIZE;
                } else {
                    // Sleep briefly to allow for ACK processing and window adjustment
                    Thread.sleep(10);
                }
            }

            // Wait for all messages to be acknowledged
            while (!messageState.isEmpty()) {
                Thread.sleep(10);
            }

            // Stop the running threads
            running = false;
            ackListenerThread.join();
            resendThread.join();
            windowAdjustmentThread.join();

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
     * Thread for receiving ACKs from the receiver.
     * This thread continuously listens for ACKs and removes acknowledged batches from messageState.
     * @param socket the socket to listen on
     */
    private void listenForAcks(DatagramSocket socket) {
        try {
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            while (running) {
                try {
                    // Listen for an ACK
                    socket.receive(packet);
                    String receivedMessage = new String(packet.getData(), 0, packet.getLength());
                    // Format: "ack senderId batchNum"

                    // Check if the message is an ACK
                    if (receivedMessage.startsWith("ack")) {

                        String[] parts = receivedMessage.split(" ");
                        int batchNum = Integer.parseInt(parts[2]);

                        // Remove the batch from messageState
                        if (messageState.containsKey(batchNum)) {
                            messageState.remove(batchNum);
                            ackCount++;
                            //System.out.println("Received ACK for batch " + batchNum);
                        }
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
     * Thread for resending unacknowledged messages periodically.
     * This thread checks for timeouts every TIMEOUT_MS and resends unacknowledged batches.
     * @param socket the socket to resend messages on
     * @param receiverAddress the address of the receiver
     * @param port the port to resend messages on
     * @param myId the id of the sender
     * @param outputFilePath the path to the output file
     * @param m the total number of messages to send
     */
    private void resendUnacknowledgedMessages(DatagramSocket socket, InetAddress receiverAddress, int port, int myId, String outputFilePath, int m) {
        try {
            while (running) {
                // Sleep for the timeout duration before checking for timeouts
                Thread.sleep(timeout_ms);

                long currentTime = System.currentTimeMillis();

                for (Map.Entry<Integer, Long> entry : messageState.entrySet()) {
                    int batchNum = entry.getKey();
                    long sentTime = entry.getValue();

                    // Check if the batch has timed out
                    if (currentTime - sentTime >= timeout_ms) {
                        Integer max = Math.min((batchNum + 1) * Constants.BATCH_SIZE, m);
                        List<Integer> seqNums = new ArrayList<>();
                        for (int i = batchNum * Constants.BATCH_SIZE + 1; i <= max; i++) {
                            seqNums.add(i);
                        }
                        //System.out.println("Resending batch " + batchNum + " with seqnums " + seqNums.toString());
                        sendBatchMessage(socket, receiverAddress, port, myId, seqNums, batchNum, outputFilePath, false);

                        // Update the sent time
                        messageState.put(batchNum, currentTime);

                        timeoutCount++;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Thread for adjusting the window size periodically based on network performance.
     * This thread wakes up every TIMEOUT_MS and adjusts the window size based on ACKs and timeouts.
     */
    private void adjustWindowSizePeriodically() {
        try {
            while (running) {
                // Sleep for the timeout duration
                Thread.sleep(timeout_ms);

                // Adjust window size based on ACKs and timeouts
                // Between 0% and 40% ACKs, decrease window size
                // Between 40% and 60% ACKs, keep window size the same
                // Between 60% and 100% ACKs, increase window size
                double ackRate = (double) ackCount / (ackCount + timeoutCount);
                if (ackRate < 0.4) {
                    windowSize = Math.max(windowSize / 2, MIN_WINDOW_SIZE);
                    System.out.println("Decreasing window size to " + windowSize);
                } else if (ackRate > 0.6) {
                    windowSize = Math.min(windowSize * 2, MAX_WINDOW_SIZE);
                    System.out.println("Increasing window size to " + windowSize);
                } else {
                    System.out.println("Keeping window size at " + windowSize);
                }

                if (windowSize == MIN_WINDOW_SIZE) {
                    nb_consec_min++;
                } else {
                    nb_consec_min = 0;
                }

                if (nb_consec_min > 3) {
                    // increasing the timeout
                    nb_consec_min = 0;
                    timeout_ms = Math.min(timeout_ms * 2, MAX_TIMEOUT_MS);
                    System.out.println("Increasing timeout to " + timeout_ms);
                }

                // Reset counters for the next interval
                ackCount = 0;
                timeoutCount = 0;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
