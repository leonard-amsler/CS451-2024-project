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
import java.util.concurrent.atomic.AtomicInteger;

import cs451.message.Message;
import cs451.message.MessageParser;
import cs451.message.MessageType;
import cs451.utils.Pair;

public class PerfectLinks {

    // Map to keep track of which message batches are awaiting acknowledgment
    private Map<Message, Long> messageState = new ConcurrentHashMap<>();

    // Map to keep track of which messages have been received already
    private Map<Message, Boolean> messagesReceived = new ConcurrentHashMap<>();

    // Congestion control parameters
    private static final double MAX_WINDOW_SIZE = Math.pow(2, 14); // Maximum window size (16384)
    private static final double MIN_WINDOW_SIZE = Math.pow(2, 0); // Minimum window size (1)
    private double windowSize =Math.pow(2, 5); // Initial window size
    private static final int MAX_TIMEOUT_MS = 1024; // Maximum timeout duration
    private static final int MIN_TIMEOUT_MS = 128; // Minimum timeout duration
    private int timeout_ms = MIN_TIMEOUT_MS; // Initial timeout duration

    // Variables for tracking acknowledgments and timeouts
    private AtomicInteger ackCount = new AtomicInteger(0);
    private AtomicInteger nb_consec_min = new AtomicInteger(0);
    private AtomicInteger timeoutCount = new AtomicInteger(0);

    // Batch number generator for new messages
    private AtomicInteger currentBatchNum = new AtomicInteger(0);

    // Class variables
    private String outputFilePath;
    private Host myHost;

    // Socket for sending and receiving messages
    private DatagramSocket socket;

    // Parser for converting messages to and from bytes
    private MessageParser messageParser = new MessageParser();

    // Threads
    private Thread receivePacketsThread;
    private Thread resendThread;
    private Thread windowAdjustmentThread;

    /**
     * Constructor for the PerfectLinks class.
     * @param outputFilePath the path to the output file
     * @param myHost the host running the process
     * @param hosts the list of all hosts in the system
     */
    public PerfectLinks(String outputFilePath, Host myHost, List<Host> hosts) {
        this.outputFilePath = outputFilePath;
        this.myHost = myHost;

        // Initialize the message parser
        messageParser.setHosts(hosts);

        // Delete & recreate the output file
        try {
            Files.deleteIfExists(Paths.get(outputFilePath));
            Files.createFile(Paths.get(outputFilePath));
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Initialize the socket
        try {
            socket = new DatagramSocket(myHost.getPort());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Initialize the threads
        receivePacketsThread = new Thread(() -> receivePackets());
        //resendThread = new Thread(() -> resendUnacknowledgedMessages());
        //windowAdjustmentThread = new Thread(() -> adjustWindowSizePeriodically());
    }

    public void start() {
        receivePacketsThread.start();
        //resendThread.start();
        //windowAdjustmentThread.start();
    }

    public void stop() {
        socket.close();
        receivePacketsThread.interrupt();
        //resendThread.interrupt();
        //windowAdjustmentThread.interrupt();
    }

    /**
     * Thread for adjusting the window size periodically based on network performance.
     * This thread wakes up every TIMEOUT_MS and adjusts the window size based on ACKs and timeouts.
     */
    private void adjustWindowSizePeriodically() {
        try {
            while (true) {
                // Sleep for the timeout duration
                Thread.sleep(timeout_ms);

                // Adjust window size based on ACKs and timeouts
                // Between 0% and 40% ACKs, decrease window size
                // Between 40% and 60% ACKs, keep window size the same
                // Between 60% and 100% ACKs, increase window size
                double ackRate = (double) ackCount.get() / (ackCount.get() + timeoutCount.get());
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
                    nb_consec_min.incrementAndGet();
                } else {
                    nb_consec_min.set(0);
                }

                if (nb_consec_min.get() > 3) {
                    // increasing the timeout
                    nb_consec_min.set(0);
                    timeout_ms = Math.min(timeout_ms * 2, MAX_TIMEOUT_MS);
                    System.out.println("Increasing timeout to " + timeout_ms);
                }

                // Reset counters for the next interval
                ackCount.set(0);
                timeoutCount.set(0);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Thread for receiving messages
    */
    private void receivePackets() {

        // Create a buffer for receiving packets
        byte[] buf = new byte[256];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);

        while (true) {
            try {
                // Receive the packet
                socket.receive(packet);

                // Extract the message from the packet
                String content = new String(packet.getData(), 0, packet.getLength());
                
                // Convert to message
                Message message = messageParser.parse(content);

                // If the message is an ACK, remove the batch from messageState
                if(message.getType() == MessageType.ACK){
                    System.out.println("Received ACK message" + message.toString());
                    
                    if (messageState.containsKey(message)){
                        System.out.println("Removing message from messageState" + message.toString());
                        messageState.remove(message);
                    } else {
                        System.out.println("Message not found in messageState" + message.toString());
                    }


                    // Increment the ACK counter
                    ackCount.incrementAndGet();
                } else if (message.getType() == MessageType.SEND) {
                    System.out.println("Received SEND message" + message.toString());
                    // Log the message if it's the first time it's received
                    if (!messagesReceived.containsKey(message)) {
                        logDeliveredMessage(message);
                    }

                    // Send ACK back to the sender
                    messagesReceived.put(message, true);
                    sendAck(message);
                } else {
                    System.out.println("Received unknown message type: " + message.getType());
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Thread for resending unacknowledged messages periodically.
     */
    private void resendUnacknowledgedMessages() {
        try {
            while (true) {
                // Sleep for the timeout duration before checking for timeouts
                Thread.sleep(timeout_ms);

                long currentTime = System.currentTimeMillis();

                for (Map.Entry<Message, Long> entry : messageState.entrySet()) {
                    Message message = entry.getKey();
                    long sentTime = entry.getValue();

                    // Check if the batch has timed out
                    if (currentTime - sentTime > timeout_ms) {
                        System.out.println("Timeout for message " + message.toString());

                        // Resend the message
                        sendMessage(message);

                        // Update the sent time in messageState
                        messageState.put(message, System.currentTimeMillis());

                        // Increment the timeout counter
                        timeoutCount.incrementAndGet();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Send a batch message to the receiver.
     * @param receiverHost the host to send the message to
     * @param seqMin the minimum sequence number in the batch
     * @param seqMax the maximum sequence number in the batch
     */
    public void sendSeqIds(Host receiverHost, int seqMin, int seqMax) {

        // Initialize the current sequence number
        int currentSeq = seqMin;

        // If the map is not full, send a batch of messages
        while (currentSeq <= seqMax) {
            if (messageState.size() < windowSize) {

                // Compute the sequence numbers to send
                int max = Math.min(currentSeq + Constants.BATCH_SIZE - 1, seqMax);
                int size = max - currentSeq + 1;
                List<Integer> seqNums = new ArrayList<>();
                for (int i = currentSeq; i <= max; i++) {
                    seqNums.add(i);
                }

                // Get the current batch number
                int batchNum = currentBatchNum.getAndIncrement();

                // Create the message
                Message message = new Message(MessageType.SEND, myHost, receiverHost, batchNum, seqNums);

                // Send the message and log it
                try {

                    sendMessage(message);
                    logBroadcastMessage(message);

                    // If successful, add the message to messageState
                    messageState.put(message, System.currentTimeMillis());

                    // Increment the current sequence number
                    currentSeq += size;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                // Sleep briefly to allow for ACK processing and window adjustment
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Send an acknowledgment message to the sender.
     * @param sent the message that was initially sent
     * @throws Exception
     */
    private void sendAck(Message sent) throws Exception {
        // Create the acknowledgment message
        Message ack = new Message(MessageType.ACK, sent.getSenderHost(), sent.getReceiverHost(), sent.getBatchNumber(), sent.getSequenceNumbers());
        // Send the acknowledgment
        sendMessage(ack);
    }

    /**
     * Send a batch message to the receiver.
     * @param message the message to send
     * @throws Exception
     */
    private void sendMessage(Message message) throws Exception {
        // Get the buffer to send
        byte[] buf = message.toBytes();

        // Get the address and port of the receiver
        Host receiverHost = message.getReceiverHost();
        InetAddress receiverAddress = InetAddress.getByName(receiverHost.getIp());
        int port = receiverHost.getPort();

        // Send the message
        DatagramPacket packet = new DatagramPacket(buf, buf.length, receiverAddress, port);
        socket.send(packet);
        System.out.println("Sent message " + message.toString());
    }

    /**
     * Log the message to the output file.
     * @param message 
     * @throws Exception
     */
    private synchronized void logBroadcastMessage(Message message) throws Exception {
        StringBuilder log = new StringBuilder();
        for (int seqNum : message.getSequenceNumbers()) {
            log.append("b").append(" ");
            log.append(seqNum).append("\n");
        }
        Files.write(Paths.get(outputFilePath), log.toString().getBytes(), StandardOpenOption.APPEND);
    }

    /**
     * Log the delivered message to the output file.
     * @param message
     * @throws Exception
     */
    private synchronized void logDeliveredMessage(Message message) throws Exception {
        StringBuilder log = new StringBuilder();
        int senderId = message.getSenderHost().getId();
        for (int seqNum : message.getSequenceNumbers()) {
            log.append("d").append(" ");
            log.append(senderId).append(" ");
            log.append(seqNum).append("\n");
        }
        Files.write(Paths.get(outputFilePath), log.toString().getBytes(), StandardOpenOption.APPEND);
    }
}
