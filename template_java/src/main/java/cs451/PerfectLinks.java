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
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import cs451.message.Message;
import cs451.message.Packet;
import cs451.message.PacketParser;
import cs451.message.PacketType;
import cs451.utils.Pair;

public class PerfectLinks {

    // Queues for messages to be braodcasted
    private LinkedBlockingQueue<Pair<Message, Host>> broadcastQueue = new LinkedBlockingQueue<>(Constants.MAX_QUEUE_SIZE); // Queue for messages to be broadcasted
    // Maps for tracking packets   
    private Map<Integer, Pair<Packet, Long>> packets = new ConcurrentHashMap<>(); // <seqNum, <Packet, timestamp>>
    // Map for delivered messages
    private Set<Message> deliveredMessages = ConcurrentHashMap.newKeySet();


    // Congestion control parameters
    private static final double MAX_WINDOW_SIZE = Math.pow(2, 17); // Maximum window size (131'072)
    private static final double MIN_WINDOW_SIZE = Math.pow(2, 0); // Minimum window size (1)
    private double windowSize = MIN_WINDOW_SIZE;
    private int timeout_ms = 512; // Initial timeout duration

    // Variables for tracking acknowledgments and timeouts
    private AtomicInteger ackCount = new AtomicInteger(0);
    private AtomicInteger timeoutCount = new AtomicInteger(0);

    // Batch number generator for new messages
    private AtomicInteger currentPacketNum = new AtomicInteger(0);

    // Class variables
    private String outputFilePath;
    private Host myHost;

    // Socket for sending and receiving messages
    private DatagramSocket socket;

    // Parser for converting messages to and from bytes
    private PacketParser packetParser = new PacketParser();

    // Threads
    private Thread receivePacketsThread;
    private Thread windowAdjustmentThread;
    private Thread processQueueThread;

    // Thread for resending unacknowledged packets
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // Parent
    private URB parentURB;

    /**
     * Constructor for the PerfectLinks class.
     * @param outputFilePath the path to the output file
     * @param myHost the host running the process
     * @param hosts the list of all hosts in the system
     */
    public PerfectLinks(String outputFilePath, Host myHost, List<Host> hosts, URB parentURB) {
        this(outputFilePath, myHost, hosts);
        this.parentURB = parentURB;
    }

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
        packetParser.setHosts(hosts);

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
        windowAdjustmentThread = new Thread(() -> adjustWindowSizePeriodically());
        processQueueThread = new Thread(() -> processQueue());
    }

    /**
     * Start the threads for the PerfectLinks class.
     */
    public void start() {
        processQueueThread.start();
        receivePacketsThread.start();
        windowAdjustmentThread.start();
    }

    /**
     * Stop the threads for the PerfectLinks class and close the socket.
     */
    public void stop() {
        socket.close();
        processQueueThread.interrupt();
        receivePacketsThread.interrupt();
        windowAdjustmentThread.interrupt();
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

                //System.out.println("\nACK count: " + ackCount.get());
                //System.out.println("Timeout count: " + timeoutCount.get());
                //System.out.println("Window size: " + windowSize);
                //System.out.println("Timeout: " + timeout_ms);
                //System.out.println("Consecutive min: " + nb_consec_min.get());
                //System.out.println("Delivered messages: " + deliveredMessages.size());
                //System.out.println("Packets: " + packets.size());
                //System.out.println("Queue: " + broadcastQueue.size());

                double ackRate = (double) ackCount.get() / (ackCount.get() + timeoutCount.get());
                if (ackRate < 0.3) {
                    windowSize = Math.max(windowSize / 2, MIN_WINDOW_SIZE);
                    //System.out.println("Decreasing window size to " + windowSize);
                } else if (ackRate > 0.7) {
                    windowSize = Math.min(windowSize * 2, MAX_WINDOW_SIZE);
                    //System.out.println("Increasing window size to " + windowSize);
                } else {
                    //System.out.println("Keeping window size at " + windowSize);
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
        byte[] buf = new byte[65535];
        DatagramPacket dgPacket = new DatagramPacket(buf, buf.length);

        while (true) {
            try {
                // Receive the packet
                socket.receive(dgPacket);

                // Extract the message from the packet
                String content = new String(dgPacket.getData(), 0, dgPacket.getLength());
                
                // Convert to packet
                Packet packet = packetParser.parse(content);

                //System.out.println("\nReceived packet " + packet.toString());

                // If the message is an ACK, remove the batch from messageState
                if(packet.getPacketType() == PacketType.ACK){
                    //System.out.println("Received ACK message" + packet.toString());

                    // Remove the packet from the packets map
                    packets.remove(packet.getPacketNumber());

                    synchronized (packets) {
                        packets.notifyAll();
                    }

                    // Increment the ACK counter
                    ackCount.incrementAndGet();

                } else if (packet.getPacketType() == PacketType.SEND) {
                    //System.out.println("Received SEND message\n" + message.toString());

                    // Deliver the message if it hasn't been delivered yet
                    for (Message message: packet.getMessages()) {
                        if (deliveredMessages.add(message)) {
                            // The message was not already delivered
                            if (parentURB == null) logDeliveredMessage(message, packet.getSenderHost().getId());
                        }

                        if (parentURB != null) {
                            parentURB.beb_deliver(message, packet.getSenderHost());
                        }
                    }

                    sendAck(packet);
                } else {
                    //System.out.println("Received unknown message type: " + message.getType());
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Thread for processing the next message in the queue. Pack the messages that have to be sent to the same receiver in the same packet.
     */
    private void processQueue() {

        while (true){

            try {
                // Verify that the packets map is smaller than the window size
                synchronized (packets) {
                    while (packets.size() >= windowSize) {
                        packets.wait();
                    }
                }
                
                // Get the next message from the queue
                Pair<Message, Host> pair = broadcastQueue.take();

                if (pair != null) {
                    // Extract the message and receiver
                    Message message = pair.getFirst();
                    Host receiverHost = pair.getSecond();
                    
                    // Try to get more messages for the same receiver
                    List<Message> messagesList = new ArrayList<>();
                    messagesList.add(message);
                    while (broadcastQueue.peek() != null && broadcastQueue.peek().getSecond().equals(receiverHost) && messagesList.size() < Constants.BATCH_SIZE) {
                        Pair<Message, Host> nextPair = broadcastQueue.poll();
                        messagesList.add(nextPair.getFirst());
                    }
                    Message[] messages = messagesList.toArray(new Message[0]);

                    // Send the messages
                    int packetNumber = currentPacketNum.getAndIncrement();
                    Packet packet = new Packet(PacketType.SEND, myHost, receiverHost, packetNumber, messages);

                    // Add the packet to the packets map
                    packets.put(packetNumber, new Pair<>(packet, System.currentTimeMillis()));

                    // Send the packet
                    try {
                        sendPacket(packet);
                        if (parentURB == null) {
                            logBroadcastPacket(packet);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Broadcast a message to host
     * @param message the message to broadcast
     * @param receiverHost the host to broadcast the message to
     */
    public void pl_broadcast(Message message, Host receiverHost) {
        // Add to the broadcast queue
        while(broadcastQueue.size() + 1000 > Constants.MAX_QUEUE_SIZE) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        broadcastQueue.add(new Pair<>(message, receiverHost));
    }

    /**
     * Send an acknowledgment message to the sender.
     * @param sent the message that was initially sent
     * @throws Exception
     */
    private void sendAck(Packet sent) throws Exception {
        // Create the acknowledgment message
        Packet ack = new Packet(PacketType.ACK, myHost, sent.getSenderHost(), sent.getPacketNumber(), sent.getMessages());
        // Send the acknowledgment
        sendPacket(ack);
    }

    /**
     * Send an packet to the receiver.
     * @param packet the packet to send
     * @throws Exception
     */
    private void sendPacket(Packet packet) throws Exception {
        //System.out.println("\nSending packet " + packet.toString());

        // Get the buffer to send
        byte[] buf = packet.toBytes();

        // Get the address and port of the receiver
        Host receiverHost = packet.getReceiverHost();
        InetAddress receiverAddress = InetAddress.getByName(receiverHost.getIp());
        int port = receiverHost.getPort();

        // Send the message
        DatagramPacket dgPacket = new DatagramPacket(buf, buf.length, receiverAddress, port);
        socket.send(dgPacket);

        // Schedule a resend task
        scheduler.schedule(() -> {
            if (packets.containsKey(packet.getPacketNumber())) {
                try {
                    sendPacket(packet);
                    timeoutCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, timeout_ms, TimeUnit.MILLISECONDS);
    }

    /**
     * Log the broadcast message.
     * @param packet
     * @throws Exception
     */
    private synchronized void logBroadcastPacket(Packet packet) throws Exception {
        StringBuilder log = new StringBuilder();
        for (Message message: packet.getMessages()) {
            log.append("b").append(" ");
            log.append(message.getContent()).append("\n");
        }
        Files.write(Paths.get(outputFilePath), log.toString().getBytes(), StandardOpenOption.APPEND);
    }

    /**
     * Log the delivered message.
     * @param message the message to log
     * @param senderId the id of the sender
     * @throws Exception
     */
    private synchronized void logDeliveredMessage(Message message, int senderId) throws Exception {
        StringBuilder log = new StringBuilder();
        log.append("d").append(" ");
        log.append(senderId).append(" ");
        log.append(message.getContent()).append("\n");
        Files.write(Paths.get(outputFilePath), log.toString().getBytes(), StandardOpenOption.APPEND);
    }
}
