package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Receiver {

    /**
     * Map to keep track of which messages have been received.
     * This can be used to ensure messages are not logged or processed multiple times.
     */
    private ConcurrentHashMap<Integer, Boolean> messagesReceived = new ConcurrentHashMap<>();

    /**
     * Receive messages on the given port and send ACKs back to the sender.
     * @param port the port to listen on
     * @param outputFilePath the path to the output file for logging received messages
     * @param myId the id of the receiver (used for logging purposes)
     * @param hosts list of hosts (used to get sender's address for ACKs)
     */
    public void receiveMessages(int port, String outputFilePath, int myId, List<Host> hosts) {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            while (true) {
                // Receive a message
                socket.receive(packet);
                String receivedMessage = new String(packet.getData(), 0, packet.getLength());

                // Parse the sender's ID and sequence number from the message
                String[] parts = receivedMessage.split(" ");
                int senderId = Integer.parseInt(parts[0]);
                int seqNum = Integer.parseInt(parts[1]);

                // Log the message if it's the first time it's received
                if (!messagesReceived.containsKey(seqNum)) {
                    logMessageReceived(seqNum, senderId, outputFilePath);
                    messagesReceived.put(seqNum, true);
                }

                // Send ACK back to the sender
                sendAck(socket, packet.getAddress(), packet.getPort(), myId, seqNum);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Log the received message to the output file.
     * @param seqNum the sequence number of the message
     * @param senderId the ID of the sender
     * @param outputFilePath the path to the output file for logging received messages
     */
    private void logMessageReceived(int seqNum, int senderId, String outputFilePath) throws Exception {
        String log = "d " + senderId + " " + seqNum + "\n";
        if (!Files.exists(Paths.get(outputFilePath))) {
            Files.createFile(Paths.get(outputFilePath));
        }
        Files.write(Paths.get(outputFilePath), log.getBytes(), StandardOpenOption.APPEND);
    }

    /**
     * Send an acknowledgment (ACK) back to the sender for the received message.
     * @param socket the socket to send the ACK on
     * @param senderAddress the address of the sender
     * @param port the port to send the ACK to
     * @param myId the ID of the receiver (used in the ACK message)
     * @param seqNum the sequence number of the message being acknowledged
     */
    private void sendAck(DatagramSocket socket, InetAddress senderAddress, int port, int myId, int seqNum) throws Exception {
        String ackMessage = "ack " + myId + " " + seqNum;
        byte[] ackBuf = ackMessage.getBytes();
        DatagramPacket ackPacket = new DatagramPacket(ackBuf, ackBuf.length, senderAddress, port);
        socket.send(ackPacket);
    }
}


