package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import cs451.utils.Pair;

public class Receiver {
    /**
     * Map to keep track of which messages have been received.
     * This can be used to ensure messages are not logged or processed multiple times.
     */
    private ConcurrentHashMap<Pair<Integer, Integer>, Boolean> messagesReceived = new ConcurrentHashMap<>();

    /**
     * Receive messages on the given port and send ACKs back to the sender.
     * @param port the port to listen on
     * @param outputFilePath the path to the output file for logging received messages
     * @param myId the id of the receiver (used for logging purposes)
     * @param hosts list of hosts (used to get sender's address for ACKs)
     */
    public void receiveMessages(int port, String outputFilePath, int myId, List<Host> hosts) {
        // Delete & recreate the output file
        try {
            Files.deleteIfExists(Paths.get(outputFilePath));
            Files.createFile(Paths.get(outputFilePath));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (DatagramSocket socket = new DatagramSocket(port)) {
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            while (true) {
                // Receive a message
                socket.receive(packet);
                String receivedMessage = new String(packet.getData(), 0, packet.getLength());
                // message format: "senderId batchNum seqNum1;seqNum2;seqNum3;..."

                // Parse the sender's ID and sequence number from the message
                String[] parts = receivedMessage.split(" ");
                int senderId = Integer.parseInt(parts[0]);
                int batchNum = Integer.parseInt(parts[1]);
                String[] seqNums = parts[2].split(";");

                // Log the message if it's the first time it's received
                if (!messagesReceived.containsKey(new Pair<>(senderId, batchNum))) {
                    messagesReceived.put(new Pair<>(senderId, batchNum), true);
                    if(messagesReceived.size() % 100 == 0) {
                        System.out.println("Received " + messagesReceived.size() + " batches");
                    }
                    logBatchReceived(seqNums, senderId, outputFilePath);
                }

                // Send ACK back to the sender
                sendBatchAck(socket, packet.getAddress(), packet.getPort(), myId, batchNum);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Logs a batch of received sequence numbers to a specified output file.
     *
     * @param seqNums       An array of sequence numbers to be logged.
     * @param senderId      The ID of the sender from whom the sequence numbers were received.
     * @param outputFilePath The path to the file where the log should be written.
     * @throws Exception    If an I/O error occurs writing to or creating the file.
     */
    private void logBatchReceived(String[] seqNums, int senderId, String outputFilePath) throws Exception {
        StringBuilder log = new StringBuilder();
        for (String seqNum : seqNums) {
            log.append("d ").append(senderId).append(" ").append(seqNum).append("\n");
        }
        Files.write(Paths.get(outputFilePath), log.toString().getBytes(), StandardOpenOption.APPEND);
    }

    /**
     * Sends a batch acknowledgment message to the specified sender.
     *
     * @param socket the DatagramSocket used to send the acknowledgment
     * @param senderAddress the InetAddress of the sender to whom the acknowledgment is sent
     * @param port the port number of the sender
     * @param myId the identifier of the sender sending the acknowledgment
     * @param batchNum the batch number being acknowledged
     * @throws Exception if an I/O error occurs while sending the acknowledgment
     */
    private void sendBatchAck(DatagramSocket socket, InetAddress senderAddress, int port, int myId, int batchNum) throws Exception {
        String ackMessage = "ack " + myId + " " + batchNum;
        byte[] ackBuf = ackMessage.getBytes();
        DatagramPacket ackPacket = new DatagramPacket(ackBuf, ackBuf.length, senderAddress, port);
        socket.send(ackPacket);
    }
}


