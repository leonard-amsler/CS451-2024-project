package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class Sender {

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

            for (int seqNum = 1; seqNum <= m; seqNum++) {
                // Create the message (senderId + seqNum)
                String message = myId + " " + seqNum;
                byte[] buf = message.getBytes();

                // Send the message as a UDP packet
                DatagramPacket packet = new DatagramPacket(buf, buf.length, receiverAddress, receiver.getPort());
                socket.send(packet);

                // Log the send event
                String log = "b " + seqNum + "\n";

                // Verify that the output file exists
                if (!Files.exists(Paths.get(outputFilePath))) {
                    Files.createFile(Paths.get(outputFilePath));
                }

                // Append the log to the output file
                Files.write(Paths.get(outputFilePath), log.getBytes(), StandardOpenOption.APPEND);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
