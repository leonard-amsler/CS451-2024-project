package cs451;

import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Receiver {

    /**
     * Receive messages on the given port and log the delivery events
     * @param port the port to listen on
     * @param outputFilePath the path to the output file
     * @param myId the id of the receiver
     */
    public void receiveMessages(int port, String outputFilePath, int myId) {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            while (true) {
                // Receive a message
                socket.receive(packet);
                String receivedMessage = new String(packet.getData(), 0, packet.getLength());

                // Extract senderId and seqNum from the message
                String[] parts = receivedMessage.split(" ");
                int senderId = Integer.parseInt(parts[0]);
                int seqNum = Integer.parseInt(parts[1]);

                // Log the delivery event
                String log = "d " + senderId + " " + seqNum + "\n";

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
