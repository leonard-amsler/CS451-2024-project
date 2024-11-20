package cs451.message;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cs451.Host;

public class MessageParser {

    private List<Host> hosts;
    
    public MessageParser(List<Host> hosts) {
        this.hosts = hosts;
    }

    public MessageParser() {
        this.hosts = new ArrayList<>();
    }

    public void setHosts(List<Host> hosts) {
        this.hosts = hosts;
    }

    public Message parse(String message) {
        // Format: <message type> <sender id> <receiver id> <batch number> <sequence number 1>;<sequence number 2>;...;<sequence number n>

        // Parse the message
        String[] splits = message.split(" ");

        // Get the data
        String type = splits[0];
        int senderId = Integer.parseInt(splits[1]);
        int receiverId = Integer.parseInt(splits[2]);
        int batchNumber = Integer.parseInt(splits[3]);

        // Convert to objects to create the message
        MessageType messageType = MessageType.fromString(type);
        Host senderHost = hosts.get(senderId - 1);
        Host receiverHost = hosts.get(receiverId - 1);

        List<Integer> sequenceNumbersSet = new ArrayList<>();
        String[] sequenceNumbers = splits[4].split(";");
        for (String seqNum : sequenceNumbers) {
            sequenceNumbersSet.add(Integer.parseInt(seqNum));
        }

        return new Message(messageType, senderHost, receiverHost, batchNumber, sequenceNumbersSet);
    }
}
