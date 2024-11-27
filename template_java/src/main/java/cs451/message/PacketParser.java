package cs451.message;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cs451.Host;

public class PacketParser {

    private List<Host> hosts;
    private MessageParser messageParser;
    
    public PacketParser(List<Host> hosts) {
        this.hosts = hosts;
        this.messageParser = new MessageParser(hosts);
    }

    public PacketParser() {
        this.hosts = new ArrayList<>();
        this.messageParser = new MessageParser();
    }

    public void setHosts(List<Host> hosts) {
        this.hosts = hosts;
        this.messageParser.setHosts(hosts);
    }

    public Packet parse(String message) {
        // PacketString format: <type> <sender id> <receiver id> <packet number> <message 1>;<message 2>;...;<message n>

        // Parse the message
        String[] splits = message.split(" ");

        // Get the data
        String type = splits[0];
        int senderId = Integer.parseInt(splits[1]);
        int receiverId = Integer.parseInt(splits[2]);
        int packetNumber = Integer.parseInt(splits[3]);
        String messages = splits[4];

        // Convert to objects to create the message
        PacketType messageType = PacketType.fromString(type);
        Host senderHost = hosts.get(senderId - 1);
        Host receiverHost = hosts.get(receiverId - 1);
        String[] messageSplits = messages.split(";");


        Message[] sequenceNumbersSet = new Message[messageSplits.length];
        for (int i = 0; i < messageSplits.length; i++) {
            sequenceNumbersSet[i] = messageParser.parse(messageSplits[i]);
        }

        return new Packet(messageType, senderHost, receiverHost, packetNumber, sequenceNumbersSet);
    }
}
