package cs451.message;

import java.util.ArrayList;
import java.util.List;

import cs451.Host;

/**
 * Message parser class. A message parser is used to parse a message from a string.
 * PacketString format: <initial sender id>:<content>
 */
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
        // MessageString format: <initial sender id>:<content>

        // Parse the message
        String[] splits = message.split(":");

        // Get the data
        int initialSenderId = Integer.parseInt(splits[0]);
        String content = splits[1];
        int timestamp = Integer.parseInt(splits[2]);

        // Convert to objects to create the message
        Host initialSenderHost = hosts.get(initialSenderId - 1);

        return new Message(initialSenderHost, content, timestamp);
    }

}
