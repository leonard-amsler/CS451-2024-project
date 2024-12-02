package cs451.message;

import cs451.Host;

/**
 * Message class. A message contains the following information:
 * - The host that initially sent the message
 * - The content of the message
 * PacketString format: <initial sender id>:<content>
 */
public class Message {

    int initialSenderHostId;
    String content;
    int timestamp;

    public Message(Host initialSenderHost, String content, int timestamp) {
        this.initialSenderHostId = initialSenderHost.getId();
        this.content = content;
        this.timestamp = timestamp;
    }

    public int getInitialSenderHostId() {
        return initialSenderHostId;
    }

    public String getContent() {
        return content;
    }

    public int getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof Message)) {
            return false;
        }

        Message message = (Message) obj;
        return initialSenderHostId == message.initialSenderHostId && content.equals(message.content) && timestamp == message.timestamp;
    }

    @Override
    public int hashCode() {
        return 17 * initialSenderHostId + 100 * content.hashCode() + 1000 * timestamp;
    }

    @Override
    public String toString() {
        return "Message{" +
                "initialSenderHostId=" + initialSenderHostId +
                ", content=" + content +
                ", timestamp=" + timestamp +
                '}';
    }

    public String toPacketString() {
        return Integer.toString(initialSenderHostId) + ":" + content + ":" + Integer.toString(timestamp);
    }

}
