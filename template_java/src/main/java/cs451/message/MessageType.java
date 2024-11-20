package cs451.message;

/**
 * Message type enumeration. A message can be of the following types:
 * - SEND: A message sent by a process to another process
 * - ACK: An acknowledgment message sent by a process to another process
 */
public enum MessageType {
    SEND, ACK;

    @Override
    public String toString() {
        switch (this) {
            case SEND:
                return "SEND";
            case ACK:
                return "ACK";
            default:
                throw new IllegalArgumentException("Unknown message type: " + this);
        }
    }

    public static MessageType fromString(String type) {
        switch (type) {
            case "SEND":
                return SEND;
            case "ACK":
                return ACK;
            default:
                throw new IllegalArgumentException("Unknown message type: " + type);
        }
    }
}
