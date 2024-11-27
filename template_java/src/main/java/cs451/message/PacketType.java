package cs451.message;

/**
 * Message type enumeration. A message can be of the following types:
 * - SEND: A message sent by a process to another process
 * - ACK: An acknowledgment message sent by a process to another process
 */
public enum PacketType {
    SEND, ACK;

    @Override
    public String toString() {
        switch (this) {
            case SEND:
                return "SEND";
            case ACK:
                return "ACK";
            default:
                throw new IllegalArgumentException("Unknown packet type: " + this);
        }
    }

    public static PacketType fromString(String type) {
        switch (type) {
            case "SEND":
                return SEND;
            case "ACK":
                return ACK;
            default:
                throw new IllegalArgumentException("Unknown packet type: " + type);
        }
    }
}
