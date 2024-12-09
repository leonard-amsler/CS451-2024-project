package cs451.message;

public enum LaticeMessageType {
    PROPOSE,
    ACK,
    NACK;

    @Override
    /**
     * Returns the string representation of the message type
     */
    public String toString() {
        switch (this) {
            case PROPOSE:
                return "PROPOSE";
            case ACK:
                return "ACK";
            case NACK:
                return "NACK";
            default:
                throw new IllegalArgumentException("Unknown packet type: " + this);
        }
    }

    /**
     * Returns the message type from the string representation
     * 
     * @param type The string representation of the message type
     * @return The message type
     */
    public static LaticeMessageType fromString(String type) {
        switch (type) {
            case "PROPOSE":
                return PROPOSE;
            case "ACK":
                return ACK;
            case "NACK":
                return NACK;
            default:
                throw new IllegalArgumentException("Unknown packet type: " + type);
        }
    }
}
