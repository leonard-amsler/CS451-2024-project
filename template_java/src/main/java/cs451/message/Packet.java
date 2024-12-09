package cs451.message;

import cs451.Host;

/**
 * Packet class. A packet contains the following information:
 * - The type of the packet
 * - The host that sent the packet
 * - The host that should receive the packet
 * - The packet number
 * - The messages contained in the packet
 * PacketString format: <type> <sender id> <receiver id> <packet number> <message 1>;<message 2>;...;<message n>
 */
public class Packet {

    private final PacketType PacketType;
    private final Host senderHost;
    private final Host receiverHost;
    private final int packetNumber;
    private final Message[] messages;

    /**
     * Constructor for the Packet class
     * 
     * @param PacketType   The type of the packet
     * @param senderHost   The host that sent the packet
     * @param receiverHost The host that should receive the packet
     * @param packetNumber The packet number
     * @param messages     The messages contained in the packet
     */
    public Packet(PacketType PacketType, Host senderHost, Host receiverHost, int packetNumber, Message[] messages) {
        this.PacketType = PacketType;
        this.senderHost = senderHost;
        this.receiverHost = receiverHost;
        this.packetNumber = packetNumber;
        this.messages = messages;
    }

    public PacketType getPacketType() {
        return PacketType;
    }

    public Host getSenderHost() {
        return senderHost;
    }

    public Host getReceiverHost() {
        return receiverHost;
    }

    public int getPacketNumber() {
        return packetNumber;
    }

    public Message[] getMessages() {
        return messages;
    }

    public String toString() {
        String toReturn = "Packet{" +
                "type=" + PacketType.toString() +
                ", senderHost=" + senderHost.getId() +
                ", receiverHost=" + receiverHost.getId() +
                ", packetNumber=" + packetNumber +
                ", messages=";

        for (Message message : messages) {
            toReturn += message.toString() + ";";
        }

        return toReturn + "}";
    }

    public byte[] toBytes() {
        // Prepare the packet
        StringBuilder packet = new StringBuilder();
        packet.append(PacketType.toString()).append(" ");
        packet.append(senderHost.getId()).append(" ");
        packet.append(receiverHost.getId()).append(" ");
        packet.append(packetNumber).append(" ");
        for (Message message : messages) {
            packet.append(message.toPacketString()).append(";");
        }
        if (messages.length > 0)
            packet.deleteCharAt(packet.length() - 1); // Remove the last semicolon

        return packet.toString().getBytes();
    }

}
