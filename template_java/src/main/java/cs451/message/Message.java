package cs451.message;

import java.util.List;
import java.util.Set;

import cs451.Host;

/**
 * Message class. A message contains the following information:
 * <message type> <sender id> <receiver id> <batch number> <sequence number 1>;<sequence number 2>;...;<sequence number n>
 */
public class Message {

    Host senderHost;
    Host receiverHost;
    int batchNumber;
    List<Integer> sequenceNumbers;
    MessageType messageType;

    public Message(MessageType messageType, Host senderHost, Host receiverHost, int batchNumber, List<Integer> sequenceNumbers) {
        this.senderHost = senderHost;
        this.receiverHost = receiverHost;
        this.batchNumber = batchNumber;
        this.sequenceNumbers = sequenceNumbers;
        this.messageType = messageType;
    }

    public Host getSenderHost() {
        return senderHost;
    }

    public Host getReceiverHost() {
        return receiverHost;
    }

    public int getBatchNumber() {
        return batchNumber;
    }

    public List<Integer> getSequenceNumbers() {
        return sequenceNumbers;
    }

    public MessageType getType() {
        return messageType;
    }

    public byte[] toBytes() {
        // Prepare the message
        StringBuilder message = new StringBuilder();
        message.append(messageType.toString()).append(" ");
        message.append(senderHost.getId()).append(" ");
        message.append(receiverHost.getId()).append(" ");
        message.append(batchNumber).append(" ");
        for (int seqNum : sequenceNumbers) {
            message.append(seqNum).append(";");
        }
        if(sequenceNumbers.size() > 0) message.deleteCharAt(message.length() - 1); // Remove the last semicolon
        

        return message.toString().getBytes();
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
        return senderHost.getId() == message.getSenderHost().getId() && receiverHost.getId() == message.getReceiverHost().getId() && batchNumber == message.getBatchNumber();
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + senderHost.getId();
        result = 31 * result + receiverHost.getId();
        result = 31 * result + batchNumber;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder message = new StringBuilder();
        message.append("Message Type: ").append(messageType).append("\n");
        message.append("Sender ID: ").append(senderHost.getId()).append("\n");
        message.append("Receiver ID: ").append(receiverHost.getId()).append("\n");
        message.append("Batch Number: ").append(batchNumber).append("\n");
        message.append("Sequence Numbers: ");
        for (int seqNum : sequenceNumbers) {
            message.append(seqNum).append(" ");
        }
        message.append("\n");

        return message.toString();
    }

}
