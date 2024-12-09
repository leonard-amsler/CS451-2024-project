package cs451;

public class Constants {
    public static final int ARG_LIMIT_CONFIG = 7;

    // indexes for id
    public static final int ID_KEY = 0;
    public static final int ID_VALUE = 1;

    // indexes for hosts
    public static final int HOSTS_KEY = 2;
    public static final int HOSTS_VALUE = 3;

    // indexes for output
    public static final int OUTPUT_KEY = 4;
    public static final int OUTPUT_VALUE = 5;

    // indexes for config
    public static final int CONFIG_VALUE = 6;

    public static final int BATCH_SIZE = 8; // Number of messages to send in a batch

    // Send messages indexes
    public static final int SEND_SERNDER_ID = 0;
    public static final int SEND_BATCH_NUM = 1;
    public static final int SEND_SEQ_NUMS = 2;

    // Ack messages indexes
    public static final int ACK_SENDER_ID = 1;
    public static final int ACK_BATCH_NUM = 2;

    // Timeout
    public static final int TIMEOUT_MS = 256;

    // Queue size
    public static final int MAX_QUEUE_SIZE = (int) Math.pow(2, 18); // 256KB
}
