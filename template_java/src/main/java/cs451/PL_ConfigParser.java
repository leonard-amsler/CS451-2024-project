package cs451;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PL_ConfigParser {

    private String path;
    private int m; // m defines how many messages each sender process should send
    private int i; // i is the index of the receiver process

    /**
     * Populate the config file
     * @param value path to the config file
     * @return true if the config file was successfully read, false otherwise
     */
    public boolean populate(String value) {
        File file = new File(value);
        path = file.getPath();

        // Read the config file
        try (BufferedReader br = new BufferedReader(new FileReader(value))) {
            String line = br.readLine();
            String[] parts = line.split(" ");
            m = Integer.parseInt(parts[0]);
            i = Integer.parseInt(parts[1]);
            return true;
        } catch (IOException e) {
            System.err.println("Error reading config file!");
            return false;
        }

    }

    /**
     * Get the path of the config file
     * @return path
     */
    public String getPath() {
        return path;
    }

    /**
     * Get the number of messages each sender process should send
     * @return m
     */
    public int getM() {
        return m;
    }

    /**
     * Get the index of the receiver process
     * @return i
     */
    public int getI() {
        return i;
    }

}
