package cs451.parsers;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigParser {

    private String path;
    private int p; // p defines the number of processes
    private int vs; // vs denotes the maximum number of elements in a proposal
    private int ds; // ds denotes the maximum number of distinct elements across all proposals of all processes
    private List<Set<Integer>> proposals;

    /**
     * Populate the config file
     * 
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
            p = Integer.parseInt(parts[0]);
            vs = Integer.parseInt(parts[1]);
            ds = Integer.parseInt(parts[2]);
            // The subsequent p lines contain proposals.
            // Each proposal is a set of positive integers, written as a list of integers separated by single spaces.
            // Every line can have up to vs integers
            proposals = new ArrayList<>();
            for (int i = 1; i <= p; i++) {
                line = br.readLine();
                parts = line.split(" ");
                Set<Integer> proposal = new HashSet<>();
                for (String part : parts) {
                    proposal.add(Integer.parseInt(part));
                }
                proposals.add(proposal);
            }

            return true;
        } catch (IOException e) {
            System.err.println("Error reading config file!");
            return false;
        }

    }

    /**
     * Get the path of the config file
     * 
     * @return path
     */
    public String getPath() {
        return path;
    }

    /**
     * Get the number of proposal per process
     * 
     * @return p
     */
    public int getP() {
        return p;
    }

    /**
     * Get the maximum number of elements in a proposal
     * 
     * @return vs
     */
    public int getVs() {
        return vs;
    }

    /**
     * Get the maximum number of distinct elements across all proposals of all processes
     * 
     * @return ds
     */
    public int getDs() {
        return ds;
    }

    /**
     * Get the proposals
     * 
     * @return proposals
     */
    public List<Set<Integer>> getProposals() {
        return proposals;
    }

}
