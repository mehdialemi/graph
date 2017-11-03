package ir.ac.sbu.graph.spark;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Read argument from a given queue
 */
public class ArgumentReader {

    private Queue<String> argQueue;

    public ArgumentReader(String[] args) {
        argQueue = new LinkedList<>();
        for (String arg : args) {
            argQueue.add(arg);
        }
    }

    public boolean isEmpty() { return argQueue.isEmpty(); }

    public String nextString(String defaultValue) {
        String arg = argQueue.peek();
        return arg == null ? defaultValue : arg;
    }

    public int nextInt(int defaultValue) {
        String arg = argQueue.peek();
        return arg == null ? defaultValue : Integer.parseInt(arg);
    }
}
