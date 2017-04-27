package ir.ac.sbu.graph.utils;

/**
 * Useful functions for logging
 */
public class Log {

    public static void log(String msg, long start, long end) {
        log(msg, (end - start));
    }

    public static void log(String msg) {
        log(msg, -1);
    }

    public static void log(String msg, long duration) {
        if (duration == -1)
            System.out.println("KCoreNeighborList " + msg);
        else
            System.out.println("KCoreNeighborList " + msg + ", duration: " + duration + " ms");
    }
}
