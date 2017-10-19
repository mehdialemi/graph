package ir.ac.sbu.biggraph.utils;

import java.util.Date;

/**
 * Useful functions for logging
 */
public class Log {

    private static String name = "SBM";

    public static void setName(String name) {
        Log.name = name;
    }

    public static void log(String msg, long start, long end) {
        log(msg, (end - start));
    }

    public static void logWithTS(String header, String msg) {
        log( header + " (" + new Date() + ") " + msg);
    }

    public static void log(String msg) {
        log(msg, -1);
    }

    public static void log(String msg, long duration) {
        if (duration == -1)
            System.out.println("[" + name + "] " + msg);
        else
            System.out.println("[" + name + "] " + msg + ", duration: " + duration + " ms");
    }
}
