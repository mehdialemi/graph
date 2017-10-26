package ir.ac.sbu.graph.utils;

import java.time.Instant;

public class DiffTime {

    public static long diffMillis(String start, String end) {
        long tStart = Instant.parse(start.substring(0, start.indexOf("GMT")) + "Z").toEpochMilli();
        long tEnd = Instant.parse(end.substring(0, end.indexOf("GMT")) + "Z").toEpochMilli();
        return tEnd - tStart;
    }
}
