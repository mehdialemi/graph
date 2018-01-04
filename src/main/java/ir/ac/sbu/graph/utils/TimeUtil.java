package ir.ac.sbu.graph.utils;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 *
 */
public class TimeUtil {

    static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    public static long diff(String t2String, String t1String) throws ParseException {
        long t2 = df.parse(t2String).toInstant().toEpochMilli();
        long t1 = df.parse(t1String).toInstant().toEpochMilli();
        return t2 - t1;
    }
}
