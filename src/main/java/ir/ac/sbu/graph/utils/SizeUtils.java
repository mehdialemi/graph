package ir.ac.sbu.graph.utils;

import java.text.DecimalFormat;

/**
 * Created by mehdi on 9/27/17.
 */
public class SizeUtils {

    final static String[] UNITS = new String[] { "B", "kB", "MB", "GB", "TB" };
    final static DecimalFormat FORMAT = new DecimalFormat("#,##0.#");

    public static String humanReadable(long size) {

        if(size <= 0) return "0";
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return FORMAT.format(size/Math.pow(1024, digitGroups)) + " " + UNITS[digitGroups];
    }
}
