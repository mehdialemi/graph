package ir.ac.sbu.graph.monitor;

import ir.ac.sbu.graph.utils.Log;
import ir.ac.sbu.graph.utils.SizeUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.TimerTask;

/**
 * TimerTask to log monitor
 */
public class Monitor extends TimerTask {

    public static final long LOG_DURATION = 5000;
    private JavaSparkContext sc;

    public Monitor(JavaSparkContext sc) {
        this.sc = sc;
    }

    @Override
    public void run() {
        if (sc.env().isStopped())
            return;

        logMemory("TIMER",sc);
    }

    public static void logMemory(String header, JavaSparkContext sc) {
        long executionMemoryUsed = sc.env().memoryManager().executionMemoryUsed();
        Log.logWithTS(header + " EXECUTION_MEMORY", "" + SizeUtils.humanReadable(executionMemoryUsed));
        long storageMemoryUsed = sc.env().memoryManager().storageMemoryUsed();
        Log.logWithTS(header + " STORAGE_MEMORY", "" + SizeUtils.humanReadable(storageMemoryUsed));
    }
}
