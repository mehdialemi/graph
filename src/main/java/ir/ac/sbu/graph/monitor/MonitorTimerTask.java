package ir.ac.sbu.graph.monitor;

import ir.ac.sbu.graph.utils.Log;
import ir.ac.sbu.graph.utils.SizeUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.TimerTask;

/**
 * TimerTask to log monitor
 */
public class MonitorTimerTask extends TimerTask {

    public static final long LOG_DURATION = 5000;
    private JavaSparkContext sc;

    public MonitorTimerTask(JavaSparkContext sc) {
        this.sc = sc;
    }

    @Override
    public void run() {
        if (sc.env().isStopped())
            return;

        long executionMemoryUsed = sc.env().memoryManager().executionMemoryUsed();
        Log.logWithTS("EXECUTION MEMORY", "" + SizeUtils.humanReadable(executionMemoryUsed));
        long storageMemoryUsed = sc.env().memoryManager().storageMemoryUsed();
        Log.logWithTS("STORAGE MEMORY", "" + SizeUtils.humanReadable(storageMemoryUsed));
    }
}
