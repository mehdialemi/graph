package ir.ac.sbu.graph.monitor;

import ir.ac.sbu.graph.utils.Log;
import org.apache.commons.io.FileUtils;
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
        long bytes = sc.env().memoryManager().storageMemoryUsed();
        Log.logWithTS(header + " STORAGE_MEMORY", FileUtils.byteCountToDisplaySize(bytes));
    }
}
