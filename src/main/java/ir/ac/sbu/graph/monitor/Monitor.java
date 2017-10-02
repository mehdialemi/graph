package ir.ac.sbu.graph.monitor;

import ir.ac.sbu.graph.utils.Log;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

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

    private static void logMemory(String header, JavaSparkContext sc) {
//        sc.env().blockManager().blockInfoManager().entries().foreach(block -> {
//            block._2().size();
//            StorageLevel level = block._2().level();
//            if (level == StorageLevel.MEMORY_ONLY()) {
//
//            }
//        });
        long bytes = sc.env().blockManager().memoryStore().currentUnrollMemory();
        Log.logWithTS(header + " STORAGE_MEMORY", FileUtils.byteCountToDisplaySize(bytes));
    }
}
