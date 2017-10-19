package ir.ac.sbu.biggraph.kcore;

import ir.ac.sbu.biggraph.utils.Log;
import org.apache.spark.SparkConf;

import java.io.File;

/**
 * Configure KCore methods
 */
public class KCoreConf {

    final protected String inputPath;
    final protected int partitionNum;
    final protected int k;
    final protected String name;
    final SparkConf sparkConf;

    public KCoreConf(String[] args, String name, Class... classes) {
        inputPath = args.length > 0 ? args[0] : "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        partitionNum = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        k = args.length > 2 ? Integer.parseInt(args[2]) : 4;
        this.name = name;

        sparkConf = new SparkConf();
        if (args.length == 0)
            sparkConf.setMaster("local[2]");

        sparkConf.setAppName(name + "-" + partitionNum + "-" + new File(inputPath).getName());
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(classes);
        Log.log("Input: " + inputPath + ", partitionNum: " + partitionNum + ", k: " + k);
    }

}
