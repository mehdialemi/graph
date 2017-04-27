package ir.ac.sbu.graph.kcore;

import ir.ac.sbu.graph.utils.Log;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;

import java.io.File;

/**
 * Configure KCore methods
 */
public class KCoreConf {

    String inputPath;
    int partitionNum;
    int k;
    SparkConf sparkConf;

    public KCoreConf(String[] args, String name, Class... classes) {
        inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        partitionNum = 10;
        if (args.length > 1)
            partitionNum = Integer.parseInt(args[1]);

        k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);

        sparkConf = new SparkConf();
        if (args.length == 0)
            sparkConf.setMaster("local[2]");
        sparkConf.setAppName(name + "-" + partitionNum + "-" + new File(inputPath).getName());
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(classes);
        Log.log("Input: " + inputPath + ", partitionNum: " + partitionNum + ", k: " + k);
    }

    public Partitioner getPartitioner() {
        return new HashPartitioner(partitionNum);
    }
}
