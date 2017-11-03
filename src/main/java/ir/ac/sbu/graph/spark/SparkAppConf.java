package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.utils.Log;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

/**
 * Configuration for spark applications
 */
public class SparkAppConf {

    private final String inputPath;
    private final int partitionNum;
    private final SparkConf sparkConf;
    private final JavaSparkContext sc;

    public SparkAppConf (SparkAppConf conf) {
        this.inputPath = conf.inputPath;
        this.partitionNum = conf.partitionNum;
        this.sparkConf = conf.sparkConf;
        this.sc = conf.sc;
    }
    public SparkAppConf (ArgumentReader argumentReader, String name) {
        sparkConf = new SparkConf();
        if (argumentReader.isEmpty())
            getSparkConf().setMaster("local[2]");

        inputPath = argumentReader.nextString("/home/mehdi/graph-data/com-amazon.ungraph.txt");
        partitionNum = argumentReader.nextInt(10);
        String appName = name + "-" + partitionNum + "-" + new File(getInputPath()).getName();

        sparkConf.setAppName(appName);
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[] {int[].class, VertexDeg.class,
                Edge.class, VertexByte.class});

        sc = new JavaSparkContext(sparkConf);
        Log.log(appName);
    }

    public String getInputPath() {
        return inputPath;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public SparkConf getSparkConf() {
        return sparkConf;
    }

    public JavaSparkContext getSc() {
        return sc;
    }
}
