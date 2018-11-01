package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.spark.partitioning.PartitionInfoAccumulator;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.VertexByte;
import ir.ac.sbu.graph.types.VertexDeg;
import ir.ac.sbu.graph.utils.Log;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

/**
 * Configuration for spark applications
 */
public class SparkAppConf {

    private String inputPath;
    private int partitionNum;
    private SparkConf sparkConf;
    private int cores;
    private JavaSparkContext sc;

    public SparkAppConf(SparkAppConf conf) {
        this.inputPath = conf.inputPath;
        this.cores = conf.cores;
    }
    public SparkAppConf (ArgumentReader argumentReader) {
        inputPath = argumentReader.nextString("/home/mehdi/graph-data/com-youtube.ungraph.txt");
        cores = argumentReader.nextInt(2);
    }

    protected String createAppName() {
        return getFileName();
    }

    public String getFileName() {
        return new File(getInputPath()).getName() + "-Cores-" + cores;
    }

    public void init() {
        String appName = createAppName();

        sparkConf = new SparkConf();
        if (!inputPath.startsWith("hdfs")) {
            sparkConf.set("spark.driver.bindAddress", "localhost");
            sparkConf.setMaster("local[" + cores + "]");
            sparkConf.set("spark.driver.memory", "10g");
            sparkConf.set("spark.driver.maxResultSize", "9g");
            sparkConf.set("spark.kryoserializer.buffer.max", "256m");
        }

        sparkConf.setAppName(appName);
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[] {int[].class, VertexDeg.class, PartitionInfoAccumulator.class,
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

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public SparkConf getSparkConf() {
        return sparkConf;
    }

    public JavaSparkContext getSc() {
        return sc;
    }
}
