package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.utils.Log;
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
    private JavaSparkContext sc;

    public SparkAppConf (ArgumentReader argumentReader) {
        inputPath = argumentReader.nextString("/home/mehdi/graph-data/com-amazon.ungraph.txt");
        partitionNum = argumentReader.nextInt(10);
    }

    protected String createAppName() {
        return new File(getInputPath()).getName() + "-" + partitionNum;
    }

    public void init() {
        String appName = createAppName();

        sparkConf = new SparkConf();
        if (inputPath.equals("/home/mehdi/graph-data/com-amazon.ungraph.txt"))
            sparkConf.setMaster("local[2]");

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
