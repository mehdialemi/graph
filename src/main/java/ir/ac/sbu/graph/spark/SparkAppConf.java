package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.spark.ktruss.MaxTSetValue;
import ir.ac.sbu.graph.spark.partitioning.PartitionInfoAccumulator;
import ir.ac.sbu.graph.types.*;
import ir.ac.sbu.graph.utils.Log;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.util.*;

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
        partitionNum = argumentReader.nextInt(4);
    }

    protected String createAppName() {
        return getFileName()+ "-Cores-" + cores;
    }

    public String getFileName() {
        return new File(getInputPath()).getName() ;
    }

    public void init() {
        String appName = createAppName();

        sparkConf = new SparkConf();
        if (!inputPath.startsWith("hdfs")) {
            sparkConf.set("spark.driver.bindAddress", "localhost");
            sparkConf.setMaster("local[" + cores + "]");

        }

        sparkConf.setAppName(appName);
        sparkConf.set("spark.driver.memory", "10g");
        sparkConf.set("spark.driver.maxResultSize", "9g");
//        sparkConf.set("spark.kryoserializer.buffer.max", "256m");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[] {int[].class, MaxTSetValue.class, TSetValue.class, VertexDeg.class, PartitionInfoAccumulator.class,
                Edge.class, OrderedVertex.class, UEdge.class, IntList.class, IntCollection.class, Collection.class,
                List.class, Iterable.class, Comparable.class, IntIterable.class, Map.class, Map.Entry.class,
                HashMap.class, AbstractMap.class, Cloneable.class, IntSet.class, IntOpenHashSet.class,
                IntCollection.class, Set.class, AbstractIntSet.class, AbstractIntCollection.class, MaxTSetValue.class
        });

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
