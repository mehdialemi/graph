package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.spark.ktruss.MaxTSetValue;
import ir.ac.sbu.graph.spark.partitioning.PartitionInfoAccumulator;
import ir.ac.sbu.graph.types.*;
import ir.ac.sbu.graph.utils.Log;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.util.*;

/**
 * Configuration for spark applications
 */
public class SparkAppConf {

    protected String graphInputPath;
    protected int partitionNum;
    protected SparkConf sparkConf;
    protected int cores;
    protected JavaSparkContext javaSparkContext;
    protected SQLContext sqlContext;
    private final StorageLevel storageLevel;

    public SparkAppConf() {
        storageLevel = StorageLevel.MEMORY_AND_DISK();
    }

    public SparkAppConf(SparkAppConf conf) {
        this.graphInputPath = conf.graphInputPath;
        this.cores = conf.cores;
        storageLevel = StorageLevel.MEMORY_AND_DISK();
    }

    public SparkAppConf (ArgumentReader argumentReader) {
        graphInputPath = argumentReader.nextString("/home/mehdi/graph-data/com-youtube.ungraph.txt");
        cores = argumentReader.nextInt(2);
        partitionNum = argumentReader.nextInt(4);
        storageLevel = StorageLevel.MEMORY_AND_DISK();
    }

    protected String createAppName() {
        return getFileName()+ "-Cores-" + cores;
    }

    public String getFileName() {
        return new File(getGraphInputPath()).getName() ;
    }

    public void init() {
        String appName = createAppName();

        sparkConf = new SparkConf();
        if (!graphInputPath.startsWith("hdfs")) {
            sparkConf.set("spark.driver.bindAddress", "localhost");
            sparkConf.setMaster("local[" + cores + "]");

        }

        sparkConf.setAppName(appName);
        sparkConf.set("spark.driver.memory", "10g");
        sparkConf.set("spark.driver.maxResultSize", "9g");
//        sparkConf.set("spark.kryoserializer.buffer.max", "256m");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[] {int[].class, byte[].class, Edge.class, VertexDeg.class,
                int[][].class, MaxTSetValue.class, TSetValue.class, VertexDeg.class, PartitionInfoAccumulator.class,
                Edge.class, VSign.class, UEdge.class, IntList.class, IntCollection.class, Collection.class,
                List.class, Iterable.class, Comparable.class, IntIterable.class, Map.class, Map.Entry.class,
                HashMap.class, AbstractMap.class, Cloneable.class, IntSet.class, IntOpenHashSet.class,
                IntCollection.class, Set.class, AbstractIntSet.class, AbstractIntCollection.class, MaxTSetValue.class
        });

        javaSparkContext = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(javaSparkContext);
        Log.log(appName);
    }

    public SQLContext getSqlContext() {
        return sqlContext;
    }

    public String getGraphInputPath() {
        return graphInputPath;
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

    public JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    public final StorageLevel getStorageLevel() {
        return storageLevel;
    }
}
