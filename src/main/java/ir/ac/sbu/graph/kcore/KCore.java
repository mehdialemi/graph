package ir.ac.sbu.graph.kcore;

import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 */
public abstract class KCore {
    public static final int[] EMPTY_ARRAY = new int[]{};
    protected JavaPairRDD<Integer, int[]> neighborList;
    protected int k;
    protected final Partitioner partitioner;

    public KCore(JavaPairRDD<Integer, int[]> neighborList, int k, Partitioner partitioner) {

        this.neighborList = neighborList;
        this.k = k;
        this.partitioner = partitioner;
    }

    public abstract void start();

    public JavaPairRDD<Integer, int[]> getNeighborList() {
        return neighborList;
    }

    public JavaPairRDD<Integer, Integer> getDegInfo() {
        return neighborList.mapValues(n -> n.length);
    }

    protected static void log(String msg, long start, long end) {
        log(msg, (end - start));
    }

    protected static void log(String msg) {
        log(msg, -1);
    }

    protected static void log(String msg, long duration) {
        if (duration == -1)
            System.out.println("KCore " + msg);
        else
            System.out.println("KCore " + msg + ", duration: " + duration + " ms");
    }

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 10;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");

        GraphUtils.setAppName(conf, "KTrussKCoreSparkUpdateList-" + k + "-MultiSteps", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, int[].class, Iterable.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);
        JavaPairRDD<Integer, int[]> neighbors = neighborList(new HashPartitioner(partition), edges);
        KCore kCore = new KCoreNeighborList(neighbors, k, new HashPartitioner(partition));
        kCore.start();

    }

    public static JavaPairRDD<Integer, int[]> neighborList(Partitioner partitioner, JavaPairRDD<Integer, Integer> edges) {
        return edges.groupByKey(partitioner).mapToPair(t -> {
            IntSet set = new IntOpenHashSet();
            for (Integer v : t._2) {
                set.add(v.intValue());
            }
            return new Tuple2<>(t._1, set.toIntArray());
        }).cache();
    }

}
