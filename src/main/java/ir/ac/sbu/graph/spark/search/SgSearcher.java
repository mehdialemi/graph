package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.search.fonl.*;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class SgSearcher extends SparkApp {

    private SgConf conf;
    private EdgeLoader edgeLoader;
    private JavaPairRDD <Integer, String> labels;

    public SgSearcher(SgConf conf, EdgeLoader edgeLoader, JavaPairRDD <Integer, String> labels) {
        super(edgeLoader);
        this.conf = conf;
        this.edgeLoader = edgeLoader;
        this.labels = labels;
    }

    public int search(QFonl qFonl) {
        List <Subquery> subqueries = LocalFonlCreator.getSubqueries(qFonl);
        if (subqueries.isEmpty())
            return 0;

        Queue<Subquery> queue = new LinkedList <>(subqueries);

        NeighborList neighborList = new NeighborList(edgeLoader);
        JavaPairRDD <Integer, LabledFonlValue> lFonl = LabelFonlCreator.createLabeledFonl(neighborList, labels);

        Broadcast <Subquery> broadcast = conf.getSc().broadcast(queue.remove());

        final int splitSize = qFonl.splits.length;
        JavaPairRDD <Integer, Tuple2 <int[], int[][]>> matches = getPartials(lFonl, broadcast)
                .mapValues(v -> {
                    int[][] match = new int[splitSize][];
                    match[0] = v._2;
                    int count = v._1;
                    int[] counts = new int[splitSize];
                    counts[0] = count;
                    return new Tuple2 <>(counts, match);
                });

        int qIndex = 1;
        while (!queue.isEmpty()) {
            Subquery subquery = queue.remove();
            broadcast = conf.getSc().broadcast(subquery);
            final int splitIndex = qIndex;

            long count = matches.count();
            System.out.println("partial count: " + count);
            if (count == 0) {
                System.out.println("No match found");
                return 0;
            }

            JavaPairRDD <Integer, Tuple2 <Integer, int[]>> splitMatch = getPartials(lFonl, broadcast);
            matches = matches.join(splitMatch).mapValues(val -> {
                IntSet set = new IntOpenHashSet(val._2._2);
                val._1._2[splitIndex] = set.toIntArray();
                val._1._1[splitIndex] = val._2._1;
                return val._1;
            }).cache();

            printMatches("matches (" + splitIndex + ")", matches);
        }

        return matches.map(kv -> {
            int matchCount = 1;
            for (int sVertices : kv._2._1) {
                matchCount *= sVertices;
            }
            return matchCount;
        }).reduce((a, b) -> a + b);
    }

    private JavaPairRDD <Integer, Tuple2 <Integer, int[]>> getPartials(JavaPairRDD <Integer, LabledFonlValue> tFonl,
                                                                       Broadcast <Subquery> broadcast) {
        return tFonl.flatMapToPair(kv -> {

            List <Tuple2 <Integer, Tuple2 <Integer, Integer>>> out = new ArrayList <>();
            Subquery subquery = broadcast.getValue();

            Int2IntOpenHashMap counters = kv._2.matchSubquery(kv._1, subquery);

            for (Map.Entry <Integer, Integer> entry : counters.entrySet()) {
                out.add(new Tuple2 <>(entry.getKey(), new Tuple2 <>(entry.getValue(), kv._1)));
            }

            return out.iterator();

        }).groupByKey(tFonl.getNumPartitions())
                .mapValues(val -> {
                    IntSortedSet sortedSet = new IntAVLTreeSet();
                    int count = 0;
                    for (Tuple2 <Integer, Integer> vId : val) {
                        count += vId._1;
                        sortedSet.add(vId._2.intValue());
                    }
                    return new Tuple2 <>(count, sortedSet.toIntArray());
                }).cache();
    }

    private void printMatches(String title, JavaPairRDD <Integer, Tuple2 <int[], int[][]>> candidates) {
        List <Tuple2 <Integer, Tuple2 <int[], int[][]>>> collect = candidates.collect();
        System.out.println("((((((((((((( Candidates (count: " + collect.size() + ") ** " + title + " ** ))))))))))))))");
        for (Tuple2 <Integer, Tuple2 <int[], int[][]>> entry : collect) {
            StringBuilder str = new StringBuilder("Key: " + entry._1 + ", Values => ");
            str.append("counts: ").append(Arrays.toString(entry._2._1)).append(" ");
            for (int[] array : entry._2._2) {
                str.append(Arrays.toString(array)).append(" , ");
            }
            System.out.println(str.toString());
        }
    }

    private void printPartials(String title, JavaPairRDD <Integer, Tuple2 <Integer, int[]>> partial) {
        List <Tuple2 <Integer, Tuple2 <Integer, int[]>>> collect = partial.collect();
        System.out.println("((((((((((((( Partials (count: " + collect.size() + ") ** " + title + " ** ))))))))))))))");
        for (Tuple2 <Integer, Tuple2 <Integer, int[]>> entry : collect) {
            StringBuilder sb = new StringBuilder("Key: " + entry._1 + ", Values: ");
            sb.append(Arrays.toString(entry._2._2));
            System.out.println(sb);
        }


    }

    public static void main(String[] args) {
        SgConf conf = new SgConf(new ArgumentReader(args));
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        JavaPairRDD <Integer, String> labels = getLables(conf.getSc(), conf.getLablePath(), conf.getPartitionNum());

        SgSearcher matcher = new SgSearcher(conf, edgeLoader, labels);

        Map <Integer, List <Integer>> neighbors = new HashMap <>();
        neighbors.put(1, Arrays.asList(4, 2, 3));
        neighbors.put(2, Arrays.asList(3, 1));
        neighbors.put(3, Arrays.asList(2, 1));
        neighbors.put(4, Arrays.asList(1));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "B");
        labelMap.put(3, "C");
        labelMap.put(4, "A");

        QFonl qFonl = LocalFonlCreator.createQFonl(neighbors, labelMap);
        System.out.println("qFonl" + qFonl);

        int matches = matcher.search(qFonl);
        System.out.println("Number of matches: " + matches);

        matcher.close();
    }

    private static JavaPairRDD <Integer, String> getLables(JavaSparkContext sc, String path, int pNum) {
        return sc
                .textFile(path, pNum)
                .map(line -> line.split("\\s+"))
                .mapToPair(split -> new Tuple2 <>(Integer.parseInt(split[0]), split[1]));
    }

    private void printFonl(JavaPairRDD <Integer, LabledFonlValue> labelFonl) {
        List <Tuple2 <Integer, LabledFonlValue>> collect = labelFonl.collect();
        for (Tuple2 <Integer, LabledFonlValue> t : collect) {
            System.out.println(t);
        }
    }
}
