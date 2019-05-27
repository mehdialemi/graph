package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.fonl.matcher.LabelMeta;
import ir.ac.sbu.graph.fonl.SparkFonlCreator;
import ir.ac.sbu.graph.fonl.matcher.LocalFonlCreator;
import ir.ac.sbu.graph.fonl.matcher.QFonl;
import ir.ac.sbu.graph.fonl.matcher.TFonlValue;
import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class SGMatcher extends SparkApp {

    private SGMatcherConf conf;
    private EdgeLoader edgeLoader;
    private JavaPairRDD <Integer, String> labels;

    public SGMatcher(SGMatcherConf conf, EdgeLoader edgeLoader, JavaPairRDD <Integer, String> labels) {
        super(edgeLoader);
        this.conf = conf;
        this.edgeLoader = edgeLoader;
        this.labels = labels;
    }

    public int search(QFonl qFonl) {

        NeighborList neighborList = new NeighborList(edgeLoader);
        JavaPairRDD <Integer, Fvalue <LabelMeta>> lFonl = SparkFonlCreator.createLabelFonl(neighborList, labels);
        printFonl(lFonl);

        JavaPairRDD <Integer, TFonlValue> tFonl = SparkFonlCreator.createTFonl(lFonl);

        Broadcast <QFonl> broadcast = conf.getSc().broadcast(qFonl);

        JavaPairRDD <Integer, int[][]> candidates = getPartials(tFonl, broadcast, 0)
                .mapValues(v -> {
                    int[][] match = new int[broadcast.getValue().splits.length][];
                    match[0] = v;
                    return match;
                });
        printCandidates("split 0", candidates);

        for (int i = 1; i < qFonl.splits.length; i++) {

            final int splitIndex = i;
            long count = candidates.count();
            System.out.println("partial count: " + count);

            if (count == 0) {
                System.out.println("No match found");
                return 0;
            }

            JavaPairRDD <Integer, int[]> partials = getPartials(tFonl, broadcast, splitIndex);
            printPartials("split" + splitIndex, partials);

            candidates = candidates.join(partials).mapValues(val -> {
                IntSet set = new IntOpenHashSet(val._2);
                val._1[splitIndex] = set.toIntArray();
                return val._1;
            }).cache();

            printCandidates("split " + splitIndex, candidates);

        }

        return candidates.map(kv -> {
            int matchCount = 1;
            for (int[] sVertices : kv._2) {
                matchCount *= sVertices.length;
            }
            return matchCount;
        }).reduce((a, b) -> a + b);
    }

    private JavaPairRDD <Integer, int[]> getPartials(JavaPairRDD <Integer, TFonlValue> tFonl,
                                                     Broadcast <QFonl> broadcast, int splitIndex) {
        return tFonl.flatMapToPair(kv -> {

            List <Tuple2 <Integer, Integer>> out = new ArrayList <>();
            for (int key : kv._2.expands(splitIndex, broadcast.getValue())) {
                out.add(new Tuple2 <>(key, kv._1));
            }

            return out.iterator();

        }).groupByKey(tFonl.getNumPartitions())
                .mapValues(val -> {
                    IntSortedSet sortedSet = new IntAVLTreeSet();
                    for (Integer vId : val) {
                        sortedSet.add(vId.intValue());
                    }
                    return sortedSet.toIntArray();
                }).cache();
    }

    private void printCandidates(String title, JavaPairRDD <Integer, int[][]> candidates) {
        List <Tuple2 <Integer, int[][]>> collect = candidates.collect();
        System.out.println("((((((((((((( Candidates (count: " + collect.size() +") ** " + title + " ** ))))))))))))))");
        for (Tuple2 <Integer, int[][]> entry : collect) {
            StringBuilder str = new StringBuilder("Key: " + entry._1 + ", Values: ");

            for (int[] array : entry._2) {
                str.append(Arrays.toString(array)).append(" , ");
            }
            System.out.println(str.toString());
        }
    }

    private void printPartials(String title, JavaPairRDD <Integer, int[]> partial) {
        List <Tuple2 <Integer, int[]>> collect = partial.collect();
        System.out.println("((((((((((((( Partials (count: " + collect.size() + ") ** " + title + " ** ))))))))))))))");
        for (Tuple2 <Integer, int[]> entry : collect) {
            StringBuilder sb = new StringBuilder("Key: " + entry._1 + ", Values: ");
            sb.append(Arrays.toString(entry._2));
            System.out.println(sb);
        }


    }

    public static void main(String[] args) {
        SGMatcherConf conf = new SGMatcherConf(new ArgumentReader(args));
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        JavaPairRDD <Integer, String> labels = getLables(conf.getSc(), conf.getLablePath(), conf.getPartitionNum());

        SGMatcher matcher = new SGMatcher(conf, edgeLoader, labels);

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
        System.out.println("qFonl" + qFonl.toString());

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

    private void printFonl(JavaPairRDD <Integer, Fvalue <LabelMeta>> labelFonl) {
        List <Tuple2 <Integer, Fvalue <LabelMeta>>> collect = labelFonl.collect();
        for (Tuple2 <Integer, Fvalue <LabelMeta>> t : collect) {
            System.out.println(t);
        }
    }
}
