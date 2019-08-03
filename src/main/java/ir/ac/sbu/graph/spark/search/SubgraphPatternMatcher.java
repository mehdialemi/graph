package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.SparkAppConf;
import ir.ac.sbu.graph.spark.search.fonl.creator.LabelTriangleFonl;
import ir.ac.sbu.graph.spark.search.fonl.creator.LocalFonlCreator;
import ir.ac.sbu.graph.spark.search.fonl.creator.TriangleFonl;
import ir.ac.sbu.graph.spark.search.fonl.local.QFonl;
import ir.ac.sbu.graph.spark.search.fonl.local.Subquery;
import ir.ac.sbu.graph.spark.search.fonl.value.LabelDegreeTriangleFonlValue;
import ir.ac.sbu.graph.spark.search.patterns.PatternReaderUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.util.*;

public class SubgraphPatternMatcher extends SparkApp {
    private SearchConfig searchConfig;
    private SparkAppConf sparkConf;
    private EdgeLoader edgeLoader;
    private JavaPairRDD<Integer, String> labels;

    public SubgraphPatternMatcher(SearchConfig searchConfig, SparkAppConf sparkConf, EdgeLoader edgeLoader,
                                  JavaPairRDD<Integer, String> labels) {
        super(edgeLoader);
        this.searchConfig = searchConfig;
        this.sparkConf = sparkConf;
        this.edgeLoader = edgeLoader;
        this.labels = labels;
    }

    public long search(QFonl qFonl) {
        List<Subquery> subQueries = LocalFonlCreator.getSubQueries(qFonl);
        if (subQueries.isEmpty())
            return 0;

        NeighborList neighborList = new NeighborList(edgeLoader);
        TriangleFonl triangleFonl = new TriangleFonl(neighborList);
        LabelTriangleFonl labelTriangleFonl = new LabelTriangleFonl(triangleFonl, labels);
        JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> ldtFonlRDD = labelTriangleFonl.create();

        Queue<Subquery> queue = new LinkedList<>(subQueries);
        Broadcast<Subquery> broadcast = sparkConf.getSc().broadcast(queue.remove());
        JavaPairRDD<Integer, String> subMatches = getSubMatches(ldtFonlRDD, broadcast);
        long count = subMatches.count();
        System.out.println("First match count: " + count);

        List<JavaPairRDD<Integer, String>> matches = new ArrayList<>();
        matches.add(subMatches);

        while (count != 0 && !queue.isEmpty()) {
            Subquery subquery = queue.remove();
            broadcast = sparkConf.getSc().broadcast(subquery);

            JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> rdd = subMatches
                    .join(ldtFonlRDD)
                    .mapValues(v -> v._2)
                    .repartition(ldtFonlRDD.getNumPartitions());

            subMatches = getSubMatches(rdd, broadcast);
            count = subMatches.count();
            matches.add(subMatches);
            System.out.println("Match count: " + count);
        }

        if (!queue.isEmpty())
            return 0;

        JavaPairRDD<Integer, String> left = matches.get(0);

        for (int i = 1; i < matches.size(); i++) {
            JavaPairRDD<Integer, Tuple2<Integer, String>> right = matches.get(i)
                    .mapToPair(kv -> new Tuple2<>(
                            Integer.parseInt(kv._2.split(" ")[0]),
                            new Tuple2<>(kv._1, kv._2)
                    ));

            left = left.join(right)
                    .mapToPair(kv -> new Tuple2<>(kv._2._2._1, kv._2._1.concat(kv._2._2._2)))
                    .cache();

            count = left.count();
            System.out.println("Current count of match: " + count);
        }

        return count;
    }

    private JavaPairRDD<Integer, String> getSubMatches(JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> tFonl,
                                                                    Broadcast<Subquery> broadcast) {
        return tFonl.flatMapToPair(kv -> {
            // vertex to count
            Set<Tuple2<String, Integer>> matches = kv._2.matches(kv._1, broadcast.getValue());
            if (matches == null)
                return Collections.emptyIterator();

            return matches.iterator();
        }).groupByKey(tFonl.getNumPartitions()).flatMapToPair(kv -> {
            Set<Integer> set = new HashSet<>();
            for (Integer v : kv._2) {
                set.add(v);
            }

            List<Tuple2<Integer, String>> out = new ArrayList<>();
            for (Integer v : set) {
                out.add(new Tuple2<>(v, kv._1));
            }

            return out.iterator();
        }).cache();
    }

    public static void main(String[] args) throws FileNotFoundException {
        SearchConfig searchConfig = SearchConfig.load(args[0]);
        SparkAppConf sparkConf = searchConfig.getSparkAppConf();
        sparkConf.init();

        QFonl qFonl = PatternReaderUtils.loadSample(searchConfig.getSampleName());
        System.out.println("qFonl" + qFonl);

        EdgeLoader edgeLoader = new EdgeLoader(sparkConf);

        JavaPairRDD<Integer, String> labels = PatternCounter.getLabels(sparkConf.getSc(),
                searchConfig.getGraphLabelPath(), sparkConf.getPartitionNum());

        SubgraphPatternMatcher matcher = new SubgraphPatternMatcher(searchConfig, sparkConf, edgeLoader, labels);
        long count = matcher.search(qFonl);
        System.out.println("Number of matches: " + count);
    }
}
