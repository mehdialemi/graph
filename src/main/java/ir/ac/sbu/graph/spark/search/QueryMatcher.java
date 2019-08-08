package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.SparkAppConf;
import ir.ac.sbu.graph.spark.search.fonl.creator.LabelTriangleFonl;
import ir.ac.sbu.graph.spark.search.fonl.creator.TriangleFonl;
import ir.ac.sbu.graph.spark.search.fonl.value.LabelDegreeTriangleFonlValue;
import ir.ac.sbu.graph.spark.search.patterns.*;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileNotFoundException;
import java.util.*;

public class QueryMatcher extends SparkApp {
    private SparkAppConf sparkConf;
    private EdgeLoader edgeLoader;
    private JavaPairRDD<Integer, String> labels;

    public QueryMatcher(SparkAppConf sparkConf, EdgeLoader edgeLoader,
                        JavaPairRDD<Integer, String> labels) {
        super(edgeLoader);
        this.sparkConf = sparkConf;
        this.edgeLoader = edgeLoader;
        this.labels = labels;
    }

    public long search(Query query) {
        List<QuerySlice> querySlices = query.getQuerySlices();
        System.out.println("Query: " + query);
        if (querySlices.isEmpty())
            throw new RuntimeException("No query slice found");

        NeighborList neighborList = new NeighborList(edgeLoader);
        TriangleFonl triangleFonl = new TriangleFonl(neighborList);

        LabelTriangleFonl labelTriangleFonl = new LabelTriangleFonl(triangleFonl, labels);
        JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> ldtFonlRDD = labelTriangleFonl.create();
//        PatternDebugUtils.printFonlLabelDegreeTriangleFonlValue(ldtFonlRDD);

        Map<QuerySlice, JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>>> sliceMatches = new HashMap<>();
        JavaPairRDD<Integer, Long> matchCounter = ldtFonlRDD.mapValues(v -> 1L);

        for (QuerySlice querySlice : querySlices) {
            JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> sliceMatch;

            if (querySlice.isProcessed()) {
                // if the current query slice and all of its links are processed then there are nothing to do
                if (!querySlice.hasNotProcessedLink())
                    continue;

                // if there are some unprocessed links, then we can get the result of current query slice from the
                // map of computed matches.
                sliceMatch = sliceMatches.get(querySlice);
                System.out.println("retrieve match count from the sliceMatch, for vertex: " + querySlice.getV());
            } else { // if the current query slice is not processed, then it should be processed
                sliceMatch = findMatchCounts(sparkConf.getSc(), ldtFonlRDD, querySlice);

                long count = sliceMatch.count();
                System.out.println("slice match count: " + count + ", for vertex: " + querySlice.getV());
                if (count == 0)
                    return 0;

                matchCounter = update(sliceMatches, matchCounter, querySlice, sliceMatch);
            }

            // find the matchIndices for the links
            for (Tuple2<Integer, QuerySlice> link : querySlice.getLinks()) {

                QuerySlice sliceLink = link._2;
                if (sliceLink.isProcessed())
                    continue;

                final int fonlValueIndex = link._1;
                JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> fonlLeftKeys =
                        sliceMatch.filter(kv -> kv._2._2().equals(fonlValueIndex))
                        .distinct();

                JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> fonlSubset =
                        fonlLeftKeys.join(ldtFonlRDD).mapValues(v -> v._2);

                JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> linkSliceMatch =
                        findMatchCounts(sparkConf.getSc(), fonlSubset, sliceLink);

                // if nothing matched then no match exist
                long count = linkSliceMatch.count();
                System.out.println("link match, parent: " + querySlice.getV() + ", link: " + sliceLink.getV() +
                        ", count: " + count);
                if (count == 0)
                    return 0;

                matchCounter = update(sliceMatches, matchCounter, querySlice, linkSliceMatch);
            }
        }

        return matchCounter.map(kv -> kv._2).reduce(Long::sum);
    }

    private JavaPairRDD<Integer, Long> update(
            Map<QuerySlice, JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>>> sliceMatches,
            JavaPairRDD<Integer, Long> matchCounter, QuerySlice querySlice,
            JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> linkSliceMatch) {

        querySlice.setProcessed(true);
        sliceMatches.put(querySlice, linkSliceMatch);
//        PatternDebugUtils.printSliceMatch(linkSliceMatch, querySlice.getV());

        matchCounter = updateMatchCounter(matchCounter, linkSliceMatch);
//        PatternDebugUtils.printMatchCounter(matchCounter, querySlice.getV());

        return matchCounter;
    }

    private JavaPairRDD<Integer, Long> updateMatchCounter(
            JavaPairRDD<Integer, Long> matchCounter,
            JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> matchCounts) {

        return matchCounts
                .mapToPair(kv -> new Tuple2<>(kv._2._1(), kv._2._3()))
                .join(matchCounter)
                .mapValues(v -> v._1 * v._2)
                .cache();
    }

    /**
     * find matches based on the given ldtFonlRdd and querySlice.
     * @param sc is used to broadcast subquery
     * @param ldtFonlRDD base data to find matches
     * @param querySlice the current query part to create subquery
     * @return key-values => (key: vertex, Tuple3<>(value: source fonl key, linkIndex, count of matches for the key))
     */
    private JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> findMatchCounts(
            final JavaSparkContext sc, final JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> ldtFonlRDD,
            QuerySlice querySlice) {

        Broadcast<Subquery> broadcast = sc.broadcast(querySlice.subquery());
        return ldtFonlRDD.flatMapToPair(kv -> {
            // get the subquery from the broadcast data
            Subquery subquery = broadcast.getValue();

            // get all of the matches for the current subquery
            Set<int[]> matchIndices = kv._2.matchIndices(subquery);
            if (matchIndices == null)
                return Collections.emptyIterator();

            List<Tuple2<Integer, Tuple3<Integer, Integer, Integer>>> out = new ArrayList<>();

            if (subquery.linkIndices.length > 0) {
                Int2IntOpenHashMap[] countIndex = new Int2IntOpenHashMap[subquery.linkIndices.length];
                for (int i = 0; i < countIndex.length; i++) {
                    Int2IntOpenHashMap count = new Int2IntOpenHashMap();
                    for (int[] matchIndex : matchIndices) {
                        int index = matchIndex[subquery.linkIndices[i]];
                        int neighbor = kv._2.fonl[index];
                        count.addTo(neighbor, 1);
                    }
                    countIndex[i] = count;
                }

                for (int i = 0; i < countIndex.length; i++) {
                    Int2IntOpenHashMap count = countIndex[i];
                    for (Map.Entry<Integer, Integer> entry : count.entrySet()) {
                        out.add(new Tuple2<>(entry.getKey(),
                                new Tuple3<>(kv._2.getSource(), subquery.linkIndices[i], entry.getValue())));
                    }
                }
            } else {
                out.add(new Tuple2<>(kv._1, new Tuple3<>(kv._2.getSource(), 0, matchIndices.size())));
            }

            return out.iterator();
        }).cache();
    }

    static JavaPairRDD <Integer, String> getLabels(JavaSparkContext sc, String path, int pNum) {
        if (path.isEmpty()) {
            return null;
//            List<Tuple2<Integer, String>> list = new ArrayList <>();
//            list.add(new Tuple2 <>(Integer.MAX_VALUE, "_"));
//            return sc.parallelizePairs(list);
        }

        return sc
                .textFile(path, pNum)
                .map(line -> line.split("\\s+"))
                .mapToPair(split -> new Tuple2 <>(Integer.parseInt(split[0]), split[1]));
    }

    public static void main(String[] args) throws FileNotFoundException {
        SearchConfig searchConfig = SearchConfig.load(args[0]);
        SparkAppConf sparkConf = searchConfig.getSparkAppConf();
        sparkConf.init();

        EdgeLoader edgeLoader = new EdgeLoader(sparkConf);
        JavaPairRDD<Integer, String> labels = getLabels(sparkConf.getSc(),
                searchConfig.getGraphLabelPath(), sparkConf.getPartitionNum());
        Query query = Samples.mySampleEmptyLabel();

        QueryMatcher matcher = new QueryMatcher(sparkConf, edgeLoader, labels);
        long count = matcher.search(query);
        System.out.println("final match count: " + count);
    }


}
