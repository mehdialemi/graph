package ir.ac.sbu.graph.spark.pattern.search;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.pattern.PatternConfig;
import ir.ac.sbu.graph.spark.pattern.index.GraphIndex;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleFonlValue;
import ir.ac.sbu.graph.spark.pattern.query.Query;
import ir.ac.sbu.graph.spark.pattern.query.QuerySlice;
import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import ir.ac.sbu.graph.spark.pattern.utils.PatternDebugUtils;
import ir.ac.sbu.graph.spark.pattern.utils.QuerySamples;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.util.*;

public class QueryMatcher extends SparkApp {
    private static final Logger logger = LoggerFactory.getLogger(QueryMatcher.class);
    private GraphIndex graphIndex;
    private PatternConfig config;

    public QueryMatcher(PatternConfig config) {
        super(config.getSparkAppConf());
        this.config = config;

        this.graphIndex = new GraphIndex(config);
        this.graphIndex.indexRDD(); // load index rdd for the first time
    }

    public long search(Query query) {
        List <QuerySlice> querySlices = query.getQuerySlices();
        logger.info("Query: {}", query);

        if (querySlices.isEmpty())
            throw new RuntimeException("No query slice found");

        Map <QuerySlice, JavaPairRDD <Integer, Tuple3 <Integer, Integer, Integer>>> sliceMatches = new HashMap <>();

        for (QuerySlice querySlice : querySlices) {
            JavaPairRDD <Integer, Tuple3 <Integer, Integer, Integer>> sliceMatch;

            if (querySlice.isProcessed()) {
                // if the current query slice and all of its links are processed then there are nothing to do
                if (!querySlice.hasNotProcessedLink())
                    continue;

                // if there are some unprocessed links, then we can get the result of current query slice from the
                // map of computed matches.
                sliceMatch = sliceMatches.get(querySlice);
                logger.info("retrieve match count from the sliceMatch, for vertex: {}", querySlice.getV());

            } else { // if the current query slice is not processed, then it should be processed

                sliceMatch = findMatchCounts(config.getSparkAppConf().getSc(), querySlice, graphIndex.indexRDD());
                long count = sliceMatch.count();

                logger.info("slice match count: {}, for vertex: {}", count, querySlice.getV());
                if (count == 0)
                    return 0;

                querySlice.setProcessed(true);
                sliceMatches.put(querySlice, sliceMatch);
                PatternDebugUtils.printSliceMatch(sliceMatch, querySlice.getV());
            }

            // find the matchIndices for the links
            for (Tuple2 <Integer, QuerySlice> link : querySlice.getLinks()) {

                QuerySlice sliceLink = link._2;
                if (sliceLink.isProcessed())
                    continue;

                final int fonlValueIndex = link._1;
                JavaPairRDD <Integer, Iterable <Tuple3 <Integer, Integer, Integer>>> fonlLeftKeys =
                        sliceMatch.filter(kv -> kv._2._2().equals(fonlValueIndex)).groupByKey();
                PatternDebugUtils.printFonlLeft(fonlLeftKeys);

                JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> indexSubset = fonlLeftKeys
                        .join(graphIndex.indexRDD())
                        .mapValues(v -> v._2);
                PatternDebugUtils.printFonlSubset(indexSubset);

                JavaPairRDD <Integer, Tuple3 <Integer, Integer, Integer>> linkSliceMatch =
                        findMatchCounts(config.getSparkAppConf().getSc(), sliceLink, indexSubset);

                // if nothing matched then no match exist
                long count = linkSliceMatch.count();
                logger.info("link match, parent: {}, link: {}, count: {}", querySlice.getV(), sliceLink.getV(), count);
                if (count == 0)
                    return 0;

                PatternDebugUtils.printLinkSliceMatch(linkSliceMatch);

                querySlice.setProcessed(true);
                sliceMatches.put(querySlice, linkSliceMatch);
                PatternDebugUtils.printSliceMatch(linkSliceMatch, querySlice.getV());
            }
        }

        return 1;
    }

    /**
     * find matches based on the given ldtFonlRdd and querySlice.
     *
     * @param sc         is used to broadcast subquery
     * @param querySlice the current query part to constructIndex subquery
     * @return key-values => (key: vertex, Tuple3<>(value: source fonl key, linkIndex, count of matches for the key))
     */
    private JavaPairRDD <Integer, Tuple3 <Integer, Integer, Integer>> findMatchCounts(
            final JavaSparkContext sc, QuerySlice querySlice,
            JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> indexRDD) {

        Broadcast <Subquery> broadcast = sc.broadcast(querySlice.subquery());
        return indexRDD.flatMapToPair(kv -> {

            // get the subquery from the broadcast data
            Subquery subquery = broadcast.getValue();

            // get all of the matches for the current subquery
            Set <int[]> matchIndices = kv._2.matchIndices(subquery);
            if (matchIndices == null)
                return Collections.emptyIterator();

            List <Tuple2 <Integer, Tuple3 <Integer, Integer, Integer>>> out = new ArrayList <>();

            if (subquery.linkIndices.length > 0) {
                Int2IntOpenHashMap[] countIndex = new Int2IntOpenHashMap[subquery.linkIndices.length];
                for (int i = 0; i < countIndex.length; i++) {
                    Int2IntOpenHashMap count = new Int2IntOpenHashMap();
                    for (int[] matchIndex : matchIndices) {
                        int index = matchIndex[subquery.linkIndices[i]];
                        int neighbor;
                        if (index == -1) {
                            neighbor = kv._1;
                        } else {
                            neighbor = kv._2.fonl[index];
                        }

                        count.addTo(neighbor, 1);
                    }
                    countIndex[i] = count;
                }

                for (int i = 0; i < countIndex.length; i++) {
                    Int2IntOpenHashMap count = countIndex[i];
                    for (Map.Entry <Integer, Integer> entry : count.entrySet()) {
                        out.add(new Tuple2 <>(entry.getKey(),
                                new Tuple3 <>(kv._2.getSource(), subquery.linkIndices[i], entry.getValue())));
                    }
                }
            } else {
                out.add(new Tuple2 <>(kv._1, new Tuple3 <>(kv._2.getSource(), 0, matchIndices.size())));
            }

            return out.iterator();
        }).cache();
    }

    public static void main(String[] args) {
        Config conf = ConfigFactory.load();
        if (args.length == 0)
            conf = ConfigFactory.parseFile(new File(args[0]));

        PatternConfig config = new PatternConfig(conf);
        Query querySample = QuerySamples.getSample(config.getQuerySample());
        QueryMatcher matcher = new QueryMatcher(config);
        long count = matcher.search(querySample);
        logger.info("final match count:{} ", count);
    }
}
