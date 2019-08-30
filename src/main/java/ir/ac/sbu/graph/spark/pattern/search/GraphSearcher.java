package ir.ac.sbu.graph.spark.pattern.search;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.pattern.PatternConfig;
import ir.ac.sbu.graph.spark.pattern.index.GraphIndexer;
import ir.ac.sbu.graph.spark.pattern.index.IndexRow;
import ir.ac.sbu.graph.spark.pattern.query.Query;
import ir.ac.sbu.graph.spark.pattern.query.QuerySlice;
import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import ir.ac.sbu.graph.spark.pattern.utils.PatternDebugUtils;
import ir.ac.sbu.graph.spark.pattern.utils.QuerySamples;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphSearcher extends SparkApp {
    private static final Logger logger = LoggerFactory.getLogger(GraphSearcher.class);
    private GraphIndexer graphIndexer;
    private PatternConfig config;
    private final Map<QuerySlice, JavaPairRDD<Integer, MatchCount>> sliceMatches = new HashMap<>();

    public GraphSearcher(PatternConfig config) {
        super(config.getSparkAppConf());
        this.config = config;

        this.graphIndexer = new GraphIndexer(config);
        this.graphIndexer.getIndex(); // load index rdd for the first time
    }

    public long search(Query query) {
        List<QuerySlice> querySlices = query.getQuerySlices();
        logger.info("Query: {}", query);

        if (querySlices.isEmpty())
            throw new RuntimeException("No query slice found");

        for (QuerySlice querySlice : querySlices) {
            JavaPairRDD<Integer, MatchCount> sliceMatch;

            if (querySlice.isProcessed()) {
                // if the current query slice and all of its links are processed
                // then there are nothing to do
                if (!querySlice.hasNotProcessedLink())
                    continue;

                // if there are some unprocessed links, then we can get the result of
                // current query slice from the map of computed matches.
                sliceMatch = sliceMatches.get(querySlice);
                logger.info("Match count from the sliceMatch, for vertex: {}", querySlice.getV());

            } else {
                // the current query slice is not processed, then it should be processed
                sliceMatch = findMatchCounts(config.getSparkAppConf().getJavaSparkContext(),
                        querySlice, graphIndexer.getIndex());
                long count = sliceMatch.count();

                logger.info("slice match count: {}, for vertex: {}", count, querySlice.getV());
                if (count == 0)
                    return 0;

                querySlice.setProcessed(true);
                sliceMatches.put(querySlice, sliceMatch);
                PatternDebugUtils.printSliceMatch(sliceMatch, querySlice.getV());
            }

            // find the matchIndices for the links
            for (Tuple2<Integer, QuerySlice> link : querySlice.getLinks()) {

                QuerySlice sliceLink = link._2;
                if (sliceLink.isProcessed())
                    continue;

                final int linkIndex = link._1;
                JavaPairRDD<Integer, Iterable<MatchCount>> leftKey = sliceMatch
                        .filter(kv -> kv._2.equalLink(linkIndex))
                        .groupByKey();

                PatternDebugUtils.printFonlLeft(leftKey);

                JavaPairRDD<Integer, IndexRow> indexSubset = leftKey
                        .join(graphIndexer.getIndex())
                        .mapValues(v -> v._2);

                PatternDebugUtils.printFonlSubset(indexSubset);

                JavaPairRDD<Integer, MatchCount> linkSliceMatch = findMatchCounts(
                        config.getSparkAppConf().getJavaSparkContext(), sliceLink, indexSubset);

                // if nothing matched then no match exist
                long count = linkSliceMatch.count();
                logger.info("link match, parent: {}, link: {}, count: {}", querySlice.getV(),
                        sliceLink.getV(), count);
                if (count == 0)
                    return 0;

                PatternDebugUtils.printLinkSliceMatch(linkSliceMatch);

                querySlice.setProcessed(true);
                sliceMatches.put(querySlice, linkSliceMatch);
                PatternDebugUtils.printSliceMatch(linkSliceMatch, querySlice.getV());
            }
        }

        for (QuerySlice querySlice : querySlices) {
            querySlice.setProcessed(false);
        }

        JavaPairRDD<Integer, Long> counter = counter(querySlices.get(0));

        return counter.map(v -> v._2).reduce(Long::sum);
    }

    private JavaPairRDD<Integer, Long> counter(QuerySlice querySlice) {

        JavaPairRDD<Integer, Long> counts = sliceMatches
                .get(querySlice)
                .mapValues(v -> v.getCount());

        if (querySlice.getLinks().isEmpty())
            return counts;

        JavaPairRDD<Integer, Long> result = counts.mapValues(v -> 0L);
        for (Tuple2<Integer, QuerySlice> link : querySlice.getLinks()) {
            JavaPairRDD<Integer, Long> count = counter(link._2);
            JavaPairRDD<Integer, Long> countJoin = counts.join(count).mapValues(v -> v._1 * v._2);
            result = result.leftOuterJoin(countJoin).mapValues(v -> v._1 + v._2.orElse(0L));
        }

        return result;
    }

    /**
     * find matches based on the given ldtFonlRdd and querySlice.
     *
     * @param sc         is used to broadcast subquery
     * @param querySlice the current query part to constructIndex subquery
     * @return key-values => (key: vertex, Tuple3<>(value: source fonl key, linkIndex, count of matches for the key))
     */
    private JavaPairRDD<Integer, MatchCount> findMatchCounts(final JavaSparkContext sc, QuerySlice querySlice,
                                                          JavaPairRDD<Integer, IndexRow> indexRDD) {

        Broadcast<Subquery> broadcast = sc.broadcast(querySlice.subquery());
        return indexRDD.flatMapToPair(kv -> {

            // get the subquery from the broadcast data
            Subquery subquery = broadcast.getValue();

            // get all of the matches for the current subquery
            return kv._2.counts(subquery);
        }).cache();
    }

    public static void main(String[] args) {
        Config conf = ConfigFactory.load();
        if (args.length > 0)
            conf = ConfigFactory.parseFile(new File(args[0]));

        PatternConfig config = new PatternConfig(conf, "search");
        Query querySample = QuerySamples.getSample(config.getQuerySample());
        for (QuerySlice querySlice : querySample.getQuerySlices()) {
            Subquery subquery = querySlice.subquery();
            logger.info("Subquery: {}", subquery);
        }

        GraphSearcher matcher = new GraphSearcher(config);
        long count = matcher.search(querySample);
        logger.info("final match count:{} ", count);
    }
}
