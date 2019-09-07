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
import ir.ac.sbu.graph.spark.pattern.utils.QuerySamples;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.util.*;

public class GraphSearcher extends SparkApp {
    private static final Logger logger = LoggerFactory.getLogger(GraphSearcher.class);
    private PatternConfig config;
    private final Map<QuerySlice, JavaPairRDD<Integer, MatchCount>> sliceMatches;
    private final JavaPairRDD<Integer, IndexRow> index;
    private long searchTime;

    public GraphSearcher(PatternConfig config) {
        super(config.getSparkAppConf());
        this.config = config;
        this.sliceMatches = new HashMap<>();
        index = new GraphIndexer(config).getIndex(); // load index rdd for the first time
    }

    public long getSearchTime() {
        return searchTime;
    }

    public long search(Query query) {
        List<QuerySlice> querySlices = query.getQuerySlices();

        if (querySlices.isEmpty())
            throw new RuntimeException("No query slice found");

        searchTime = 0;
        Queue<QuerySlice> querySliceQueue = new LinkedList<>(querySlices);
        while (!querySliceQueue.isEmpty()) {
            long start = System.currentTimeMillis();
            QuerySlice querySlice = querySliceQueue.poll();
            Subquery subquery = querySlice.subquery();


            JavaPairRDD<Integer, IndexRow> index;
            if (querySlice.hasParent()) {
                QuerySlice parentQuerySlice = querySlice.getParentVertex();
                int linkIndex = parentQuerySlice.getLinkIndex(querySlice);

                final Broadcast<Integer> linkIndexBroadcast = config
                        .getSparkContext()
                        .broadcast(linkIndex);

                JavaPairRDD<Integer, MatchCount> parent = sliceMatches.get(parentQuerySlice);
                index = parent
                        .filter(kv -> kv._2.equalLink(linkIndexBroadcast.getValue()))
                        .groupByKey(config.getPartitionNum())
                        .distinct()
                        .join(this.index)
                        .mapValues(v -> v._2)
                        .repartition(config.getPartitionNum())
                        .persist(config.getSparkAppConf().getStorageLevel());

            } else {
                index = this.index;
            }

            long indexRows = index.count();
            JavaPairRDD<Integer, MatchCount> matches = matches(index, subquery);
            long matchCount = matches.count();
            long duration = System.currentTimeMillis() - start;
            searchTime += duration;
            logger.info("(SBM) index rows: {}, match count: {}, " +
                            "subquery vertex: {}, duration: {} ms",
                    indexRows, matchCount, querySlice.getV(), duration);

            if (matchCount == 0)
                return 0;

            querySlice.setProcessed(true);
            sliceMatches.put(querySlice, matches);
        }


        long start = System.currentTimeMillis();
        JavaPairRDD<Integer, Long> counter = counter(querySlices.get(0));
        long matchCount = counter.map(v -> v._2).reduce(Long::sum);
        long duration = System.currentTimeMillis() - start;

        logger.info("(SBM) match count: {}, duration: {} ms", matchCount, duration);

        return matchCount;
    }

    private JavaPairRDD<Integer, Long> counter(QuerySlice querySlice) {

        JavaPairRDD<Integer, Long> counts = sliceMatches
                .get(querySlice)
                .mapValues(MatchCount::getCount);

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
     * @param subquery the current query part to constructIndex subquery
     * @return key-values => (key: vertex, Tuple3<>(value: source fonl key, linkIndex, count of matches for the key))
     */
    private JavaPairRDD<Integer, MatchCount> matches(JavaPairRDD<Integer, IndexRow> indexRDD,
                                                     Subquery subquery) {


        Broadcast<Subquery> subqueryBroadcast = config.getSparkContext().broadcast(subquery);
        return indexRDD.flatMapToPair(kv ->
                new PatternCounter(kv._2, subqueryBroadcast.getValue()).counts())
                .persist(config.getSparkAppConf().getStorageLevel());
    }

    public static void main(String[] args) {
        Config conf = ConfigFactory.load();
        if (args.length > 0)
            conf = ConfigFactory.parseFile(new File(args[0]));

        PatternConfig config = new PatternConfig(conf, "search");
        Query querySample = QuerySamples.getSample(config.getQuerySample());

        GraphSearcher matcher = new GraphSearcher(config);
        long start = System.currentTimeMillis();
        matcher.search(querySample);
        long duration = System.currentTimeMillis() - start;
        logger.info("(SBM) total duration: {} ms, search time: {} ms", duration, matcher.getSearchTime());
    }
}
