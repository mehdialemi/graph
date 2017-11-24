package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Calculate local clustering coefficient per vertex
 */
public class LocalCC extends SparkApp {

    public LocalCC(SparkAppConf conf) {
        super(conf);
    }

    public JavaPairRDD<Integer, Float> generate() {
        EdgeLoader edgeLoader = new EdgeLoader(conf);

        NeighborList neighborList = new NeighborList(edgeLoader);

        Triangle triangle = new Triangle(neighborList);

        JavaPairRDD<Integer, int[]> fonl = triangle.createFonl(5);

        JavaPairRDD<Integer, int[]> candidates = triangle.generateCandidates(fonl);

        JavaPairRDD<Integer, Integer> tVertex = candidates.cogroup(fonl, fonl.getNumPartitions()).flatMapToPair(t -> {
            Iterator<int[]> iterator = t._2._2.iterator();
            List<Tuple2<Integer, Integer>> output = new ArrayList<>();
            if (!iterator.hasNext())
                return output.iterator();

            int[] hDegs = iterator.next();

            iterator = t._2._1.iterator();
            if (!iterator.hasNext())
                return output.iterator();

            Arrays.sort(hDegs, 1, hDegs.length);

            int sum = 0;
            do {
                int[] forward = iterator.next();
                int count = GraphUtils.sortedIntersectionCountInt(hDegs, forward, output, 1, 1);
                if (count > 0) {
                    sum += count;
                    output.add(new Tuple2<>(forward[0], count));
                }
            } while (iterator.hasNext());

            if (sum > 0) {
                output.add(new Tuple2<>(t._1, sum));
            }

            return output.iterator();
        }).reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, Float> localCC = fonl
                .leftOuterJoin(tVertex)
                .mapValues(v -> (!v._2.isPresent() || v._1[0] < 2) ? 0.0f : 2.0f * v._2.get() / (v._1[0] * (v._1[0] - 1)));

        return localCC;
    }

    public static void main(String[] args) {

        long t1 = System.currentTimeMillis();
        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args));
        conf.init();

        LocalCC lcc = new LocalCC(conf);
        JavaPairRDD<Integer, Float> lccGraph = lcc.generate();


        Float sum = lccGraph.map(kv -> kv._2).reduce((a, b) -> a + b);
        long count = lccGraph.count();

        long t2 = System.currentTimeMillis();

        log("Average lcc: " + (sum / count), t1, t2);

        lcc.close();
    }
}
