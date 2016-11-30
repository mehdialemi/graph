package ir.ac.sbu.graph.clusteringco;

import ir.ac.sbu.graph.GraphLoader;
import ir.ac.sbu.graph.GraphUtils;
import ir.ac.sbu.graph.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Calculate LCC with id based ordering. In this method we have no dealing with degree of nodes and to quickSort nodes in
 * the fonl just id of the nodes are taken into account.
 */
public class FonlIdLCC {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
        if (args != null && args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args != null && args.length > 1) {
            partition = Integer.parseInt(args[1]);
        }

        final int batchSize = 1000;

        SparkConf conf = new SparkConf();
        if (args == null || args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "Fonl-LCC-Id", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.CandidateState.class, int[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createFonlIdBasedInt(edges, partition);
        long totalNodes = fonl.count();

        JavaPairRDD<Integer, GraphUtils.CandidateState> candidates = fonl.flatMapToPair(tuple -> {
            List<Tuple2<Integer, GraphUtils.CandidateState>> output = new ArrayList<>();
            int[] higherIds = new int[tuple._2.length - 1];
            if (higherIds.length < 2)
                return Collections.emptyIterator();

            System.arraycopy(tuple._2, 1, higherIds, 0, higherIds.length);

            GraphUtils.CandidateState candidateState = new GraphUtils.CandidateState(tuple._1, higherIds);
            int size = tuple._2.length - 1;
            int split = batchSize;
            if (size < 1000)
                split = Integer.MAX_VALUE;
            for (int index = 1; index < size; index++) {
                if (split != 0) {
                    if (index % split == 0) {
                        int blockSize = tuple._2.length - (index + 1);
                        int[] block = new int[blockSize];
                        System.arraycopy(tuple._2, index + 1, block, 0, blockSize);
                        candidateState = new GraphUtils.CandidateState(tuple._1, block);
                    }
                }
                output.add(new Tuple2<>(tuple._2[index], candidateState));
            }

            if (output.size() == 0)
                return Collections.emptyIterator();

            return output.iterator();
        });

        JavaPairRDD<Integer, Integer> localTriangleCount = candidates.cogroup(fonl, partition)
            .flatMapToPair(t -> {
                Iterator<int[]> higherIdsIter = t._2._2.iterator();
                List<Tuple2<Integer, Integer>> output = new ArrayList<>();
                if (!higherIdsIter.hasNext())
                    return Collections.emptyIterator();

                int[] higherIds = higherIdsIter.next();

                Iterator<GraphUtils.CandidateState> candidateIter = t._2._1.iterator();
                if (!candidateIter.hasNext())
                    return Collections.emptyIterator();

                int sum = 0;
                do {
                    GraphUtils.CandidateState candidates1 = candidateIter.next();
                    int count = GraphUtils.sortedIntersectionCountInt(higherIds, candidates1.higherIds, output, 0, 0);
                    if (count > 0) {
                        sum += count;
                        output.add(new Tuple2<>(candidates1.firstNodeId, count));
                    }
                } while (candidateIter.hasNext());

                if (sum > 0) {
                    output.add(new Tuple2<>(t._1, sum));
                }

                if (output.size() == 0)
                    return Collections.emptyIterator();

                return output.iterator();
            }).reduceByKey((a, b) -> a + b);

        Float sumLCC = localTriangleCount.filter(t -> t._2 > 0).join(fonl, partition)
            .mapValues(t -> 2 * t._1 / (float) (t._2[0] * (t._2[0] - 1)))
            .map(t -> t._2)
            .reduce((a, b) -> a + b);

        float avgLCC = sumLCC / totalNodes;
        OutUtils.printOutputLCC(totalNodes, sumLCC, avgLCC);

        sc.close();
    }
}
