package graph.clusteringco;

import graph.GraphUtils;
import graph.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Calculate LCC with id based ordering. In this method we have no dealing with degree of nodes and to sort nodes in
 * the fonl just id of the nodes are taken into account.
 */
public class FonlIdLCC {

    public static void main(String[] args) {
        String inputPath = "input.txt";
        if (args != null && args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args != null && args.length > 1) {
            partition = Integer.parseInt(args[1]);
        }

        int bSize = 10;
        if (args != null && args.length > 2) {
            bSize = Integer.parseInt(args[2]);
        }

        SparkConf conf = new SparkConf();
        if (args == null || args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "Fonl-LCC-Id", partition,inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.CandidateState.class, int[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);
        Broadcast<Integer> batchSize = sc.broadcast(bSize);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Integer, Integer> edges = GraphUtils.loadUndirectedEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createFonlIdBasedInt(edges, partition);
        long totalNodes = fonl.count();

        JavaPairRDD<Integer, GraphUtils.CandidateState> candidates = fonl
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, int[]>, Integer, GraphUtils.CandidateState>() {
                @Override
                public Iterator<Tuple2<Integer, GraphUtils.CandidateState>> call(Tuple2<Integer, int[]> tuple) throws
                    Exception {
                    List<Tuple2<Integer, GraphUtils.CandidateState>> output = new ArrayList<>();
                    int[] higherIds = new int[tuple._2.length - 1];
                    if (higherIds.length < 2)
                        return output.iterator();
                    System.arraycopy(tuple._2, 1, higherIds, 0, higherIds.length);

                    GraphUtils.CandidateState candidateState = new GraphUtils.CandidateState(tuple._1, higherIds);
                    Integer bSize = batchSize.getValue();
                    int size = tuple._2.length - 1;
                    for (int index = 1; index < size; index++) {
                        if (bSize != 0) {
                            if (index % bSize == 0) {
                                int blockSize = tuple._2.length - (index + 1);
                                int[] block = new int[blockSize];
                                System.arraycopy(tuple._2, index + 1, block, 0, blockSize);
                                candidateState = new GraphUtils.CandidateState(tuple._1, block);
                            }
                        }
                        output.add(new Tuple2<>(tuple._2[index], candidateState));
                    }
                    return output.iterator();
                }
            });

        JavaPairRDD<Integer, Integer> localTriangleCount = candidates.cogroup(fonl, partition)
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,
            Tuple2<Iterable<GraphUtils.CandidateState>, Iterable<int[]>>>, Integer, Integer>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Tuple2<Integer, Tuple2<Iterable<GraphUtils.CandidateState>,
                Iterable<int[]>>> t) throws Exception {
                Iterator<int[]> higherIdsIter = t._2._2.iterator();
                List<Tuple2<Integer, Integer>> output = new ArrayList<>();
                if (!higherIdsIter.hasNext())
                    return output.iterator();
                int[] higherIds = higherIdsIter.next();

                Iterator<GraphUtils.CandidateState> candidateIter = t._2._1.iterator();
                if (!candidateIter.hasNext())
                    return output.iterator();

                int sum = 0;
                do {
                    GraphUtils.CandidateState candidates = candidateIter.next();
                    int count = GraphUtils.sortedIntersectionCountInt(higherIds, candidates.higherIds, output, 0, 0);
                    if (count > 0) {
                        sum += count;
                        output.add(new Tuple2<>(candidates.firstNodeId, count));
                    }
                } while (candidateIter.hasNext());

                if (sum > 0) {
                    output.add(new Tuple2<>(t._1, sum));
                }
                return output.iterator();
            }
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
