package graph.clusteringco;

import graph.GraphUtils;
import graph.OutputUtils;
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
 * Calculate GCC with id based ordering. In this method we have no dealing with degree of nodes and to sort nodes in
 * the fonl just id of the nodes are taken into account.
 */
public class FonlIdGCC {

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
        GraphUtils.setAppName(conf, "Fonl-GCC-Id", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.CandidateState.class, int[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        Broadcast<Integer> batchSize = sc.broadcast(bSize);

        JavaPairRDD<Integer, Integer> edges = GraphUtils.loadUndirectedEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createFonlIdBasedInt(edges, partition);

        long nodes = fonl.count();

        JavaPairRDD<Integer, GraphUtils.CandidateState> candidate = fonl
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, int[]>, Integer, GraphUtils.CandidateState>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Iterable<Tuple2<Integer, GraphUtils.CandidateState>> call(Tuple2<Integer, int[]> tuple) throws Exception {
                    List<Tuple2<Integer, GraphUtils.CandidateState>> output = new ArrayList<>();
                    GraphUtils.CandidateState candidateState = new GraphUtils.CandidateState(tuple._1, tuple._2);
                    Integer bSize = batchSize.getValue();
                    int size = tuple._2.length - 1;
                    for (int index = 1; index < size; index++) {
                        if (bSize != 0) {
                            if (index % bSize == 0) {
                                int blockSize = tuple._2.length - (index + 1);
                                int[] block = new int[blockSize];
                                System.arraycopy(tuple._2, index + 1, block, 0, block.length);
                                candidateState = new GraphUtils.CandidateState(tuple._1, block);
                            }
                        }
                        output.add(new Tuple2<>(tuple._2[index], candidateState));
                    }
                    return output;
                }
            });

        long totalTriangles = candidate.cogroup(fonl, partition).map(t -> {
            Iterator<int[]> iterator = t._2._2.iterator();
            if (!iterator.hasNext())
                return 0L;
            int[] higherIds = iterator.next();

            long triangleCount = 0;
            Iterable<GraphUtils.CandidateState> allCandidates = t._2._1;
            for (GraphUtils.CandidateState can : allCandidates) {
                triangleCount += GraphUtils.sortedIntersectionCountInt(higherIds, can.higherIds, null, 1, 0);
            }
            return triangleCount;
        }).repartition(partition).reduce((t1, t2) -> t1 + t2);

        float globalCC = totalTriangles / (float) (nodes * (nodes - 1));

        OutputUtils.printOutputGCC(nodes, totalTriangles, globalCC);
        sc.close();
    }
}
