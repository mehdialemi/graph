package graph.clusteringco;

import graph.GraphLoader;
import graph.GraphUtils;
import graph.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Counts total number of triangles in the given graph using degree based fonl. Copied from {@link FonlIdGCC}.
 */
public class FonlIdTC {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args != null && args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args != null && args.length > 1) {
            partition = Integer.parseInt(args[1]);
        }

        final int batchSize = 1000;

        SparkConf conf = new SparkConf();
        conf.registerKryoClasses(new Class[]{GraphUtils.CandidateState.class, int[].class});
        if (args == null || args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "Fonl-TC-Id", partition, inputPath);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createFonlIdBasedInt(edges, partition);

        JavaPairRDD<Integer, GraphUtils.CandidateState> candidates = generateCandidates(fonl, batchSize);

        long totalTriangles = countTriangles(candidates, fonl, partition);

        OutUtils.printOutputTC(totalTriangles);
        sc.close();
    }

    public static long countTriangles(JavaPairRDD<Integer, GraphUtils.CandidateState> candidates,
                                      JavaPairRDD<Integer, int[]> fonl, int partition) {
        return candidates.cogroup(fonl, partition).map(t -> {
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
            }).reduce((t1, t2) -> t1 + t2);
    }

    public static JavaPairRDD<Integer, GraphUtils.CandidateState> generateCandidates(JavaPairRDD<Integer, int[]> fonl,
                                                                                     final int splitSize) {
        return fonl.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, int[]>, Integer, GraphUtils.CandidateState>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Integer, GraphUtils.CandidateState>> call(Tuple2<Integer, int[]> tuple) throws
                        Exception {
                        List<Tuple2<Integer, GraphUtils.CandidateState>> output = new ArrayList<>();
                        GraphUtils.CandidateState candidateState = new GraphUtils.CandidateState(tuple._1, tuple._2);
                        int size = tuple._2.length - 1;
                        int split = splitSize;
                        if (size < 1000)
                             split = Integer.MAX_VALUE;
                        for (int index = 1; index < size; index++) {
                            if (split != 0) {
                                if (index % split == 0) {
                                    int blockSize = tuple._2.length - (index + 1);
                                    int[] block = new int[blockSize];
                                    System.arraycopy(tuple._2, index + 1, block, 0, block.length);
                                    candidateState = new GraphUtils.CandidateState(tuple._1, block);
                                }
                            }
                            output.add(new Tuple2<>(tuple._2[index], candidateState));
                        }
                        return output.iterator();
                    }
                });
    }
}
