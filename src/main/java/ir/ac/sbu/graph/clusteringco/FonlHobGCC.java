package ir.ac.sbu.graph.clusteringco;

import ir.ac.sbu.graph.GraphLoader;
import ir.ac.sbu.graph.GraphUtils;
import ir.ac.sbu.graph.OutUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * Consider hop nodes when finding triangles.
 */
public class FonlHobGCC {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");

        GraphUtils.setAppName(conf, "Fonl-GCC-Deg", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class,
            Map.class, HashMap.class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);

        JavaPairRDD<Long, long[]> fonl = FonlUtils.createWith2ReduceNoSort(edges, partition);
        Long sumDegree = fonl.map(t -> t._2[0]).reduce((a, b) -> a + b);
        long totalNodes = fonl.count();
        double avgDeg = sumDegree / (double) fonl.count();
        final int minHobDeg = (int) (avgDeg * totalNodes * 0.01);

        System.out.println("Configure minHobDeg is " + minHobDeg);

        Map<Long, long[]> hobs = fonl.filter(t -> t._2[0] > minHobDeg).collectAsMap();
        Map<Long, long[]> hobsMap = new HashMap<>(hobs);
        System.out.println("HobsMap is created with " + hobsMap.size() + " items.");

        Broadcast<Map<Long, long[]>> hobBD = sc.broadcast(hobsMap);
        Accumulator<Long> triangleCount = sc.accumulator((long) 0, "triangles", new AccumulatorParam<Long>() {

            @Override
            public Long addAccumulator(Long t1, Long t2) {
                return t1 + t2;
            }

            @Override
            public Long addInPlace(Long r1, Long r2) {
                return r1 + r2;
            }

            @Override
            public Long zero(Long initialValue) {
                return initialValue;
            }
        });

        JavaPairRDD<Long, long[]> candidates = fonl
            .filter(t -> t._2[0] <= minHobDeg && t._2.length > 2).flatMapToPair(t -> {
                // find the first hobs.
                int size = t._2.length - 1;
                Map<Long, long[]> map = hobBD.getValue();
                int maxIndex = 1;
                if (map != null) {
                    for (maxIndex = t._2.length - 1; maxIndex >= 1; maxIndex--) {
                        long node = t._2[maxIndex];
                        if (!map.containsKey(node)) {
                            break;
                        }
                    }

                    if (maxIndex != t._2.length - 1) {
                        // find triangle count with hobs
                        int triangles = findTriangles(t._2, map, maxIndex + 1);
                        triangleCount.add((long) triangles);
                    }
                }

                List<Tuple2<Long, long[]>> output = new ArrayList<>(size - 1);

                for (int index = 1; index <= maxIndex; index++) {
                    int len = size - index;
                    long[] forward = new long[len];
                    System.arraycopy(t._2, index + 1, forward, 0, len);
                    Arrays.sort(forward);
                    output.add(new Tuple2<>(t._2[index], forward));
                }

                if (output.size() == 0)
                    return Collections.emptyIterator();

                return output.iterator();
            });

        long triangle1 = candidates.cogroup(fonl, partition).map(t -> {
            Iterator<long[]> iterator = t._2._2.iterator();
            if (!iterator.hasNext()) {
                return 0L;
            }
            long[] hDegs = iterator.next();

            iterator = t._2._1.iterator();
            if (!iterator.hasNext()) {
                return 0L;
            }

            Arrays.sort(hDegs, 1, hDegs.length);
            long sum = 0;

            do {
                long[] forward = iterator.next();
                int count = GraphUtils.sortedIntersectionCount(hDegs, forward, null, 1, 0);
                sum += count;
            } while (iterator.hasNext());

            return sum;
        }).reduce((a, b) -> a + b);

        Long triangleWithHobs = triangleCount.localValue();

        int sumHobe = 0;
        if (hobs != null) {
            // find triangles amongs hobs
            Set<Long> hobeNodes = hobs.keySet();
            for (Long node : hobeNodes) {
                long[] nodeNeighbors = hobs.get(node);
                sumHobe += findTriangles(nodeNeighbors, hobs, 0);
            }
        }

        long totalTriangles = triangle1 + triangleWithHobs + sumHobe;
        float globalCC = totalTriangles / (float) (totalNodes * (totalNodes - 1));
        OutUtils.printOutputGCC(totalNodes, totalTriangles, globalCC);

    }

    private static int findTriangles(long[] vertices, Map<Long, long[]> map, int maxIndex) {
        int sum = 0;
        for (int index = maxIndex; index < vertices.length - 1; index++) {
            for (int next = index + 1; next < vertices.length; next++) {
                long v = vertices[index];
                long w = vertices[next];
                long[] vNeighbors = map.get(v);
                if (vNeighbors == null)
                    continue;

                for (int i = 1; i < vNeighbors.length; i++) {
                    if (vNeighbors[i] == w) {
                        sum++;
                        break;
                    }
                }
            }
        }
        return sum;
    }
}
