package ir.ac.sbu.graph.ktruss.multicore;

import ir.ac.sbu.graph.ktruss.sequential.Edge;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static ir.ac.sbu.graph.ktruss.multicore.MultiCoreUtils.createBuckets;

/**
 * Find triangles.
 */
public class TriangleParallel {

    public static final int BUCKET_COUNT = 10000;
    public static final Tuple2<Integer, Integer> INVALID_TUPLE2 = new Tuple2<>(-1, -1);

    public static Tuple2<Map<Integer, int[]>, Set<Integer>[]> findEdgeTriangles(final Edge[] edges, int threads) throws Exception {
        // find max vertex Id in parallel
        List<Tuple2<Integer, Integer>> buckets = createBuckets(threads, edges);
        int max = buckets.parallelStream()
            .map(bucket -> Arrays.stream(edges, bucket._1, bucket._2)
                    .map(e -> Math.max(e.v1, e.v2))
                    .reduce((v1, v2) -> Math.max(v1, v2)).orElse(-1))
            .reduce((m1, m2) -> Math.max(m1, m2)).orElse(-1).intValue();

        if (max == -1)
            throw new Exception("Problem in max finding");

        // construct deg array
        AtomicInteger[] degArray = new AtomicInteger[max + 1];
        buckets = createBuckets(threads, degArray);
        buckets.parallelStream().forEach(bucket ->
            IntStream.range(bucket._1, bucket._2).forEach(i -> degArray[i] = new AtomicInteger(0)));

        // Construct degree array such that vertexId is the index of the array in parallel
        buckets = createBuckets(threads, edges);
        buckets.parallelStream().forEach(bucket ->
                Arrays.stream(edges, bucket._1, bucket._2).forEach(e -> {
                    degArray[e.v1].incrementAndGet();
                    degArray[e.v2].incrementAndGet();
                })
        );

        // Fill and sort vertices array.
        int[] vertices = sort(degArray, threads);

        // Construct neighborhood
        List<Long>[] neighbors = new List[vertices.length];
        buckets = createBuckets(threads, vertices);
        buckets.parallelStream().forEach(bucket ->
            Arrays.stream(vertices, bucket._1, bucket._2).forEach(v ->
                neighbors[v] = Collections.synchronizedList(new ArrayList())
            )
        );

        // Fill neighbors array
        buckets = createBuckets(threads, edges);
        buckets.parallelStream().forEach(bucket -> {
            for (int i = bucket._1 ; i < bucket._2; i ++) {
                Edge e = edges[i];
                float diff = (degArray[e.v2].get() - degArray[e.v1].get()) + (e.v2 - e.v1) / (float) (e.v2 + e.v1);
                if (diff >= 0) {
                    neighbors[e.v1].add(MultiCoreUtils.toLong(e.v2, i));
                } else {
                    neighbors[e.v2].add(MultiCoreUtils.toLong(e.v1, i));
                }
            }
        });
        System.out.println("Neighbor list is created");

        // construct eTriangles
        Set<Integer>[] eTriangles = new Set[edges.length];
        buckets = createBuckets(threads, edges);
        buckets.parallelStream().forEach(bucket ->
            IntStream.range(bucket._1, bucket._2).forEach(i ->
                eTriangles[i] = Collections.synchronizedSet(new HashSet<>())));

        AtomicInteger triangleOffset = new AtomicInteger(0);
        long count = Arrays.stream(degArray).parallel().filter(d -> d.get() > 1).count();
        int start = (int) (vertices.length - count);
        buckets = createBuckets(threads, vertices, start);

        HashMap<Integer, int[]> trianglesMap = buckets.parallelStream().map(bucket -> {
            HashMap<Integer, int[]> triangleIndexMap = new HashMap<>();
            int localTriangleIndex = triangleOffset.getAndAdd(BUCKET_COUNT);
            for (int i = bucket._2 - 1; i >= bucket._1; i--) {
                int u = vertices[i];   // get current vertex id as u

                // construct a map of nodes to their edges for u
                List<Long> uNeighbors = neighbors[u];
                Map<Integer, Integer> unEdges = new HashMap<>(uNeighbors.size() / 2);
                for (int ni = 0; ni < uNeighbors.size(); ni ++) {
                    long ve = uNeighbors.get(ni);
                    int v = (int)(ve >> 32);
                    int e = (int)ve;
                    unEdges.put(v, e);
                }

                // iterate over neighbors of u using edge info
                for (int j = 0; j < uNeighbors.size(); j ++) {
                    long ve = uNeighbors.get(j);
                    int v = (int)(ve >> 32);
                    int uv = (int)ve;

                    // iterate over neighbors of v
                    List<Long> vNeighbors = neighbors[v];
                    for (int k = 0; k < vNeighbors.size(); k ++) {
                        long we = vNeighbors.get(k);
                        int w = (int)(we >> 32);
                        int vw = (int)we;

                        Integer uw = unEdges.get(w);
                        if (uw != null) {
                            eTriangles[vw].add(localTriangleIndex);
                            eTriangles[uv].add(localTriangleIndex);
                            eTriangles[uw].add(localTriangleIndex);

                            triangleIndexMap.put(localTriangleIndex, new int[]{vw, uv, uw});
                            localTriangleIndex++;
                            if (localTriangleIndex % BUCKET_COUNT == 0)
                                localTriangleIndex = triangleOffset.getAndAdd(BUCKET_COUNT);
                        }
                    }
                }
            }
            return triangleIndexMap;
        }).reduce((map1, map2) -> {
            map1.putAll(map2);
            return map1;
        }).orElse(new HashMap<>());

        return new Tuple2<>(trianglesMap, eTriangles);
    }

    public static int[] sort(AtomicInteger[] degArray, int threads) throws Exception {
        List<Tuple2<Integer, Integer>> buckets = createBuckets(threads, degArray);
        Tuple2<Integer, Integer> minMaxLen = buckets.parallelStream().map(bucket -> {
                int min = degArray[bucket._1].get();
                int max = degArray[bucket._1].get();
                for (int i = bucket._1 + 1; i < bucket._2; i++) {
                    int deg = degArray[i].get();
                    if (deg == 0)
                        continue;
                    if (deg < min) {
                        min = deg;
                    } else if (deg > max)
                        max = deg;
                }
                return new Tuple2<>(min, max);
            }).reduce((r1 , r2) -> new Tuple2<>(Math.min(r1._1(), r2._1()), Math.max(r1._2(), r2._2())))
            .orElse(INVALID_TUPLE2);

        if (minMaxLen == INVALID_TUPLE2)
            throw new Exception("Invalid tuple");

        int min = minMaxLen._1();
        int max = minMaxLen._2();

        // init the frequencies
        AtomicInteger[] counts = new AtomicInteger[max - min + 1];
        for(int i = 0 ; i < counts.length ; i ++)
            counts[i] = new AtomicInteger(0);
        buckets.parallelStream().forEach(bucket ->
            IntStream.range(bucket._1, bucket._2).forEach(i ->
                counts[degArray[i].get() - min].incrementAndGet()));

        counts[0].decrementAndGet();
        for (int i = 1; i < counts.length; i++) {
            counts[i].addAndGet(counts[i - 1].get());
        }

        int[] vertices = new int[degArray.length];
        for (int i = vertices.length - 1; i >= 0; i--) {
            int index = counts[degArray[i].get() - min].getAndDecrement();
            vertices[index] = i;
        }
        return vertices;
    }
}
