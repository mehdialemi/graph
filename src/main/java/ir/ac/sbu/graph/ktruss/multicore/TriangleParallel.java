package ir.ac.sbu.graph.ktruss.multicore;

import ir.ac.sbu.graph.ktruss.sequential.Edge;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static ir.ac.sbu.graph.ktruss.multicore.MultiCoreUtils.createBuckets;

/**
 * Find triangles.
 */
public class TriangleParallel {

    public static final int BUCKET_COUNT = 10000;
    public static final Tuple2<Integer, Integer> INVALID_TUPLE2 = new Tuple2<>(-1, -1);

    public static Tuple2<Map<Integer, int[]>, Set<Integer>[]> findEdgeTriangles(final Edge[] edges, int threads) throws Exception {
        // find max vertex Id in parallel
        long t1 = System.currentTimeMillis();
        List<Tuple2<Integer, Integer>> buckets = createBuckets(threads, edges);
        int max = buckets.parallelStream()
            .map(bucket -> Arrays.stream(edges, bucket._1, bucket._2)
                    .map(e -> Math.max(e.v1, e.v2))
                    .reduce((v1, v2) -> Math.max(v1, v2)).orElse(-1))
            .reduce((m1, m2) -> Math.max(m1, m2)).orElse(-1).intValue();
        long t2 = System.currentTimeMillis();
        System.out.println("Finding max in " + (t2 - t1) + " ms");

        if (max == -1)
            throw new Exception("Problem in max finding");

        // construct deg array
        AtomicInteger[] degArray = new AtomicInteger[max + 1];
        buckets = createBuckets(threads, degArray);
        buckets.parallelStream().forEach(bucket ->
            IntStream.range(bucket._1, bucket._2).forEach(i -> degArray[i] = new AtomicInteger(0)));
        long t3 = System.currentTimeMillis();
        System.out.println("Construct degArray (AtomicInteger) in " + (t3 - t2) + " ms");

        // Construct degree array such that vertexId is the index of the array in parallel
        buckets = createBuckets(threads, edges);
        buckets.parallelStream().forEach(bucket ->
                Arrays.stream(edges, bucket._1, bucket._2).forEach(e -> {
                    degArray[e.v1].incrementAndGet();
                    degArray[e.v2].incrementAndGet();
                })
        );
        long t4 = System.currentTimeMillis();
        System.out.println("Fill degArray in " + (t4 - t3) + " ms");

        // Fill and sort vertices array.
        int[] vertices = sort(degArray, threads);
        long t5 = System.currentTimeMillis();
        System.out.println("Sort degArray in " + (t5 - t4) + " ms");

        // Construct neighborhood
        List<Long>[] neighbors = new List[vertices.length];
        buckets = createBuckets(threads, vertices);
        buckets.parallelStream().forEach(bucket ->
            Arrays.stream(vertices, bucket._1, bucket._2).forEach(v ->
                neighbors[v] = new ArrayList()
            )
        );
        long t6 = System.currentTimeMillis();
        System.out.println("Construct neighbors in " + (t6 - t5) + " ms");

        // Fill neighbors array
        buckets = createBuckets(threads, edges);
        buckets.parallelStream().forEach(bucket -> {
            for (int i = bucket._1 ; i < bucket._2; i ++) {
                Edge e = edges[i];
                float diff = (degArray[e.v2].get() - degArray[e.v1].get()) + (e.v2 - e.v1) / (float) (e.v2 + e.v1);
                if (diff >= 0) {
                    synchronized (degArray[e.v1]) {
                        neighbors[e.v1].add((long) e.v2 << 32 | i & 0xFFFFFFFFL);
                    }
                } else {
                    synchronized (degArray[e.v2]) {
                        neighbors[e.v2].add((long) e.v1 << 32 | i & 0xFFFFFFFFL);
                    }
                }
            }
        });
        long t7 = System.currentTimeMillis();
        System.out.println("Fill neighbors in " + (t7 - t6) + " ms");

        // construct eTriangles
//        Set<Integer>[] eTriangles = new Set[edges.length];
//        buckets = createBuckets(threads, edges);
//        buckets.parallelStream().forEach(bucket ->
//            IntStream.range(bucket._1, bucket._2).forEach(i ->
//                eTriangles[i] = Collections.synchronizedSet(new HashSet<>())));
        long t8 = System.currentTimeMillis();

        AtomicInteger triangleOffset = new AtomicInteger(0);
        long count = Arrays.stream(degArray).parallel().filter(d -> d.get() > 1).count();
        int start = (int) (vertices.length - count);
        buckets = createBuckets(threads, vertices, start);
        long t9 = System.currentTimeMillis();
        System.out.println("Ready to triangle in " + (t9 - t8) + " ms with " + buckets.size() + " buckets");
        final Map<Integer, int[]> triangles = new HashMap<>();
//        Tuple2<HashMap<Integer, int[]>, Map<Integer, Set<Integer>>> teMap =
        Stream<Map<Integer, Set<Integer>>> map = buckets.parallelStream().map(bucket -> {
//            HashMap<Integer, int[]> triangleIndexMap = new HashMap<>();
            int triangleIndex = triangleOffset.getAndAdd(BUCKET_COUNT);
            Map<Integer, Set<Integer>> localET = new HashMap<>();
            int progress = 0;
            int step = (bucket._2 - bucket._1) / 10;
            progress = step;
            for (int i = bucket._2 - 1; i >= bucket._1; i--) {
                if (bucket._2 - i > progress) {
                    System.out.println("bucket (" + bucket._1 + ") progress = " + progress / step * 10 + "%");
                    progress += step;
                }
                int u = vertices[i];   // get current vertex id as u

                // construct a map of nodes to their edges for u
                List<Long> uNeighbors = neighbors[u];
                Map<Integer, Integer> unEdges = new HashMap<>(uNeighbors.size());
                for (int ni = 0; ni < uNeighbors.size(); ni++) {
                    long ve = uNeighbors.get(ni);
                    int v = (int) (ve >> 32);
                    int e = (int) ve;
                    unEdges.put(v, e);
                }

                // iterate over neighbors of u using edge info
                for (int j = 0; j < uNeighbors.size(); j++) {
                    long ve = uNeighbors.get(j);
                    int v = (int) (ve >> 32);
                    int uv = (int) ve;

                    // iterate over neighbors of v
                    List<Long> vNeighbors = neighbors[v];
                    for (int k = 0; k < vNeighbors.size(); k++) {
                        long we = vNeighbors.get(k);
                        int w = (int) (we >> 32);
                        int vw = (int) we;

                        Integer uw = unEdges.get(w);
                        if (uw != null) {
                            Set<Integer> set = localET.get(vw);
                            if (set == null) {
                                set = new HashSet<>();
                                localET.put(vw, set);
                            }
                            set.add(triangleIndex);

                            set = localET.get(uv);
                            if (set == null) {
                                set = new HashSet<>();
                                localET.put(uv, set);
                            }
                            set.add(triangleIndex);

                            set = localET.get(uw);
                            if (set == null) {
                                set = new HashSet<>();
                                localET.put(uw, set);
                            }
                            set.add(triangleIndex);

//                            triangleIndexMap.put(triangleIndex, new int[]{vw, uv, uw});
                            triangles.put(triangleIndex, new int[]{vw, uv, uw});
                            triangleIndex++;
                            if (triangleIndex % BUCKET_COUNT == 0)
                                triangleIndex = triangleOffset.getAndAdd(BUCKET_COUNT);
                        }
                    }
                }
            }
            System.out.println("Bucket (" + bucket._1 + ") finished");
//            return new Tuple2<>(triangleIndexMap, localET);
            return localET;
        });
        long tMap = System.currentTimeMillis();
        System.out.println("Map is finished in " + (tMap - t9) + " ms");

        Map<Integer, Set<Integer>> et = map.reduce((m1, m2) -> {
            m2.entrySet().parallelStream().forEach(entry -> {
                Set<Integer> set = m1.get(entry.getKey());
                if (set == null)
                    m1.put(entry.getKey(), entry.getValue());
                else
                    set.addAll(entry.getValue());
            });
            return m1;
        }).orElse(new HashMap<>());
        long t10 = System.currentTimeMillis();
        System.out.println("Triangle finished in " + (t10 - t9) + " ms, reducer time: " + (t10 - tMap) + " ms");
        Set<Integer>[] eTriangles = new Set[edges.length];
        et.entrySet().parallelStream().forEach(entry -> eTriangles[entry.getKey()] = entry.getValue());
        long t11 = System.currentTimeMillis();
        System.out.println("Construct and fill eTriangles in " + (t11 - t10) + " ms");
//                .reduce((m1, m2) -> {
//            m1._1.putAll(m2._1);
//            m2._2.entrySet().parallelStream().forEach(entry -> {
//                Set<Integer> set = m1._2.get(entry.getKey());
//                if (set == null)
//                    m1._2.put(entry.getKey(), entry.getValue());
//                else
//                    set.addAll(entry.getValue());
//            });
//            return m1;
//        }).orElse(new Tuple2<>(new HashMap<>(), new HashMap<>()));

        return new Tuple2<>(triangles, eTriangles);
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
