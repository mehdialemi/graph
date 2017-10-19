package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static ir.ac.sbu.graph.utils.MultiCoreUtils.createBuckets;

/**
 * Find triangles.
 */
public class TriangleParallel {

    public static final int BUCKET_COUNT = 10000;
    public static final Tuple2<Integer, Integer> INVALID_TUPLE2 = new Tuple2<>(-1, -1);

    public static Tuple2<int[][], Set<Integer>[]> findEdgeTriangles(final Edge[] edges, final int threads) throws Exception {
        // find max vertex Id in parallel
        long t1 = System.currentTimeMillis();
        List<Tuple2<Integer, Integer>> buckets = createBuckets(threads, edges.length);
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
        buckets = createBuckets(threads, degArray.length);
        buckets.parallelStream().forEach(bucket ->
            IntStream.range(bucket._1, bucket._2).forEach(i -> degArray[i] = new AtomicInteger(0)));
        long t3 = System.currentTimeMillis();
        System.out.println("Construct degArray (AtomicInteger) in " + (t3 - t2) + " ms");

        // Construct degree array such that vertexId is the index of the array in parallel
        buckets = createBuckets(threads, edges.length);
        buckets.parallelStream().forEach(bucket ->
            Arrays.stream(edges, bucket._1, bucket._2).forEach(e -> {
                degArray[e.v1].incrementAndGet();
                degArray[e.v2].incrementAndGet();
            })
        );
        long t4 = System.currentTimeMillis();
        System.out.println("Fill degArray in " + (t4 - t3) + " ms");

        // Fill and quickSort vertices array.
        int[] vertices = sort(degArray, threads);
        int[] rvertices = new int[vertices.length];
        IntStream.range(0, threads).forEach(index -> {
            for (int i = index; i < vertices.length; i += threads)
                rvertices[vertices[i]] = i;
        });

        long t5 = System.currentTimeMillis();
        System.out.println("Sort degArray in " + (t5 - t4) + " ms");

        // Construct neighborhood
        List<Long>[] neighbors = new List[vertices.length];
        buckets = createBuckets(threads, vertices.length);
        buckets.parallelStream().forEach(bucket ->
            Arrays.stream(vertices, bucket._1, bucket._2).forEach(v ->
                neighbors[v] = new ArrayList()
            )
        );
        long t6 = System.currentTimeMillis();
        System.out.println("Construct neighbors in " + (t6 - t5) + " ms");

        // Fill neighbors array
        buckets = createBuckets(threads, edges.length);
        buckets.parallelStream().forEach(bucket -> {
            for (int i = bucket._1; i < bucket._2; i++) {
                Edge e = edges[i];
                if (rvertices[e.v2] >= rvertices[e.v1]) {
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
        long t8 = System.currentTimeMillis();

        AtomicInteger triangleOffset = new AtomicInteger(0);
        final int offset = (int) Arrays.stream(degArray).parallel().filter(d -> d.get() < 2).count();
        final List<Tuple2<Integer, Integer>> bucketsIndex = createBuckets(threads, offset, vertices.length);
        HashSet<Integer>[] validEdges = new HashSet[edges.length];
        long t9 = System.currentTimeMillis();
        System.out.println("Ready to triangle in " + (t9 - t8) + " ms with " + buckets.size() + " buckets");

        Map<Integer, HashSet<Integer>>[][] validEdgesMap = new HashMap[threads][];
        final Map<Integer, int[]>[] triangles = new HashMap[threads];
        for (int i = 0; i < threads; i++) {
            validEdgesMap[i] = new HashMap[threads];
            triangles[i] = new HashMap<>();
            for (int j = 0; j < threads; j++)
                validEdgesMap[i][j] = new HashMap<>();
        }

        final int maxBucketIndex = bucketsIndex.size() - 1;
        IntStream.range(0, threads).parallel().forEach(index -> {
            int bucketLen = bucketsIndex.get(0)._2 - bucketsIndex.get(0)._1;
            int triangleIndex = triangleOffset.getAndAdd(BUCKET_COUNT);
            Map<Integer, HashSet<Integer>>[] targetArray = validEdgesMap[index];

            for (int b = 0; b < bucketsIndex.size(); b++) {
                Tuple2<Integer, Integer> bucket = bucketsIndex.get(b);
                for (int i = index + bucket._1; i < bucket._2; i += threads) {
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
                                if (validEdges[uv] == null)
                                    validEdges[uv] = new HashSet<>();
                                validEdges[uv].add(triangleIndex);

                                if (validEdges[uw] == null)
                                    validEdges[uw] = new HashSet<>();
                                validEdges[uw].add(triangleIndex);

                                int vIndex = rvertices[v] - offset;
                                int target = (vIndex + offset - bucketsIndex.get(Math.min(maxBucketIndex, vIndex / bucketLen))._1) % threads;
                                if (target == index) {
                                    if (validEdges[vw] == null)
                                        validEdges[vw] = new HashSet<>();
                                    validEdges[vw].add(triangleIndex);
                                } else {
                                    HashSet<Integer> set = targetArray[target].get(vw);
                                    if (set == null) {
                                        set = new HashSet<>();
                                        targetArray[target].put(vw, set);
                                    }
                                    set.add(triangleIndex);
                                }

                                triangles[index].put(triangleIndex, new int[]{vw, uv, uw});
                                triangleIndex++;
                                if (triangleIndex % BUCKET_COUNT == 0)
                                    triangleIndex = triangleOffset.getAndAdd(BUCKET_COUNT);
                            }
                        }
                    }
                }
            }
        });

        // add remaining local found valid edges to the global valid edges
        IntStream.range(0, threads).parallel().forEach(index -> {
            for (int i = 0; i < threads; i++) {
                for(Map.Entry<Integer, HashSet<Integer>> entry : validEdgesMap[i][index].entrySet()) {
                    if (validEdges[entry.getKey()] == null)
                        validEdges[entry.getKey()] = new HashSet<>();
                    validEdges[entry.getKey()].addAll(entry.getValue());
                }
            }
        });

        // construct triangle array
        int tNum = triangleOffset.addAndGet(BUCKET_COUNT);
        int[][] triangleArray = new int[tNum][];
        IntStream.range(0, threads).parallel().forEach(index -> {
            Map<Integer, int[]> localT = triangles[index];
            for(Map.Entry<Integer, int[]> entry : localT.entrySet()) {
                triangleArray[entry.getKey()] = entry.getValue();
            }
        });

        long t10 = System.currentTimeMillis();
        System.out.println("TriangleSubgraph finished in " + (t10 - t9));

        return new Tuple2<>(triangleArray, validEdges);
    }

    public static int[] sort(AtomicInteger[] degArray, int threads) throws Exception {
        List<Tuple2<Integer, Integer>> buckets = createBuckets(threads, degArray.length);
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
        }).reduce((r1, r2) -> new Tuple2<>(Math.min(r1._1(), r2._1()), Math.max(r1._2(), r2._2())))
            .orElse(INVALID_TUPLE2);

        if (minMaxLen == INVALID_TUPLE2)
            throw new Exception("Invalid tuple");

        int min = minMaxLen._1();
        int max = minMaxLen._2();

        // init the frequencies
        AtomicInteger[] counts = new AtomicInteger[max - min + 1];
        for (int i = 0; i < counts.length; i++)
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
