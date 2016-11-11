package ir.ac.sbu.graph.ktruss.multicore;

import ir.ac.sbu.graph.ktruss.sequential.Edge;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static ir.ac.sbu.graph.ktruss.multicore.MultiCoreUtils.createBuckets;

public class TriangleParallelExecutor {
    public static final int BUCKET_COUNT = 10000;
    public static final Tuple2<Integer, Integer> INVALID_TUPLE2 = new Tuple2<>(-1, -1);

    public static Tuple2<int[][], Set<Integer>[]> findEdgeTriangles(final Edge[] edges, final int threads) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
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

        // Fill and sort vertices array.
        int[] vertices = TriangleParallel.sort(degArray, threads);
        int[] rvertices = new int[vertices.length];
        IntStream.range(0, threads).forEach(index -> {
            for (int i = index; i < vertices.length; i += threads)
                rvertices[vertices[i]] = i;
        });

        long t5 = System.currentTimeMillis();
        System.out.println("Sort degArray in " + (t5 - t4) + " ms");

        // Construct neighborhood
        List<Long>[] neighbors = new List[vertices.length];
        List<Tuple2<Integer, Integer>> veBucket = createBuckets(threads, vertices.length);
        veBucket.parallelStream().forEach(bucket ->
            Arrays.stream(vertices, bucket._1, bucket._2).forEach(v ->
                neighbors[v] = new ArrayList()
            )
        );
        long t6 = System.currentTimeMillis();
        System.out.println("Construct neighbors in " + (t6 - t5) + " ms");

        buckets = MultiCoreUtils.createBuckets(threads, edges.length);
        // Fill neighbors array
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
        long t9 = System.currentTimeMillis();
        System.out.println("Ready to triangle in " + (t9 - t8) + " ms with " + buckets.size() + " buckets");

        List<Future<Tuple2<Map<Integer, int[]>, Map<Integer, List<Integer>>>>> futures = new ArrayList<>();
        ArrayBlockingQueue<Integer> completedThreadsQueue = new ArrayBlockingQueue<>(threads);
        for (int th = 0; th < threads; th++) {
            final int index = th;
            Future<Tuple2<Map<Integer, int[]>, Map<Integer, List<Integer>>>> future = executorService.submit(() -> {
                Map<Integer, List<Integer>> localValids = new HashMap<>();
                Map<Integer, int[]> localTriangles = new HashMap<>();
                int triangleIndex = triangleOffset.getAndAdd(BUCKET_COUNT);

                for (int b = 0; b < bucketsIndex.size(); b++) {
                    Tuple2<Integer, Integer> bucket = bucketsIndex.get(b);
                    for (int i = index + bucket._1; i < bucket._2; i += threads) {
                        int u = vertices[i];   // get current vertex id as u

                        // construct a map of nodes to their edges for u
                        final List<Long> uNeighbors = neighbors[u];
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
                                    List<Integer> list = localValids.get(uv);
                                    if (list == null) {
                                        list = new ArrayList<>();
                                        localValids.put(uv, list);
                                    }
                                    list.add(triangleIndex);

                                    list = localValids.get(uw);
                                    if (list == null) {
                                        list = new ArrayList<>();
                                        localValids.put(uw, list);
                                    }
                                    list.add(triangleIndex);

                                    list = localValids.get(vw);
                                    if (list == null) {
                                        list = new ArrayList<>();
                                        localValids.put(vw, list);
                                    }
                                    list.add(triangleIndex);

                                    localTriangles.put(triangleIndex, new int[]{vw, uv, uw});
                                    triangleIndex++;
                                    if (triangleIndex % BUCKET_COUNT == 0)
                                        triangleIndex = triangleOffset.getAndAdd(BUCKET_COUNT);
                                }
                            }
                        }
                    }
                }
                completedThreadsQueue.add(index);
                return new Tuple2<>(localTriangles, localValids);
            });
            futures.add(future);
        }

        final HashSet<Integer>[] validEdges = new HashSet[edges.length];
        final int[][] triangleArray = new int[triangleOffset.addAndGet(BUCKET_COUNT)][];
        List<Tuple2<Integer, Integer>> edgeBuckets = MultiCoreUtils.createBuckets(threads, edges.length);

        int counter = 0;
        while (counter < threads) {
            Integer completedThread = completedThreadsQueue.peek();
            while (!futures.get(completedThread).isDone()) Thread.sleep(1);
            final Tuple2<Map<Integer, int[]>, Map<Integer, List<Integer>>> result = futures.get(completedThread).get();
            Set<Map.Entry<Integer, int[]>> triangles = result._1.entrySet();
            Map<Integer, List<Integer>> localValids = result._2;
            edgeBuckets.parallelStream().forEach(eBucket -> {
                for (int edgeIndex = eBucket._1; edgeIndex < eBucket._2; edgeIndex++) {
                    List<Integer> et = localValids.get(edgeIndex);
                    if (et != null) {
                        if (validEdges[edgeIndex] == null)
                            validEdges[edgeIndex] = new HashSet<>();
                        validEdges[edgeIndex].addAll(et);
                    }
                }
            });
            triangles.parallelStream().forEach(entry -> triangleArray[entry.getKey()] = entry.getValue());
            counter ++;
        }
        long t10 = System.currentTimeMillis();
        System.out.println("Triangle finished in " + (t10 - t9));
        return new Tuple2<>(triangleArray, validEdges);
    }
}
