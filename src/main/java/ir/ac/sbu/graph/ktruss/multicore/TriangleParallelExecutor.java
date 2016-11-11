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
        final List<Tuple2<Integer, Integer>> edgeBuckets = createBuckets(threads, edges.length);
        int max = edgeBuckets.parallelStream()
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
        IntStream.range(0, degArray.length).parallel().forEach(i -> degArray[i] = new AtomicInteger(0));

        long t3 = System.currentTimeMillis();
        System.out.println("Construct degArray (AtomicInteger) in " + (t3 - t2) + " ms");

        // Construct degree array such that vertexId is the index of the array in parallel
        Arrays.stream(edges).parallel().forEach(e -> {
                degArray[e.v1].incrementAndGet();
                degArray[e.v2].incrementAndGet();
        });

        long t4 = System.currentTimeMillis();
        System.out.println("Fill degArray in " + (t4 - t3) + " ms");

        // Fill and sort vertices array.
        final int[] vertices = TriangleParallel.sort(degArray, threads);
        final int[] rvertices = new int[vertices.length];
        IntStream.range(0, threads).forEach(index -> {
            for (int i = index; i < vertices.length; i += threads)
                rvertices[vertices[i]] = i;
        });

        long t5 = System.currentTimeMillis();
        System.out.println("Sort degArray in " + (t5 - t4) + " ms");

        // Construct neighborhood
        final List<Long>[] neighbors = new List[vertices.length];
        Arrays.stream(vertices).parallel().forEach(v -> neighbors[v] = new ArrayList<>());

        long t6 = System.currentTimeMillis();
        System.out.println("Construct neighbors in " + (t6 - t5) + " ms");

        // Fill neighbors array
        edgeBuckets.parallelStream().forEach(bucket -> {
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

        //TODO sort neighbor list???

        long t7 = System.currentTimeMillis();
        System.out.println("Fill neighbors in " + (t7 - t6) + " ms");
        long t8 = System.currentTimeMillis();

        AtomicInteger triangleOffset = new AtomicInteger(0);
        long t9 = System.currentTimeMillis();
        System.out.println("Ready to triangle in " + (t9 - t8) + " ms with " + edgeBuckets.size() + " edgeBuckets");

        List<Future<Tuple2<Map<Integer, int[]>, Map<Integer, List<Integer>>>>> futures = new ArrayList<>();
        ArrayBlockingQueue<Integer> completedThreadsQueue = new ArrayBlockingQueue<>(threads);
        final AtomicInteger maxTriangles = new AtomicInteger(0);
        final Object lock = new Object();
        final int offset = (int) Arrays.stream(degArray).parallel().filter(d -> d.get() < 2).count();
        final int length = vertices.length;
        final AtomicInteger blockOffset = new AtomicInteger(offset);

        for (int th = 0; th < threads; th++) {
            final int index = th;
            Future<Tuple2<Map<Integer, int[]>, Map<Integer, List<Integer>>>> future = executorService.submit(() -> {
                Map<Integer, List<Integer>> localEdgeTriangles = new HashMap<>();
                Map<Integer, int[]> localTriangles = new HashMap<>();
                int triangleNum = triangleOffset.getAndAdd(BUCKET_COUNT);
                int i = 0;
                while (i < length) {
                    int start = blockOffset.getAndAdd(BUCKET_COUNT);
                    for (i = start; i < start + BUCKET_COUNT && i < length; i++) {
                        int u = vertices[i];   // get current vertex id as u

                        // TODO use bitset
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
                                    List<Integer> list = localEdgeTriangles.get(uv);
                                    if (list == null) {
                                        list = new ArrayList<>();
                                        localEdgeTriangles.put(uv, list);
                                    }
                                    list.add(triangleNum);

                                    list = localEdgeTriangles.get(uw);
                                    if (list == null) {
                                        list = new ArrayList<>();
                                        localEdgeTriangles.put(uw, list);
                                    }
                                    list.add(triangleNum);

                                    list = localEdgeTriangles.get(vw);
                                    if (list == null) {
                                        list = new ArrayList<>();
                                        localEdgeTriangles.put(vw, list);
                                    }
                                    list.add(triangleNum);


                                    localTriangles.put(triangleNum, new int[]{vw, uv, uw});
                                    triangleNum++;
                                    if (triangleNum % BUCKET_COUNT == 0)
                                        triangleNum = triangleOffset.getAndAdd(BUCKET_COUNT);
                                }
                            }
                        }
                    }
                }

                synchronized (lock) {
                    if (triangleNum > maxTriangles.get())
                        maxTriangles.set(triangleNum);
                    System.out.println("Thread " + index + ", triangles: " + localTriangles.size());
                }
                completedThreadsQueue.add(index);
                return new Tuple2<>(localTriangles, localEdgeTriangles);
            });
            futures.add(future);
        }

        final HashSet<Integer>[] edgeTriangles = new HashSet[edges.length];

        int counter = 0;
        List<Map<Integer, int[]>> localTrianglesList = new ArrayList<>(threads);
        while (counter < threads) {
            Integer completedThread = completedThreadsQueue.take();
            System.out.println("Completed thread: " + completedThread);
            while (!futures.get(completedThread).isDone()) Thread.sleep(1);

            final Tuple2<Map<Integer, int[]>, Map<Integer, List<Integer>>> result = futures.get(completedThread).get();
            localTrianglesList.add(result._1);
            Set<Map.Entry<Integer, List<Integer>>> localEdgeTriangles = result._2.entrySet();

            localEdgeTriangles.parallelStream().forEach(entry -> {
                if (edgeTriangles[entry.getKey()] == null)
                    edgeTriangles[entry.getKey()] = new HashSet<>();
                edgeTriangles[entry.getKey()].addAll(entry.getValue());
            });
//            edgeBuckets.parallelStream().forEach(eBucket -> {
//                for (Map.Entry<Integer, List<Integer>> entry : localEdgeTriangles) {
//                    Integer edgeIndex = entry.getKey();
//                    if (edgeIndex >= eBucket._1 && edgeIndex < eBucket._2) {
//                        if (edgeTriangles[edgeIndex] == null)
//                            edgeTriangles[edgeIndex] = new HashSet<>();
//                        edgeTriangles[edgeIndex].addAll(entry.getValue());
//                    }
//                }
//            });
            counter++;
        }

        executorService.shutdown();

        int[][] triangles = new int[maxTriangles.get()][];
        localTrianglesList.parallelStream().forEach(localTriangles ->
            localTriangles.entrySet().forEach(triangle ->
                triangles[triangle.getKey()] = triangle.getValue()
            ));

        long t10 = System.currentTimeMillis();
        System.out.println("Triangle finished in " + (t10 - t9));
        return new Tuple2<>(triangles, edgeTriangles);
    }
}
