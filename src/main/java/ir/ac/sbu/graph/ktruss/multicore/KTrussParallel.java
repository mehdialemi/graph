package ir.ac.sbu.graph.ktruss.multicore;

import ir.ac.sbu.graph.ktruss.sequential.Edge;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ir.ac.sbu.graph.ktruss.multicore.MultiCoreUtils.createBuckets;

/**
 * Truss Decomposition based on Edge TriangleParallel list.
 */
public class KTrussParallel {

    public static final double MAX_CHECK_RATIO = 0.3;
    private static final Tuple3<Integer, Integer, Integer> INVALID_TUPLE3 = new Tuple3<>(-1, -1, -1);
    private static final int BATCH_SIZE = 10000;
    public static final float BATCH_RATIO = 0.05f;

    public static void main(String[] args) throws Exception {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
//        String inputPath = "/home/mehdi/graph-data/Email-EuAll.txt";
        if (args.length > 0)
            inputPath = args[0];

        int k = 4; // k-truss
        if (args.length > 1)
            k = Integer.parseInt(args[1]);
        int minSup = k - 2;

        int threads = 4;
        if (args.length > 2)
            threads = Integer.parseInt(args[2]);
//        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "" + threads);
        ForkJoinPool forkJoinPool = new ForkJoinPool(threads);

        System.out.println("Start ktruss with k = " + k + ", threads = " + threads + ", input: " + inputPath);

        FileInputStream inputStream = new FileInputStream(inputPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        List<Edge> list = reader.lines().parallel().filter(line -> !line.startsWith("#"))
            .map(line -> line.split("\\s+"))
            .map(split -> new Edge(Integer.parseInt(split[0]), Integer.parseInt(split[1])))
            .filter(e -> e.v1 != e.v2)
            .collect(Collectors.toList());
        System.out.println("Graph loaded, edges: " + list.size());

//        ktrussArrayIndex(minSup, threads, forkJoinPool, list);
        ktrussMap(minSup, threads, forkJoinPool, list);
    }

    private static void ktrussMap(int minSup, int threads, ForkJoinPool forkJoinPool, List<Edge> edges) throws Exception {
        long tStart = System.currentTimeMillis();
        Map<Long, Set<Integer>> edgeMap = TriangleParallelForkJoin.findEdgeVerticesMap(edges, threads, forkJoinPool, BATCH_RATIO);
        long triangleTime = System.currentTimeMillis() - tStart;
        System.out.println("triangle time: " + triangleTime);
        int iteration = 0;

        while (true) {
            long t1 = System.currentTimeMillis();
            System.out.println("iteration: " + ++iteration);

            System.out.println("e size: " + edgeMap.size());

            // find invalid edges (those have support lesser than min sum)
            final List<Map.Entry<Long, Set<Integer>>> invalids = forkJoinPool.submit(() ->
                edgeMap.entrySet().parallelStream()
                    .filter(entry -> entry.getValue().size() < minSup)
                    .collect(Collectors.toList())
            ).get();

            if (invalids.size() == 0)
                break;

            long t2 = System.currentTimeMillis();
            System.out.println("Invalid size: " + invalids.size() + ", duration: " + (t2 - t1) + " ms");
            final Object lock = new Object();

            // find edges that should be updated. Here we create a map from edge to vertices which should be removed
            final int invalidSize = invalids.size();
            AtomicInteger batchSelector = new AtomicInteger(0);
            forkJoinPool.submit(() ->
                IntStream.range(0, threads).parallel().forEach(index -> {
                    int start = 0;
                    while (start < invalidSize) {
                        final Map<Long, List<Integer>> edgeInvalidVertices = new HashMap<>();
                        start = batchSelector.getAndAdd(BATCH_SIZE);
                        final List<Long> keys = new ArrayList<>();
                        for (int i = start; i < start + BATCH_SIZE && i < invalidSize; i++) {
                            Map.Entry<Long, Set<Integer>> invalidEV = invalids.get(i);
                            long edge = invalidEV.getKey();
                            keys.add(edge);
                            int u = (int) (edge >> 32);
                            int v = (int) edge;

                            for (int w : invalidEV.getValue()) {
                                long uw = u < w ? (long) u << 32 | w & 0xFFFFFFFFL : (long) w << 32 | u & 0xFFFFFFFFL; // construct edge uw
                                long vw = v < w ? (long) v << 32 | w & 0xFFFFFFFFL : (long) w << 32 | v & 0xFFFFFFFFL; // construct edge vw
                                List<Integer> vertices = edgeInvalidVertices.get(uw);
                                if (vertices == null) {
                                    vertices = new ArrayList<>();
                                    edgeInvalidVertices.put(uw, vertices);
                                }
                                vertices.add(v);  // add vertex v to include in invalid edges of uw

                                vertices = edgeInvalidVertices.get(vw);
                                if (vertices == null) {
                                    vertices = new ArrayList<>();
                                    edgeInvalidVertices.put(vw, vertices);
                                }
                                vertices.add(u);  // add vertex u to include in invalid edges of vw
                            }
                        }

                        synchronized (lock) {
                            for (long key : keys) {
                                edgeMap.remove(key);
                            }
                            edgeInvalidVertices.entrySet().stream().forEach(entry -> {
                                Set<Integer> vertices = edgeMap.get(entry.getKey());
                                if (vertices != null && vertices.size() > 0)
                                    vertices.removeAll(entry.getValue());
                            });
                        }
                    }
                })).get();

            long t3 = System.currentTimeMillis();
            System.out.println("Find invalid vertices: " + (t3 - t2) + " ms");
        }

        long duration = System.currentTimeMillis() - tStart;
        System.out.println("Number of edges is " + edgeMap.size() + ", in " + duration + " ms");
    }

    private static void ktrussArrayIndex(int minSup, int threads, ForkJoinPool forkJoinPool, List<Edge> list) throws Exception {
        long t1 = System.currentTimeMillis();
        Tuple2<int[][], Set<Integer>[]> result = TriangleParallelForkJoin.findEdgeTriangles(list, threads, forkJoinPool, BATCH_RATIO);
        long triangleTime = System.currentTimeMillis() - t1;
        Set<Integer>[] eTriangles = result._2;
        int[][] triangles = result._1;
        System.out.println("Triangle time: " + triangleTime);

        Tuple2<Integer, int[]> sorted = null;
        int iteration = 0;
        while (true) {
            System.out.println("iteration: " + ++iteration);
            long t1_sort = System.currentTimeMillis();
            sorted = sort(eTriangles, sorted, threads, minSup);
            long t2_sort = System.currentTimeMillis();
            System.out.println("Valid edges: " + sorted._2.length + ", sort time: " + (t2_sort - t1_sort) + " ms");
            List<Tuple2<Integer, Integer>> buckets = MultiCoreUtils.createBuckets(threads, sorted._1);
            if (sorted._1 == 0)
                break;
            final int[] etIndex = sorted._2;
            HashSet<Integer>[] tInvalids = new HashSet[threads];
            for (int i = 0; i < threads; i++)
                tInvalids[i] = new HashSet<>();

            Arrays.sort(etIndex, 0, sorted._1);

            IntStream.range(0, threads).parallel().forEach(index -> {
                Tuple2<Integer, Integer> bucket = buckets.get(index);
                for (int i = bucket._1; i < bucket._2; i++) {
                    for (int tIndex : eTriangles[etIndex[i]])
                        tInvalids[index].add(tIndex);
                    eTriangles[etIndex[i]] = null;
                }
            });
            long t2_findInvalids = System.currentTimeMillis();
            System.out.println("invalid time: " + (t2_findInvalids - t2_sort) + " ms");


            Arrays.stream(tInvalids).parallel().forEach(invalids -> {
                for (int tIndex : invalids) {
                    for (int e : triangles[tIndex]) {
                        if (eTriangles[e] != null)
                            eTriangles[e].remove(tIndex);
                    }
                }
            });
            long t2_remove = System.currentTimeMillis();
            System.out.println("remove invalid time: " + (t2_remove - t2_findInvalids) + " ms");
        }
        long duration = System.currentTimeMillis() - t1;
        System.out.println("Number of edges is " + sorted._2.length + ", in " + duration + " ms");
    }

    public static Tuple2<Integer, int[]> sort(final Set<Integer>[] eTriangles, final Tuple2<Integer, int[]> prevEdges, int threads, int minSup) throws Exception {
        final boolean usePrev = prevEdges == null || prevEdges._2 == null ||
            (prevEdges != null && prevEdges._1 < prevEdges._2.length * MAX_CHECK_RATIO) ? false : true;
        List<Tuple2<Integer, Integer>> buckets =
            usePrev ? createBuckets(threads, prevEdges._1, prevEdges._2.length) : createBuckets(threads, eTriangles.length);
        Tuple3<Integer, Integer, Integer> minMaxLen = buckets.parallelStream().map(bucket -> {
            int min = eTriangles.length;
            int max = 0;
            int len = 0;
            if (usePrev) {
                for (int i = bucket._1; i < bucket._2; i++) {
                    if (eTriangles[prevEdges._2[i]] == null || eTriangles[prevEdges._2[i]].isEmpty())
                        continue;
                    int num = eTriangles[prevEdges._2[i]].size();
                    if (num < min) {
                        min = num;
                    } else if (num > max) {
                        max = num;
                    }
                    len++;
                }
            } else {
                for (int i = bucket._1; i < bucket._2; i++) {
                    if (eTriangles[i] == null || eTriangles[i].isEmpty())
                        continue;
                    int num = eTriangles[i].size();
                    if (num < min) {
                        min = num;
                    } else if (num > max) {
                        max = num;
                    }
                    len++;
                }
            }
            return new Tuple3<>(min, max, len);
        }).reduce((r1, r2) -> new Tuple3<>(Math.min(r1._1(), r2._1()), Math.max(r1._2(), r2._2()), r1._3() + r2._3()))
            .orElse(INVALID_TUPLE3);

        if (minMaxLen == INVALID_TUPLE3)
            throw new Exception("Invalid tuple");

        int min = minMaxLen._1();
        int max = minMaxLen._2();
        int length = minMaxLen._3();

        // init the frequencies
        AtomicInteger[] counts = new AtomicInteger[max - min + 1];
        for (int i = 0; i < counts.length; i++)
            counts[i] = new AtomicInteger(0);

        buckets.parallelStream().forEach(bucket -> {
            if (usePrev) {
                for (int i = bucket._1; i < bucket._2; i++) {
                    Set<Integer> et = eTriangles[prevEdges._2[i]];
                    if (et == null || et.isEmpty())
                        continue;
                    counts[eTriangles[prevEdges._2[i]].size() - min].incrementAndGet();
                }
            } else {
                for (int i = bucket._1; i < bucket._2; i++) {
                    Set<Integer> et = eTriangles[i];
                    if (et == null || et.isEmpty())
                        continue;
                    counts[et.size() - min].incrementAndGet();
                }
            }
        });

        counts[0].decrementAndGet();
        for (int i = 1; i < counts.length; i++) {
            counts[i].addAndGet(counts[i - 1].get());
        }

        int minSupIndex = 0;
        int[] edges = new int[length];
        if (usePrev) {
            for (int i = prevEdges._1; i < prevEdges._2.length; i++) {
                Set<Integer> et = eTriangles[prevEdges._2[i]];
                if (et == null || et.isEmpty())
                    continue;
                int sup = et.size();
                int index = counts[sup - min].getAndDecrement();
                edges[index] = prevEdges._2[i];
                if (sup < minSup)
                    minSupIndex++;
            }
        } else {
            for (int i = eTriangles.length - 1; i >= 0; i--) {
                Set<Integer> et = eTriangles[i];
                if (et == null || et.isEmpty())
                    continue;
                int sup = et.size();
                int index = counts[sup - min].getAndDecrement();
                edges[index] = i;
                if (sup < minSup)
                    minSupIndex++;
            }
        }
        return new Tuple2<>(minSupIndex, edges);
    }
}
