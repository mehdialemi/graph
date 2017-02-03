package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static ir.ac.sbu.graph.utils.MultiCoreUtils.createBuckets;

/**
 *
 */
public class ParallelKTruss2 extends ParallelKTrussBase {

    private final ForkJoinPool forkJoinPool;

    public ParallelKTruss2(Edge[] edges, int minSup, int threads) {
        super(edges, minSup, threads);
        forkJoinPool = new ForkJoinPool(threads);
    }

    @Override
    public void start() throws Exception {
        long tStart = System.currentTimeMillis();
        Map<Long, Set<Integer>> edgeMap = findEdgeVerticesMap(edges, threads, forkJoinPool, BATCH_RATIO);
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

    public static Map<Long, Set<Integer>> findEdgeVerticesMap(final Edge[] edges, final int threads, ForkJoinPool forkJoinPool,
                                                              float batchRatio)
        throws Exception {
        long t1 = System.currentTimeMillis();

        // for each thread, create a map from vertex id to its degree
        final List<Tuple2<Integer, Integer>> edgeBuckets = createBuckets(threads, edges.length);
        Stream<Map<Integer, Integer>> vdResult = forkJoinPool.submit(() -> {
            return edgeBuckets.parallelStream().map(bucket -> {
                Map<Integer, Integer> localVertexDegree = new HashMap<>();
                for (int i = bucket._1; i < bucket._2; i++) {
                    int v1 = edges[i].v1;
                    Integer count = localVertexDegree.get(v1);
                    if (count == null)
                        count = 0;
                    localVertexDegree.put(v1, count + 1);

                    int v2 = edges[i].v2;
                    count = localVertexDegree.get(v2);
                    if (count == null)
                        count = 0;
                    localVertexDegree.put(v2, count + 1);
                }

                return localVertexDegree;
            });
        }).get();

        // find the number of vertices having degree more than zero
        List<Map<Integer, Integer>> vdLocalMaps = vdResult.collect(Collectors.toList());
        final int vertexNum = vdLocalMaps.stream().map(v -> v.size()).reduce((a, b) -> b + a).get() + 1;
        final int batchSize = ((int) (vertexNum * batchRatio) / threads) * threads; // make batchSize as a multiplier of threads

        // create an arrayList to hold each vertex's degree. index of this arrayList corresponds to vertex Id
        int[] vertexDegrees = new int[vertexNum];

        // Iterate over local vertex degree map (vdLocalMaps) and update vertexDegrees accordingly.
        for (Map<Integer, Integer> vdLocalMap : vdLocalMaps) {
            forkJoinPool.submit(() -> { // per local vertex degree vdLocalMap perform the task in parallel to update vertexDegrees
                vdLocalMap.entrySet().parallelStream().forEach(vd -> vertexDegrees[vd.getKey()] += vd.getValue());
            }).join();
        }

        long t2 = System.currentTimeMillis();
        System.out.println("Calculating vertex degree in " + (t2 - t1) + " ms");

        // Construct neighborhood
        Stream<Map<Integer, List<Integer>>> localNeighborsMaps = forkJoinPool.submit(() -> edgeBuckets.parallelStream().map(bucket -> {
            Map<Integer, List<Integer>> localMap = new HashMap<>();
            for (int i = bucket._1; i < bucket._2; i++) {
                Edge e = edges[i];
                int dv1 = vertexDegrees[e.v1];
                int dv2 = vertexDegrees[e.v2];
                if (dv1 < 2 || dv2 < 2)
                    continue;

                // When we have equal degree, vertex Ids are taken into account
                if (dv1 == dv2) {
                    dv1 = e.v1;
                    dv2 = e.v2;
                }

                if (dv1 < dv2) {
                    List<Integer> list = localMap.get(e.v1);
                    if (list == null) {
                        list = new ArrayList<>();
                        localMap.put(e.v1, list);
                    }
                    list.add(e.v2);
                } else {
                    List<Integer> list = localMap.get(e.v2);
                    if (list == null) {
                        list = new ArrayList<>();
                        localMap.put(e.v2, list);
                    }
                    list.add(e.v1);
                }
            }
            return localMap;
        })).join();

        // Aggregate local neighbors into fnl (filtered neighbor list)
        Set<Integer>[] fnl = new Set[vertexDegrees.length];
        localNeighborsMaps.forEach(localNeighborsMap -> forkJoinPool.submit(() -> {
            localNeighborsMap.entrySet().parallelStream() // Per localNeighborMap update fnl in parallel
                .forEach(entry -> {
                    int v = entry.getKey();
                    if (fnl[v] == null)
                        fnl[v] = new HashSet<>();
                    fnl[v].addAll(entry.getValue());
                });
        }).join());

        long t3 = System.currentTimeMillis();
        System.out.println("Construct neighbors in " + (t3 - t2) + " ms");

        final Object lock = new Object();
        final Map<Long, Set<Integer>> eMap = new HashMap<>();

        // Find triangles here
        final AtomicInteger batchSelector = new AtomicInteger(0);
        final AtomicInteger syncSizeSelector = new AtomicInteger(0);
        int syncIncrement = batchSize / threads;

        forkJoinPool.submit(() ->
            IntStream.range(0, threads).parallel().forEach(index -> {
                int start = 0;
                syncSizeSelector.compareAndSet(batchSize, 0);
                int syncSize = syncSizeSelector.getAndAdd(syncIncrement) + batchSize;
                Map<Long, Set<Integer>> eVerticesMap = new HashMap<>(batchSize * 2);

                // Each thread create an output as Map which represents edge as a key and a list of pair ver as value.
                while (start < vertexNum) {
                    start = batchSelector.getAndAdd(batchSize);

                    // for each vertex u (index of the arrayList is equal to its Id)
                    for (int u = start; u < start + batchSize && u < vertexNum; u++) {
                        final Set<Integer> uNeighbors = fnl[u];   // get neighbors of u as uNeighbors
                        if (uNeighbors == null)
                            continue;
                        for (int v : uNeighbors) {
                            Set<Integer> vNeighbors = fnl[v];
                            if (vNeighbors == null)
                                continue;
                            for (int w : vNeighbors) {
                                if (uNeighbors.contains(w)) {

                                    // A triangle with vertices u,v,w is found.
                                    // Construct edges from lower vertex Id to higher vertex Id
                                    long uv = u < v ? (long) u << 32 | v & 0xFFFFFFFFL : (long) v << 32 | u & 0xFFFFFFFFL; // edge uv
                                    long vw = v < w ? (long) v << 32 | w & 0xFFFFFFFFL : (long) w << 32 | v & 0xFFFFFFFFL; // edge vw
                                    long uw = u < w ? (long) u << 32 | w & 0xFFFFFFFFL : (long) w << 32 | u & 0xFFFFFFFFL; // edge uw

                                    Set<Integer> set = eVerticesMap.get(uv);
                                    if (set == null) {
                                        set = new HashSet<>();
                                        eVerticesMap.put(uv, set);
                                    }
                                    set.add(w);

                                    set = eVerticesMap.get(uw);
                                    if (set == null) {
                                        set = new HashSet<>();
                                        eVerticesMap.put(uw, set);
                                    }
                                    set.add(v);

                                    set = eVerticesMap.get(vw);
                                    if (set == null) {
                                        set = new HashSet<>();
                                        eVerticesMap.put(vw, set);
                                    }
                                    set.add(u);

                                    if (eVerticesMap.size() > syncSize) {
                                        synchronized (lock) {
                                            Utils.updateEdgeMap(eMap, eVerticesMap);
                                        }
                                        eVerticesMap.clear();
                                        syncSizeSelector.compareAndSet(batchSize, 0);
                                        syncSize = syncSizeSelector.getAndAdd(syncIncrement) + batchSize;
                                    }
                                }
                            }
                        }
                    }
                }
                if (eVerticesMap.size() > 0) {
                    synchronized (lock) {
                        Utils.updateEdgeMap(eMap, eVerticesMap);
                    }
                    eVerticesMap = new HashMap<>();
                }
            })).get();

        long t4 = System.currentTimeMillis();
        System.out.println("TriangleSubgraph finished, edgeMapSize: " + eMap.size() + ", duration: " + (t4 - t3) + " ms");

        return eMap;
    }



}
