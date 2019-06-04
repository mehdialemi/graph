package ir.ac.sbu.graph.others.ktruss.parallel;

import ir.ac.sbu.graph.types.Edge;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static ir.ac.sbu.graph.utils.MultiCoreUtils.createBuckets;

public class TriangleParallelForkJoin {

    public static Map<Long, Set<Integer>> findEdgeVerticesArray(final List<Edge> edges, final int threads, ForkJoinPool forkJoinPool,
                                                                float batchRatio)
        throws Exception {
        long t1 = System.currentTimeMillis();

        // for each thread, getOrCreateLFonl a map from vertex id to its degree
        final List<Tuple2<Integer, Integer>> edgeBuckets = createBuckets(threads, edges.size());
        Stream<Map<Integer, Integer>> vdResult = forkJoinPool.submit(() -> {
            return edgeBuckets.parallelStream().map(bucket -> {
                Map<Integer, Integer> localVertexDegree = new HashMap<>();
                for (int i = bucket._1; i < bucket._2; i++) {
                    int v1 = edges.get(i).v1;
                    Integer count = localVertexDegree.get(v1);
                    if (count == null)
                        count = 0;
                    localVertexDegree.put(v1, count + 1);

                    int v2 = edges.get(i).v2;
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

        // getOrCreateLFonl an array to hold each vertex's degree. index of this array corresponds to vertex Id
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
                Edge e = edges.get(i);
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

                // Each thread getOrCreateLFonl an output as Map which represents edge as a key and a list of pair ver as value.
                while (start < vertexNum) {
                    start = batchSelector.getAndAdd(batchSize);

                    // for each vertex u (index of the array is equal to its Id)
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

                                    // A triangle with vertices u,vertex,w is found.
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

    public static Tuple2<long[][], Map<Long, List<Integer>>> findEdgeTrianglesMR(final List<Edge> edges, final int threads,
                                                                                 ForkJoinPool forkJoinPool, float batchRatio)
        throws Exception {
        // find max vertex Id in parallel
        long t1 = System.currentTimeMillis();

        final List<Tuple2<Integer, Integer>> edgeBuckets = createBuckets(threads, edges.size());
        final Map<Integer, Integer> vertexDegree = new HashMap<>();
        Stream<Map<Integer, Integer>> vdResult = forkJoinPool.submit(() -> {
            return edgeBuckets.parallelStream().map(bucket -> {
                Map<Integer, Integer> localVertexDegree = new HashMap<>();
                for (int i = bucket._1; i < bucket._2; i++) {
                    Integer count = localVertexDegree.get(edges.get(i).v1);
                    if (count == null)
                        count = 0;
                    localVertexDegree.put(edges.get(i).v1, ++count);

                    count = localVertexDegree.get(edges.get(i).v2);
                    if (count == null)
                        count = 0;
                    localVertexDegree.put(edges.get(i).v2, ++count);
                }
                return localVertexDegree;
            });
        }).get();

        // aggregate vertex degrees
        forkJoinPool.submit(() ->
            vdResult.forEach(map -> map.entrySet().parallelStream().forEach(vd -> {
                Integer num = vertexDegree.get(vd.getKey());
                if (num == null)
                    num = 0;
                vertexDegree.put(vd.getKey(), num + vd.getValue());
            }))).get();

        final int batchSize = (int) (vertexDegree.size() * batchRatio);
        long t2 = System.currentTimeMillis();
        System.out.println("Calculating vertex degree in " + (t2 - t1) + " ms");

        // Construct neighborhood

        // Fill neighbors array
        Stream<Map<Integer, List<Integer>>> localMaps = forkJoinPool.submit(() -> edgeBuckets.parallelStream().map(bucket -> {
            Map<Integer, List<Integer>> localMap = new HashMap<>();
            for (int i = bucket._1; i < bucket._2; i++) {
                Edge e = edges.get(i);
                Integer dv1 = vertexDegree.get(e.v1);
                Integer dv2 = vertexDegree.get(e.v2);
                if (dv1 < 2 || dv2 < 2)
                    continue;

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
        })).get();

        final Map<Integer, Set<Integer>> filteredNeighbors = new HashMap<>();
        forkJoinPool.submit(() -> localMaps.forEach(localMap -> {
            localMap.entrySet().parallelStream().forEach(entry -> {
                Set<Integer> list = filteredNeighbors.get(entry.getKey());
                if (list == null) {
                    list = new TreeSet<>();
                    filteredNeighbors.put(entry.getKey(), list);
                }
                list.addAll(entry.getValue());
            });
        })).get();

        long t3 = System.currentTimeMillis();
        System.out.println("Fill neighbors in " + (t3 - t2) + " ms, ready to triangle in " + (t3 - t1) + " ms");

        final AtomicInteger triangleOffset = new AtomicInteger(0);

        final ThreadLocal<Integer> triangleNums = new ThreadLocal<>();
        final Map<Long, Map<Long, List<Integer>>> localEdgeTriangles = new HashMap<>();
        final Map<Long, Map<Integer, long[]>> allLocalTriangles = new HashMap<>();
        forkJoinPool.submit(() -> filteredNeighbors.entrySet().parallelStream().forEach(uNeighbors -> {
            long id = Thread.currentThread().getId();
            int u = uNeighbors.getKey();
            final Set<Integer> neighbors = uNeighbors.getValue();
            uNeighbors.getValue().forEach(v -> {
                Set<Integer> vNeighbors = filteredNeighbors.get(v);
                Stream<Integer> wList = vNeighbors.stream().filter(vn -> neighbors.contains(vn));
                wList.forEach(w -> {
                        long uv = (long) u << 32 | v & 0xFFFFFFFFL;
                        long vw = (long) v << 32 | w & 0xFFFFFFFFL;
                        long uw = (long) u << 32 | w & 0xFFFFFFFFL;

                        if (!localEdgeTriangles.containsKey(id)) {
                            localEdgeTriangles.put(id, new HashMap<>());
                        }

                        if (!allLocalTriangles.containsKey(id)) {
                            allLocalTriangles.put(id, new HashMap<>());
                        }
                        Integer triangleNum = triangleNums.get();

                        if (triangleNum % batchSize == 0)
                            triangleNum = triangleOffset.getAndAdd(batchSize);

                        List<Integer> list = localEdgeTriangles.get(id).get(uv);
                        if (list == null) {
                            list = new ArrayList<>();
                            localEdgeTriangles.get(id).put(uv, list);
                        }
                        list.add(triangleNum);

                        list = localEdgeTriangles.get(id).get(uw);
                        if (list == null) {
                            list = new ArrayList<>();
                            localEdgeTriangles.get(id).put(uw, list);
                        }
                        list.add(triangleNum);

                        list = localEdgeTriangles.get(id).get(vw);
                        if (list == null) {
                            list = new ArrayList<>();
                            localEdgeTriangles.get(id).put(vw, list);
                        }
                        list.add(triangleNum);

                        allLocalTriangles.get(id).put(triangleNum, new long[]{vw, uv, uw});
                        triangleNums.set(++triangleNum);
                    }
                );
            });
        })).get();

        Map<Long, List<Integer>> edgeTriangles = new HashMap<>();
        forkJoinPool.submit(() -> localEdgeTriangles.entrySet().forEach(entry -> entry.getValue().entrySet().parallelStream().forEach(et -> {
            List<Integer> list = edgeTriangles.get(et.getKey());
            if (list == null) {
                list = new ArrayList<>();
                edgeTriangles.put(et.getKey(), list);
            }
            list.addAll(et.getValue());
        }))).get();

        long[][] triangles = new long[triangleOffset.getAndAdd(batchSize) + batchSize * threads][];
        forkJoinPool.submit(() -> allLocalTriangles.entrySet().forEach(entry -> entry.getValue().entrySet().parallelStream().forEach(triangle -> {
            triangles[triangle.getKey()] = triangle.getValue();
        }))).get();

        return new Tuple2<>(triangles, edgeTriangles);
    }

}
