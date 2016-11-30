package ir.ac.sbu.graph.ktruss.multicore;

import ir.ac.sbu.graph.Edge;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static ir.ac.sbu.graph.ktruss.multicore.MultiCoreUtils.createBuckets;

public class TriangleParallelForkJoin {
    public static final Tuple2<Integer, Integer> INVALID_TUPLE2 = new Tuple2<>(-1, -1);

    public static Tuple2<int[][], Set<Integer>[]> findEdgeTriangles(final List<Edge> edges, final int threads, ForkJoinPool forkJoinPool,
                                                                    float batchRatio)
        throws Exception {
        // find max vertex Id in parallel
        long t1 = System.currentTimeMillis();

        final List<Tuple2<Integer, Integer>> edgeBuckets = createBuckets(threads, edges.size());
        int max = forkJoinPool.submit(() -> {
            return edgeBuckets.parallelStream().mapToInt(bucket -> {
                int maxV = 0;
                for (int i = bucket._1; i < bucket._2; i++) {
                    int maxE = Math.max(edges.get(i).v1, edges.get(i).v2);
                    if (maxE > maxV)
                        maxV = maxE;
                }
                return maxV;
            }).reduce((max1, max2) -> Math.max(max1, max2)).getAsInt();
        }).get();

        long t2 = System.currentTimeMillis();
        System.out.println("Finding max in " + (t2 - t1) + " ms");

        if (max == -1)
            throw new Exception("Problem in max finding");

        // construct deg array
        AtomicInteger[] degArray = new AtomicInteger[max + 1];
        final int batchSize = (int) (degArray.length * batchRatio);
        forkJoinPool.submit(() -> IntStream.range(0, degArray.length).parallel().forEach(i -> degArray[i] = new AtomicInteger(0))).get();

        long t3 = System.currentTimeMillis();
        System.out.println("Construct degArray (AtomicInteger) in " + (t3 - t2) + " ms");

        // Construct degree array such that vertexId is the index of the array in parallel
        forkJoinPool.submit(() -> edgeBuckets.parallelStream().forEach(bucket -> {
            for (int i = bucket._1; i < bucket._2; i++) {
                degArray[edges.get(i).v1].incrementAndGet();
                degArray[edges.get(i).v2].incrementAndGet();
            }
        })).get();

        long t4 = System.currentTimeMillis();
        System.out.println("Fill degArray in " + (t4 - t3) + " ms");

        // Fill and quickSort vertices array.
        final int[] vertices = sort(degArray, threads, forkJoinPool);
        final int[] rvertices = new int[vertices.length];
        forkJoinPool.submit(() -> IntStream.range(0, threads).forEach(index -> {
            for (int i = index; i < vertices.length; i += threads)
                rvertices[vertices[i]] = i;
        })).get();

        long t5 = System.currentTimeMillis();
        System.out.println("Sort degArray in " + (t5 - t4) + " ms");

        // Construct neighborhood
        final List<Long>[] neighbors = new List[vertices.length];
        forkJoinPool.submit(() -> Arrays.stream(vertices).parallel().forEach(v -> neighbors[v] = new ArrayList<>())).get();

        long t6 = System.currentTimeMillis();
        System.out.println("Construct neighbors in " + (t6 - t5) + " ms");

        // Fill neighbors array
        forkJoinPool.submit(() -> edgeBuckets.parallelStream().forEach(bucket -> {
            for (int i = bucket._1; i < bucket._2; i++) {
                Edge e = edges.get(i);
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
        })).get();

        //TODO quickSort neighbor list???

        long t7 = System.currentTimeMillis();
        System.out.println("Fill neighbors in " + (t7 - t6) + " ms");
        long t8 = System.currentTimeMillis();

        long t9 = System.currentTimeMillis();
        System.out.println("Ready to triangle in " + (t9 - t8) + " ms with " + edgeBuckets.size() + " edgeBuckets");

        final int offset = forkJoinPool.submit(() -> (int) Arrays.stream(degArray).parallel().filter(d -> d.get() < 2).count()).get();
        final int length = vertices.length;
        final AtomicInteger blockOffset = new AtomicInteger(offset);
        final AtomicInteger triangleOffset = new AtomicInteger(0);

        Stream<Tuple2<Map<Integer, int[]>, Map<Integer, List<Integer>>>> results = forkJoinPool.submit(() -> {
            return IntStream.range(0, threads).parallel().mapToObj(index -> {
                Map<Integer, List<Integer>> localEdgeTriangles = new HashMap<>();
                Map<Integer, int[]> localTriangles = new HashMap<>();
                int triangleNum = triangleOffset.getAndAdd(batchSize);
                int i = 0;
                while (i < length) {
                    int start = blockOffset.getAndAdd(batchSize);
                    for (i = start; i < start + batchSize && i < length; i++) {
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
                                    if (triangleNum % batchSize == 0)
                                        triangleNum = triangleOffset.getAndAdd(batchSize);
                                }
                            }
                        }
                    }
                }
                return new Tuple2<>(localTriangles, localEdgeTriangles);
            });
        }).get();

        final HashSet<Integer>[] edgeTriangles = new HashSet[edges.size()];
        final List<Map<Integer, int[]>> localTrianglesList = new ArrayList<>(threads);
        final AtomicInteger triangleCount = new AtomicInteger(0);
        forkJoinPool.submit(() -> {
            results.forEach(result -> {
                Set<Map.Entry<Integer, List<Integer>>> localEdgeTriangles = result._2.entrySet();
                localEdgeTriangles.parallelStream().forEach(entry -> {
                    if (edgeTriangles[entry.getKey()] == null)
                        edgeTriangles[entry.getKey()] = new HashSet<>();
                    edgeTriangles[entry.getKey()].addAll(entry.getValue());
                });
                localTrianglesList.add(result._1);
                triangleCount.addAndGet(result._1.size());
            });
        }).get();

        System.out.println("triangle count: " + triangleCount.get());
        int[][] triangles = new int[triangleCount.get() + batchSize * threads][];
        forkJoinPool.submit(() ->
            localTrianglesList.parallelStream().forEach(localTriangles ->
                localTriangles.entrySet().forEach(entry ->
                    triangles[entry.getKey()] = entry.getValue())
            )).get();

        long t10 = System.currentTimeMillis();
        System.out.println("Triangle finished in " + (t10 - t9));
        return new Tuple2<>(triangles, edgeTriangles);
    }

    public static int[] sort(AtomicInteger[] degArray, int threads, ForkJoinPool forkJoinPool) throws Exception {
        List<Tuple2<Integer, Integer>> buckets = createBuckets(threads, degArray.length);
        Tuple2<Integer, Integer> minMaxLen = forkJoinPool.submit(() ->
            buckets.parallelStream().map(bucket -> {
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
                .orElse(INVALID_TUPLE2)
        ).get();

        if (minMaxLen == INVALID_TUPLE2)
            throw new Exception("Invalid tuple");

        int min = minMaxLen._1();
        int max = minMaxLen._2();

        // init the frequencies
        AtomicInteger[] counts = new AtomicInteger[max - min + 1];
        for (int i = 0; i < counts.length; i++)
            counts[i] = new AtomicInteger(0);
        forkJoinPool.submit(() -> buckets.parallelStream().forEach(bucket ->
            IntStream.range(bucket._1, bucket._2).forEach(i ->
                counts[degArray[i].get() - min].incrementAndGet()))
        ).get();

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


    public static Map<Long, Set<Integer>> findEdgeVerticesMap(final List<Edge> edges, final int threads, ForkJoinPool forkJoinPool,
                                                              float batchRatio)
        throws Exception {
        long t1 = System.currentTimeMillis();

        // for each thread, create a map from vertex id to its degree
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

        // create an array to hold each vertex's degree. index of this array corresponds to vertex Id
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

                // Each thread create an output as Map which represents edge as a key and a list of pair ver as value.
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
                                            updateEdgeMap(eMap, eVerticesMap);
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
                        updateEdgeMap(eMap, eVerticesMap);
                    }
                    eVerticesMap = new HashMap<>();
                }
            })).get();

        long t4 = System.currentTimeMillis();
        System.out.println("Triangle finished, edgeMapSize: " + eMap.size() + ", duration: " + (t4 - t3) + " ms");

        return eMap;
    }

    private static void updateEdgeMap(Map<Long, Set<Integer>> eMap, Map<Long, Set<Integer>> eVerticesMap) {
        eVerticesMap.entrySet().forEach(eVertices -> {
            Set<Integer> tv = eMap.get(eVertices.getKey());
            if (tv == null)
                eMap.put(eVertices.getKey(), eVertices.getValue());
            else
                tv.addAll(eVertices.getValue());
        });
    }

    public static Map<Long, Set<Integer>> findEdgeVerticesArray(final List<Edge> edges, final int threads, ForkJoinPool forkJoinPool,
                                                                float batchRatio)
        throws Exception {
        long t1 = System.currentTimeMillis();

        // for each thread, create a map from vertex id to its degree
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

        // create an array to hold each vertex's degree. index of this array corresponds to vertex Id
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

                // Each thread create an output as Map which represents edge as a key and a list of pair ver as value.
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
                                            updateEdgeMap(eMap, eVerticesMap);
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
                        updateEdgeMap(eMap, eVerticesMap);
                    }
                    eVerticesMap = new HashMap<>();
                }
            })).get();

        long t4 = System.currentTimeMillis();
        System.out.println("Triangle finished, edgeMapSize: " + eMap.size() + ", duration: " + (t4 - t3) + " ms");

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
