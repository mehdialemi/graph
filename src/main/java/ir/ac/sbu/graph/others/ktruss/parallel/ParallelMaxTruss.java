package ir.ac.sbu.graph.others.ktruss.parallel;

import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.utils.VertexCompare;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 *
 */
public class ParallelMaxTruss extends ParallelKTrussBase {

    private final ForkJoinPool forkJoinPool;

    public ParallelMaxTruss(Edge[] edges, int threads) {
        super(edges, 1, threads);
        forkJoinPool = new ForkJoinPool(threads);
    }

    @Override
    public void start() throws Exception {
        long tStart = System.currentTimeMillis();
        batchSelector = new AtomicInteger(0);
        int maxVertexNum = 0;
        for (Edge edge : edges) {
            if (edge.v1 > maxVertexNum)
                maxVertexNum = edge.v1;
            if (edge.v2 > maxVertexNum)
                maxVertexNum = edge.v2;
        }

        long tMax = System.currentTimeMillis();

        // Construct degree arrayList such that vertexId is the index of the arrayList.
        final int vCount = maxVertexNum + 1;
        int[] d = new int[vCount];  // vertex degree
        for (Edge e : edges) {
            d[e.v1]++;
            d[e.v2]++;
        }
        long t2 = System.currentTimeMillis();
//        System.out.println("find degrees in " + (t2 - tStart) + " ms");

        final int[][] neighbors = new int[vCount][];
        for (int i = 0; i < vCount; i++)
            neighbors[i] = new int[d[i]];

        int[] flen = new int[vCount];
        for (Edge e : edges) {
            int dv1 = d[e.v1];
            int dv2 = d[e.v2];
            if (dv1 == dv2) {
                dv1 = e.v1;
                dv2 = e.v2;
            }
            if (dv1 < dv2) {
                neighbors[e.v1][flen[e.v1]++] = e.v2;

            } else {
                neighbors[e.v2][flen[e.v2]++] = e.v1;
            }
        }

        long tInitFonl = System.currentTimeMillis();
//        System.out.println("initialize fonl " + (tInitFonl - t2) + " ms");
        AtomicInteger[][] veSups = new AtomicInteger[vCount][];

        final VertexCompare vertexCompare = new VertexCompare(d);
        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(thread -> {
            int maxFonlSize = 0;
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount)
                    break;
                int end = Math.min(vCount, BATCH_SIZE + start);

                for (int u = start; u < end; u++) {
                    if (flen[u] == 0)
                        continue;

                    veSups[u] = new AtomicInteger[flen[u]];
                    for (int i = 0; i < flen[u]; i++)
                        veSups[u][i] = new AtomicInteger(0);

                    if (flen[u] < 2) {
                        continue;
                    }
                    vertexCompare.quickSort(neighbors[u], 0, flen[u] - 1);
                    if (maxFonlSize < flen[u])
                        maxFonlSize = flen[u];
                }
            }
        })).get();

        long tSort = System.currentTimeMillis();
        long tCreateFonl = tSort - tInitFonl;
        System.out.println("getOrCreate fonl in " + tCreateFonl + " ms");

        Long2ObjectOpenHashMap<IntSet>[] mapThreads = new Long2ObjectOpenHashMap[threads];
        for (int i = 0; i < threads; i++) {
            mapThreads[i] = new Long2ObjectOpenHashMap(vCount);
        }

        boolean[] include = new boolean[vCount];
        batchSelector = new AtomicInteger(0);
        int maxSup = forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().map(thread -> {
            int max = 0;
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount)
                    break;
                int end = Math.min(vCount, BATCH_SIZE + start);

                for (int u = start; u < end; u++) {
                    if (flen[u] < 2)
                        continue;

                    int[] neighborsU = neighbors[u];
                    // Find triangle by checking connectivity of neighbors
                    for (int vIndex = 0; vIndex < flen[u]; vIndex++) {
                        int v = neighborsU[vIndex];
                        int[] vNeighbors = neighbors[v];

                        long uv = (long) u << 32 | v & 0xFFFFFFFFL;

                        // intersection on u neighbors and v neighbors
                        int uwIndex = vIndex + 1, vwIndex = 0;

                        while (uwIndex < flen[u] && vwIndex < flen[v]) {
                            if (neighborsU[uwIndex] == vNeighbors[vwIndex]) {
                                int w = neighborsU[uwIndex];
                                IntSet set = mapThreads[thread].get(uv);
                                if (set == null) {
                                    set = new IntOpenHashSet();
                                    mapThreads[thread].put(uv, set);
                                }
                                set.add(w);
                                int sup = veSups[u][vIndex].incrementAndGet();
                                if (sup > max)
                                    max = sup;
                                if (!include[u] && sup == 1)
                                    include[u] = true;

                                long uw = (long) u << 32 | w & 0xFFFFFFFFL;
                                set = mapThreads[thread].get(uw);
                                if (set == null) {
                                    set = new IntOpenHashSet();
                                    mapThreads[thread].put(uw, set);
                                }
                                set.add(v);
                                sup = veSups[u][uwIndex].incrementAndGet();
                                if (sup > max)
                                    max = sup;
                                if (!include[u] && sup == 1)
                                    include[u] = true;

                                long vw = (long) v << 32 | w & 0xFFFFFFFFL;
                                set = mapThreads[thread].get(vw);
                                if (set == null) {
                                    set = new IntOpenHashSet();
                                    mapThreads[thread].put(vw, set);
                                }
                                set.add(u);
                                sup = veSups[v][vwIndex].incrementAndGet();
                                if (sup > max)
                                    max = sup;
                                if (!include[v] && sup == 1)
                                    include[v] = true;

                                uwIndex++;
                                vwIndex++;
                            } else if (vertexCompare.compare(neighborsU[uwIndex], vNeighbors[vwIndex]) == -1)
                                uwIndex++;
                            else
                                vwIndex++;
                        }
                    }
                }
            }
            return max;
        })).get().reduce((a, b) -> Math.max(a, b)).getAsInt();
        long tTC = System.currentTimeMillis();
        System.out.println("tc duration: " + (tTC - tSort) + " ms");

        final IntArrayList[] localInvalids = new IntArrayList[threads];
        for (int i = 0; i < threads; i++) {
            localInvalids[i] = new IntArrayList(maxSup / threads);
        }

        int index = 0;
        for (int u = 0; u < vCount; u++) {
            if (!include[u]) {
                continue;
            }
            for (int i = 0; i < flen[u]; i++) {
                int sup = veSups[u][i].get();
                if (sup == 0) {
                    continue;
                }
                index ++;
            }
        }

        int len = index + 1;
        long[] edgeIndexes = new long[len];
        AtomicInteger[] edgeSup = new AtomicInteger[len];
        final int[] invalidIndexes = new int[len];
        Long2IntMap edgeToIndexMap = new Long2IntOpenHashMap(len);
        index = 0;
        for (int u = 0; u < vCount; u++) {
            if (!include[u]) {
                continue;
            }
            for (int i = 0; i < flen[u]; i++) {
                int sup = veSups[u][i].get();
                if (sup == 0)
                    continue;

                long e = (long) u << 32 | neighbors[u][i] & 0xFFFFFFFFL;
                edgeToIndexMap.put(e, index);
                edgeIndexes[index] = e;
                edgeSup[index] = new AtomicInteger(sup);
                index ++;
            }
        }

        int min = 1;
        int max = 2;
        int kmIter = 0;
        while (true) {
            final int supUpper = max;
            final int supLower = min;
            long tm1 = System.currentTimeMillis();

            int partition = 0;
            int batchSize = BATCH_SIZE;
            int rem = vCount - threads * batchSize * 2;
            boolean finish = true;
            for (int u = 0; u < vCount; u++) {
                if (!include[u]) {
                    continue;
                }
                boolean exclude = true;
                for (int i = 0; i < flen[u]; i++) {
                    int sup = veSups[u][i].get();
                    if (sup == 0 || sup < supLower)
                        continue;
                    exclude = false;
                    finish = false;
                    if (sup > supUpper)
                        continue;

                    long e = (long) u << 32 | neighbors[u][i] & 0xFFFFFFFFL;
                    int idx = edgeToIndexMap.get(e);
                    int p = partition % threads;
                    localInvalids[p].add(idx);
                    if ((u + 1) % batchSize == 0)
                        partition++;
                    if (p == 0 && u > rem) {
                        batchSize = Math.max(batchSize / 2, 10);
                        rem = vCount - threads * batchSize * 2;
                    }
                }
                if (exclude)
                    include[u] = false;
            }

            if (finish)
                break;

            int removed = 0;
            while (true) {
                long t1 = System.currentTimeMillis();
                List<Tuple2<Integer, IntArrayList>> fillList = new ArrayList<>();
                int last = 0;
                fillList.add(new Tuple2<>(last, localInvalids[0]));
                for (int i = 1; i < localInvalids.length; i++) {
                    last += localInvalids[i - 1].size();
                    fillList.add(new Tuple2<>(last, localInvalids[i]));
                }

                final int invalidSize = forkJoinPool.submit(() -> fillList.parallelStream().map(packet -> {
                    int offset = packet._1;
                    for (int i = 0; i < packet._2.size(); i++) {
                        invalidIndexes[i + offset] = packet._2.getInt(i);
                    }
                    return packet._2.size();
                })).get().reduce((a, b) -> a + b).get();
                if (invalidSize == 0)
                    break;

                removed += invalidSize;

                for (int i = 0; i < localInvalids.length; i++)
                    localInvalids[i].clear();

                forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(thread -> {
                    for (int i = 0; i < invalidSize; i++) {
                        int idx = invalidIndexes[i];
                        long edge = edgeIndexes[idx];
                        IntSet list = mapThreads[thread].get(edge);
                        if (list == null)
                            continue;
                        IntIterator iterator = list.iterator();

                        while (iterator.hasNext()) {
                            int w = iterator.nextInt();
                            int u = (int) (edge >> 32);
                            int v = (int) edge;
                            long e;
                            if (vertexCompare.compare(u, w) == -1) {
                                e = (long) u << 32 | w & 0xFFFFFFFFL;
                            } else {
                                e = (long) w << 32 | u & 0xFFFFFFFFL;
                            }

                            idx = edgeToIndexMap.get(e);
                            int sup = edgeSup[idx].decrementAndGet();
                            if (sup == supUpper - 1) {
                                // add idx to invalid indexes
                                localInvalids[thread].add(idx);
                            }
                            IntSet vList = mapThreads[thread].get(edgeIndexes[idx]);
                            if (vList != null) {
                                vList.remove(v);
                                if (vList.size() == 0)
                                    mapThreads[thread].remove(edgeIndexes[idx]);
                            }

                            if (vertexCompare.compare(v, w) == -1) {
                                e = (long) v << 32 | w & 0xFFFFFFFFL;
                            } else {
                                e = (long) w << 32 | v & 0xFFFFFFFFL;
                            }
                            idx = edgeToIndexMap.get(e);
                            sup = edgeSup[idx].decrementAndGet();
                            if (sup == supUpper - 1) {
                                // add idx to invalid indexes
                                localInvalids[thread].add(idx);
                            }
                            vList = mapThreads[thread].get(edgeIndexes[idx]);
                            if (vList != null) {
                                vList.remove(u);
                                if (vList.size() == 0)
                                    mapThreads[thread].remove(edgeIndexes[idx]);
                            }
                        }
                    }
                })).get();
            }
            long tm2 = System.currentTimeMillis();
            kmIter ++;
            System.out.println("Iteration: " + kmIter+ ", removed: " + removed + ", duration: " + (tm2 - tm1) + " ms");
            min++;
            max++;
        }
        long tcm = System.currentTimeMillis();
        System.out.println("Complete max truss in " + (tcm - tTC) + " ms");
    }
}
