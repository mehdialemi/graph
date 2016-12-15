package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.utils.VertexCompare;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 *
 */
public class ParallelKTruss6 extends ParallelKTrussBase {

    public static final int INT_SIZE = 4;
    private final ForkJoinPool forkJoinPool;

    public ParallelKTruss6(Edge[] edges, int minSup, int threads) {
        super(edges, minSup, threads);
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
        System.out.println("find maxVertexNum in " + (tMax - tStart) + " ms");

        // Construct degree arrayList such that vertexId is the index of the arrayList.
        final int vCount = maxVertexNum + 1;
        int[] d = new int[vCount];  // vertex degree
        for (Edge e : edges) {
            d[e.v1]++;
            d[e.v2]++;
        }
        long t2 = System.currentTimeMillis();
        System.out.println("find degrees in " + (t2 - tStart) + " ms");

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
        System.out.println("initialize fonl " + (tInitFonl - t2) + " ms");
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
        System.out.println("sort fonl in " + (tSort - t2) + " ms");

        Long2ObjectOpenHashMap<IntSet>[] mapThreads = new Long2ObjectOpenHashMap[threads];
        for (int i = 0; i < threads; i++) {
            mapThreads[i] = new Long2ObjectOpenHashMap(vCount);
        }

        boolean[] veCount = new boolean[vCount];
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
                                if (!veCount[u] && sup == 1)
                                    veCount[u] = true;

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
                                if (!veCount[u] && sup == 1)
                                    veCount[u] = true;

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
                                if (!veCount[v] && sup == 1)
                                    veCount[v] = true;

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

        int len = maxSup + 1;
        AtomicInteger[] eCounts = new AtomicInteger[len];
        for(int u = 0 ; u < vCount; u ++) {
            if (veCount[u])
                continue;
            for(int i = 0 ; i < flen[u]; i ++) {
                if (veSups[u][i].get() == 0)
                    continue;
                eCounts[veSups[u][i].get()].incrementAndGet();
            }
        }

        for(int i = 1 ; i < len; i ++)
            eCounts[i].addAndGet(eCounts[i - 1].get());

        len = eCounts[maxSup].get();
        long[] eSorted = new long[len];
        Long2IntMap edgeToIndexMap = new Long2IntOpenHashMap();
        for(int u = 0 ; u < vCount; u ++) {
            if (veCount[u])
                continue;
            for(int i = 0 ; i < flen[u]; i ++) {
                if (veSups[u][i].get() == 0)
                    continue;
                long e = (long) u << 32 | neighbors[u][i] & 0xFFFFFFFFL;
                int index = eCounts[veSups[u][i].get()].decrementAndGet();
                eSorted[index] = e;
                edgeToIndexMap.put(e, index);
            }
        }
        long tSortE = System.currentTimeMillis();
        System.out.println("Sort edges based on counts in " + (tSortE - tTC) + " ms");

        int max = 0;
        for (int i = 0 ; i < threads; i ++) {
            if (max < mapThreads[i].size())
                max = mapThreads[i].size();
        }

        Long2ObjectMap<IntSet> map = Long2ObjectMaps.synchronize(mapThreads[0]);
        map.putAll(mapThreads[0]);
        for (int i = 1 ; i < threads; i ++) {
            final int index = i;
            forkJoinPool.submit(() -> mapThreads[index].long2ObjectEntrySet().parallelStream().forEach(edge -> {
                IntSet set = map.get(edge.getLongKey());
                if (set == null)
                    map.put(edge.getLongKey(), edge.getValue());
                else
                    set.addAll(edge.getValue());
            })).get();
        }
        long tAgg = System.currentTimeMillis();
        System.out.println("Aggregate in " + (tAgg - tSortE) + " ms");

        int sum = 0;
        for (Map.Entry<Long, IntSet> entry : map.entrySet()) {
            sum += entry.getValue().size();
        }
        System.out.println("tc: " + sum / 3);
    }
}
