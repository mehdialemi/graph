package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
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
public class ParallelKTruss6 extends ParallelKTrussBase {

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
        System.out.println("len: " + len);
        int[] eCounts = new int[len];
        for (int u = 0; u < vCount; u++) {
            if (!veCount[u])
                continue;
            for (int i = 0; i < flen[u]; i++) {
                if (veSups[u][i].get() == 0)
                    continue;
                eCounts[veSups[u][i].get()]++;
            }
        }

        for (int i = 1; i < len; i++) {
            eCounts[i] += eCounts[i - 1];
        }

        int eCountLen = eCounts[maxSup];
        System.out.println("edge triangle: " + eCountLen);

        System.out.println("minSup: " + minSup);
        len = eCounts[maxSup] + 1;
        long[] eSorted = new long[len];
        AtomicInteger[] edgeSup = new AtomicInteger[len];

        final IntArrayList[] invalidEdgePerThread = new IntArrayList[threads];
        for (int i = 0; i < threads; i++) {
            invalidEdgePerThread[i] = new IntArrayList(len / threads);
        }

        int partition = 0;
        Long2IntMap edgeToIndexMap = new Long2IntOpenHashMap();
        int batchSize = BATCH_SIZE;
        int rem = vCount - threads * batchSize * 2;
        for (int u = 0; u < vCount; u++) {
            if (!veCount[u]) {
                continue;
            }
            for (int i = 0; i < flen[u]; i++) {
                int sup = veSups[u][i].get();
                if (sup == 0) {
                    continue;
                }
                long e = (long) u << 32 | neighbors[u][i] & 0xFFFFFFFFL;
                int index = eCounts[sup]--;
                if (sup < minSup) {
                    int p = partition % threads;
                    invalidEdgePerThread[p].add(index);

                    if ((u + 1) % batchSize == 0)
                        partition++;
                    if (p == 0 && u > rem) {
                        batchSize = Math.max(batchSize / 2, 10);
                        rem = vCount - threads * batchSize * 2;
                    }
                }
                eSorted[index] = e;
                edgeSup[index] = new AtomicInteger(sup);
                edgeToIndexMap.put(e, index);
            }
        }

        long tSortE = System.currentTimeMillis();
        System.out.println("sort edges based on counts in " + (tSortE - tTC) + " ms");
        int iter = 0;

        final int[] invalidIndexes = new int[len];
        while (true) {
            long t1 = System.currentTimeMillis();
            List<Tuple2<Integer, IntArrayList>> fillList = new ArrayList<>();
            int last = 0;
            fillList.add(new Tuple2<>(last, invalidEdgePerThread[0]));
            for (int i = 1; i < invalidEdgePerThread.length; i++) {
                last += invalidEdgePerThread[i - 1].size();
                fillList.add(new Tuple2<>(last, invalidEdgePerThread[i]));
            }

            final int invalidSize = forkJoinPool.submit(() -> fillList.parallelStream().map(packet -> {
                int offset = packet._1;
                for (int i = 0; i < packet._2.size(); i++) {
                    invalidIndexes[i + offset] = packet._2.getInt(i);
                }
                return packet._2.size();
            })).get().reduce((a, b) -> a + b).get();

            long tInvalid = System.currentTimeMillis();
            System.out.println("iteration " + ++iter + ", invalid size: " + invalidSize + " time: " + (tInvalid - t1) + " ms");
            if (invalidSize == 0)
                break;

            for(int i = 0 ; i < invalidEdgePerThread.length; i ++)
                invalidEdgePerThread[i].clear();

            forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(thread -> {
                for (int i = 0; i < invalidSize; i++) {
                    int index = invalidIndexes[i];
                    long edge = eSorted[index];
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

                        index = edgeToIndexMap.get(e);
                        int sup = edgeSup[index].decrementAndGet();
                        if (sup == minSup - 1) {
                            // add index to invalid indexes
                            invalidEdgePerThread[thread].add(index);
                        }
                        IntSet vList = mapThreads[thread].get(eSorted[index]);
                        if (vList != null) {
                            vList.remove(v);
                            if (vList.size() == 0)
                                mapThreads[thread].remove(eSorted[index]);
                        }

                        if (vertexCompare.compare(v, w) == -1) {
                            e = (long) v << 32 | w & 0xFFFFFFFFL;
                        } else {
                            e = (long) w << 32 | v & 0xFFFFFFFFL;
                        }
                        index = edgeToIndexMap.get(e);
                        sup = edgeSup[index].decrementAndGet();
                        if (sup == minSup - 1) {
                            // add index to invalid indexes
                            invalidEdgePerThread[thread].add(index);
                        }
                        vList = mapThreads[thread].get(eSorted[index]);
                        if (vList != null) {
                            vList.remove(u);
                            if (vList.size() == 0)
                                mapThreads[thread].remove(eSorted[index]);
                        }
                    }
                }
            })).get();
            long tUpdate = System.currentTimeMillis();
            System.out.println("Update in " + (tUpdate - tInvalid) + " ms");
        }

        long tk = System.currentTimeMillis();
        System.out.println("Complete in " + (tk - tSortE) + " ms");
    }
}
