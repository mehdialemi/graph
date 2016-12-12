package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.utils.VertexCompare;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Arrays;
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

        int[] pos = new int[vCount];
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
                neighbors[e.v2][d[e.v2] - 1 - pos[e.v2]++] = e.v1;

            } else {
                neighbors[e.v2][flen[e.v2]++] = e.v1;
                neighbors[e.v1][d[e.v1] - 1 - pos[e.v1]++] = e.v2;
            }
        }

        long tInitFonl = System.currentTimeMillis();
        System.out.println("initialize fonl " + (tInitFonl - t2) + " ms");

        final VertexCompare vertexCompare = new VertexCompare(d);
        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(i -> {
            int maxFonlSize = 0;
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount)
                    break;
                int end = Math.min(vCount, BATCH_SIZE + start);

                for (int u = start; u < end; u++) {
                    if (flen[u] < 2)
                        continue;
                    vertexCompare.quickSort(neighbors[u], 0, flen[u] - 1);
                    if (maxFonlSize < flen[u])
                        maxFonlSize = flen[u];
                    Arrays.sort(neighbors[u], flen[u], neighbors[u].length);
                }
            }
        })).get();

        long t3 = System.currentTimeMillis();
        System.out.println("sort fonl in " + (t3 - t2) + " ms");

        Long2ObjectOpenHashMap<IntSet>[] map = new Long2ObjectOpenHashMap[threads];
        for (int i = 0; i < threads; i++) {
            map[i] = new Long2ObjectOpenHashMap(vCount);
        }

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(thread -> {
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

                        int count = 0;
                        // intersection on u neighbors and v neighbors
                        int uwIndex = vIndex + 1, vwIndex = 0;

                        while (uwIndex < flen[u] && vwIndex < flen[v]) {
                            if (neighborsU[uwIndex] == vNeighbors[vwIndex]) {
                                int w = neighborsU[uwIndex];
                                IntSet set = map[thread].get(uv);
                                if (set == null) {
                                    set = new IntOpenHashSet();
                                    map[thread].put(uv, set);
                                }
                                set.add(w);

                                long uw = (long) u << 32 | w & 0xFFFFFFFFL;
                                set = map[thread].get(uw);
                                if (set == null) {
                                    set = new IntOpenHashSet();
                                    map[thread].put(uw, set);
                                }
                                set.add(v);

                                long vw = (long) v << 32 | w & 0xFFFFFFFFL;
                                set = map[thread].get(vw);
                                if (set == null) {
                                    set = new IntOpenHashSet();
                                    map[thread].put(vw, set);
                                }
                                set.add(u);

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
        })).get();

        long tTC = System.currentTimeMillis();
        System.out.println("tc duration: " + (tTC - tStart) + " ms");

    }
}
