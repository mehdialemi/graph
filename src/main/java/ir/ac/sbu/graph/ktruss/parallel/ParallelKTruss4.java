package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.VertexCompare;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 *
 */
public class ParallelKTruss4 extends ParallelKTrussBase {

    private final ForkJoinPool forkJoinPool;

    public ParallelKTruss4(Edge[] edges, int minSup, int threads) {
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
            neighbors[i] = new int[d[i] + 1];

        int[] pos = new int[vCount];
        for (Edge e : edges) {
            int dv1 = d[e.v1];
            int dv2 = d[e.v2];
            if (dv1 == dv2) {
                dv1 = e.v1;
                dv2 = e.v2;
            }
            if (dv1 < dv2) {
                neighbors[e.v1][++neighbors[e.v1][0]] = e.v2;
                neighbors[e.v2][d[e.v2] - pos[e.v2]++] = e.v1;

            } else {
                neighbors[e.v2][++neighbors[e.v2][0]] = e.v1;
                neighbors[e.v1][d[e.v1] - pos[e.v1]++] = e.v2;
            }
        }

        long tInitFonl = System.currentTimeMillis();
        System.out.println("initialize fonl " + (tInitFonl - t2) + " ms");

        final VertexCompare vertexCompare = new VertexCompare(d);
        batchSelector = new AtomicInteger(0);
        final int maxFSize = forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().map(i -> {
            int maxFonlSize = 0;
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount)
                    break;
                int end = Math.min(vCount, BATCH_SIZE + start);

                for (int u = start; u < end; u++) {
                    if (neighbors[u][0] < 2)
                        continue;
                    vertexCompare.quickSort(neighbors[u], 1, neighbors[u][0]);
                    if (maxFonlSize < neighbors[u][0])
                        maxFonlSize = neighbors[u][0];
                    Arrays.sort(neighbors[u], neighbors[u][0] + 1, neighbors[u].length);
                }
            }
            return maxFonlSize;
        })).get().reduce((a, b) -> Integer.max(a, b)).getAsInt();

        long t3 = System.currentTimeMillis();
        System.out.println("sort fonl in " + (t3 - t2) + " ms");

        // number of edges in triangles per vertex
        long tsCounts = System.currentTimeMillis();
        AtomicInteger[][] counts = new AtomicInteger[vCount][];
        int threadCount = Math.max(Math.min(threads, 2), threads / 2);
        System.out.println("using " + threadCount + " threads to construct counts");

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threadCount).parallel().forEach(partition -> {
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount)
                    break;
                int end = Math.min(vCount, BATCH_SIZE + start);
                for (int u = start; u < end; u++) {
                    if (neighbors[u][0] == 0)
                        continue;
                    counts[u] = new AtomicInteger[neighbors[u][0]];
                    for (int i = 0; i < neighbors[u][0]; i++)
                        counts[u][i] = new AtomicInteger(0);
                }
            }
        })).get();

        long tCounts = System.currentTimeMillis();
        System.out.println("construct counts in " + (tCounts - tsCounts) + " ms");

        BitSet[] threadBitSets = new BitSet[threads];
        for(int i = 0; i < threads; i ++)
            threadBitSets[i] = new BitSet(vCount);

        byte[][] fonlNeighborL1 = new byte[vCount][];
        byte[][] fonlNeighborL2 = new byte[vCount][];
        batchSelector = new AtomicInteger(0);
        BitSet involvedBitSet = new BitSet(vCount);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(thread -> {
            int[] vIndexes = new int[maxFSize];
            int[] lens = new int[maxFSize];
            DataOutputBuffer out1 = new DataOutputBuffer(maxFSize);
            DataOutputBuffer out2 = new DataOutputBuffer(maxFSize);
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount)
                    break;
                int end = Math.min(vCount, BATCH_SIZE + start);

                for (int u = start; u < end; u++) {
                    if (neighbors[u][0] < 2)
                        continue;

                    int lastIndex = 0;
                    out2.reset();

                    int[] neighborsU = neighbors[u];
                    // Find triangle by checking connectivity of neighbors
                    for (int vIndex = 1; vIndex < neighborsU[0]; vIndex++) {
                        int v = neighborsU[vIndex];
                        int[] vNeighbors = neighbors[v];

                        int intersection = 0;
                        // intersection on u neighbors and v neighbors
                        int uwIndex = vIndex + 1, vwIndex = 1;

                        while (uwIndex < neighbors[u][0] + 1 && vwIndex < neighbors[v][0] + 1) {
                            if (neighborsU[uwIndex] == vNeighbors[vwIndex]) {
                                try {
                                    counts[u][uwIndex - 1].incrementAndGet();
                                    counts[v][vwIndex - 1].incrementAndGet();
                                    WritableUtils.writeVInt(out2, uwIndex);
                                    WritableUtils.writeVInt(out2, vwIndex);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                                intersection++;
                                uwIndex++;
                                vwIndex++;
                            } else if (vertexCompare.compare(neighborsU[uwIndex], vNeighbors[vwIndex]) == -1)
                                uwIndex++;
                            else
                                vwIndex++;
                        }

                        if (intersection == 0)
                            continue;

                        involvedBitSet.set(v);
                        threadBitSets[thread].set(v);

                        vIndexes[lastIndex] = vIndex;
                        lens[lastIndex++] = intersection;
                    }

                    if (lastIndex == 0)
                        continue;

                    involvedBitSet.set(u);

                    out1.reset();
                    try {
                        WritableUtils.writeVInt(out1, lastIndex);
                        for (int j = 0; j < lastIndex; j++) {
                            WritableUtils.writeVInt(out1, lens[j]);
                            WritableUtils.writeVInt(out1, vIndexes[j]);
                            counts[u][vIndexes[j] - 1].addAndGet(lens[j]);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    fonlNeighborL1[u] = new byte[out1.getLength()];
                    System.arraycopy(out1.getData(), 0, fonlNeighborL1[u], 0, out1.getLength());

                    fonlNeighborL2[u] = new byte[out2.getLength()];
                    System.arraycopy(out2.getData(), 0, fonlNeighborL2[u], 0, out2.getLength());
                }
            }
        })).get();

        long tTC = System.currentTimeMillis();
        System.out.println("tc after counts: " + (tTC - tCounts) + " ms");
        System.out.println("tc duration: " + (tTC - tStart) + " ms");
        System.out.println("vCount: " + vCount + " involved vCount: " + involvedBitSet.cardinality());

        int sum = 0;
        for (AtomicInteger[] count : counts) {
            if (count == null)
                continue;
            for (AtomicInteger atomicInteger : count) {
                sum += atomicInteger.get();
            }
        }
        System.out.println("tc: " + sum / 3);
    }

    private int[] findPartition(int threads, int[][] fonls, int[] fl, byte[][] fonlNeighborL1, int[] pSizes) throws IOException {
        int[] partitions = new int[fonls.length];
        BitSet bitSet = new BitSet(fonls.length);
        for (int u = 0; u < partitions.length; u++) {
            partitions[u] = -1;
        }

        // create dominate set
        DataInputBuffer in = new DataInputBuffer();
        int neighborhood = 5;
        int currentPartition = 0;
        int added = 0;
        for (int u = 0; u < fonls.length; u++) {
            if (fonlNeighborL1[u] == null || bitSet.get(u))
                continue;
            int p = currentPartition % threads;
            added++;
            if (added % neighborhood == 0)
                currentPartition++;

            partitions[u] = p;
            pSizes[p]++;
            bitSet.set(u);
            // set neighbors of u in the bitSet
            in.reset(fonlNeighborL1[u], fonlNeighborL1[u].length);
            while (true) {
                if (in.getPosition() >= fonlNeighborL1[u].length)
                    break;

                int size = WritableUtils.readVInt(in);
                for (int i = 0; i < size; i++) {
                    WritableUtils.readVInt(in);  // skip len
                    int vIndex = WritableUtils.readVInt(in);
                    bitSet.set(fonls[u][vIndex]);
                }
            }
        }

        int[] pScores = new int[threads];
        for (int u = 0; u < fonls.length; u++) {
            if (fonlNeighborL1[u] == null || partitions[u] != -1)
                continue;

            // find appropriate findPartition
            int maxScore = 0;
            int targetPartition = 0;
            in.reset(fonlNeighborL1[u], fonlNeighborL1[u].length);
            for (int pIndex = 0; pIndex < pScores.length; pIndex++)
                pScores[pIndex] = 0;

            while (true) {
                if (in.getPosition() >= fonlNeighborL1[u].length)
                    break;

                int size = WritableUtils.readVInt(in);
                for (int i = 0; i < size; i++) {
                    WritableUtils.readVInt(in);  // skip len
                    int vIndex = WritableUtils.readVInt(in);
                    int v = fonls[u][vIndex];
                    if (partitions[v] == -1 || fonlNeighborL1[v] == null)
                        continue;

                    pScores[partitions[v]]++;
                    if (pScores[partitions[v]] > maxScore) {
                        maxScore = pScores[partitions[v]];
                        targetPartition = partitions[v];
                    }
                }
            }

            // find target findPartition => the smallest size findPartition with maximum maxScore
            for (int pIndex = 0; pIndex < pScores.length; pIndex++) {
                if (pIndex == targetPartition)
                    continue;
                if (pSizes[pIndex] < pSizes[targetPartition])
                    targetPartition = pIndex;
            }

            partitions[u] = targetPartition;
            pSizes[targetPartition]++;
        }
        return partitions;
    }
}
