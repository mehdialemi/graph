package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.GraphUtils;
import ir.ac.sbu.graph.PartitioningUtils;
import ir.ac.sbu.graph.VertexCompare;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import scala.Tuple3;

import java.io.IOException;
import java.util.BitSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 *
 */
public class ParallelKTruss5 extends ParallelKTrussBase {

    private final ForkJoinPool forkJoinPool;

    public ParallelKTruss5(Edge[] edges, int minSup, int threads) {
        super(edges, minSup, threads);
        forkJoinPool = new ForkJoinPool(threads);
    }

    @Override
    public void start() throws Exception {
        Tuple3<int[][], int[], Integer> result = GraphUtils.createNeighbor(edges);
        int[][] neighbors = result._1();
        final int[] d = result._2();
        int vCount = d.length;
        long tStart = System.currentTimeMillis();
        final int[] partitions = PartitioningUtils.createPartitions(vCount, threads, BATCH_SIZE);
        long tep = System.currentTimeMillis();
        System.out.println("partition in " + (tep - tStart) + " ms");

        int sumDeg = 0;
        for (int u = 0; u < vCount; u++)
            sumDeg += d[u];
        final int degThreshold = (int) (sumDeg / (vCount * 0.2));
        long tDegThreshold = System.currentTimeMillis();
        System.out.println("avg degree: " + sumDeg / vCount + " degThreshold: " + degThreshold + " in " + (tDegThreshold - tStart) + " ms");

        final VertexCompare vertexCompare = new VertexCompare(d);
        int hCount = 0;
        final BitSet hSet = new BitSet();
        for (int u = 0; u < vCount; u++) {
            if (d[u] < degThreshold)
                continue;
            hSet.set(u);
            hCount++;
            int index = -1;
            int[] uNeighbors = neighbors[u];
            for (int vIdx = 0; vIdx < neighbors[u].length; vIdx++) {
                if (vertexCompare.compare(u, uNeighbors[vIdx]) == -1) {
                    index++;
                    if (index == vIdx)
                        continue;

                    int hv = uNeighbors[vIdx];
                    uNeighbors[vIdx] = uNeighbors[index];
                    uNeighbors[index] = hv;
                }
            }
            int[] n = new int[uNeighbors.length + 1];
            n[0] = index + 1;
            System.arraycopy(uNeighbors, 0, n, 1, uNeighbors.length);
            neighbors[u] = n;
        }

        long tHighVertices = System.currentTimeMillis();
        System.out.println("hCount " + hCount + " in " + (tHighVertices - tDegThreshold) + " ms");

        byte[][] externals = new byte[vCount][];
        int[][] counts = new int[vCount][];
        int maxHDeg = 0;
        for (int u = 0; u < vCount; u++) {
            if (hSet.get(u)) {
                counts[u] = new int[neighbors[u][0] + 1];
                if (neighbors[u][0] > maxHDeg)
                    maxHDeg = neighbors[u][0];
            } else
                counts[u] = new int[d[u]];
        }

        System.out.println("maxHDeg: " + maxHDeg);

        final int maxFSize = Math.max(1, degThreshold * degThreshold);

        batchSelector = new AtomicInteger(0);

        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(partition -> {
                DataOutputBuffer out = new DataOutputBuffer(maxFSize);
                for (int u = 0; u < vCount; u++) {
                    if (partitions[u] != partition || d[u] < 2)
                        continue;

                    out.reset();

                    int[] uNeighbors = neighbors[u];
                    int uEnd = d[u];
                    int uStart = 0;
                    if (hSet.get(u)) {
                        uStart = 1;
                        uEnd = uNeighbors[0] + 1;
                    }

                    // Find triangle by checking connectivity of neighbors
                    for (int vi = uStart; vi < uEnd; vi++) {
                        int v = uNeighbors[vi];
                        if (vertexCompare.compare(u, v) == 1)
                            continue;

                        int[] vNeighbors = neighbors[v];
                        int vStart = 0;
                        int vEnd = d[v];
                        if (hSet.get(v)) {
                            vStart = 1;
                            vEnd = vNeighbors[0] + 1;
                        }

                        // intersection on u neighbors and v neighbors
                        for (int wi = vStart; wi < vEnd; wi++) {
                            int w = vNeighbors[wi];

                            if (vertexCompare.compare(v, w) != -1)
                                continue;

                            for (int vi2 = uStart; vi2 < uEnd; vi2++) {
                                if (vi == vi2)
                                    continue;
                                if (uNeighbors[vi2] == w) {
                                    counts[u][vi2]++;
                                    counts[u][vi]++;
                                    if (partitions[v] == partition)
                                        counts[v][wi]++;
                                    else {
                                        try {
                                            WritableUtils.writeVInt(out, v);
                                            WritableUtils.writeVInt(out, wi);
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }

                    if (out.getLength() == 0)
                        continue;

                    externals[u] = new byte[out.getLength()];
                    System.arraycopy(out.getData(), 0, externals[u], 0, out.getLength());
                }
            }
        )).get();

        long tTC = System.currentTimeMillis();
        System.out.println("tc in " + (tTC - tStart) + " ms");

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> {
            IntStream.range(0, threads).parallel().forEach(partition -> {
                DataInputBuffer in = new DataInputBuffer();
                for (int u = 0; u < vCount; u++) {
                    if (externals[u] == null)
                        continue;
                    in.reset(externals[u], externals[u].length);
                    while (in.getPosition() < externals[u].length) {
                        try {
                            int v = WritableUtils.readVInt(in);
                            int vwIndex = WritableUtils.readVInt(in);
                            if (partitions[v] == partition)
                                counts[v][vwIndex]++;
                        } catch (IOException e) {
                        }
                    }
                }
            });
        }).get();

        long tUpdateCounts = System.currentTimeMillis();
        int sum = 0;
        for (
            int i = 0;
            i < vCount; i++)
            for (
                int j = 0;
                j < counts[i].length; j++)
                sum += counts[i][j];

        System.out.println("tCount: " + sum / 3 + " update counts in " + (tUpdateCounts - tTC) + " ms");
    }

    private void findTriangles(int vCount, int[][] neighbors, VertexCompare vertexCompare, int maxFSize,
                               byte[][] fonlNeighborL1, byte[][] fonlNeighborL2)
        throws InterruptedException, ExecutionException {


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
