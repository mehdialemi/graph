package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.PartitioningUtils;
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
public class ParallelKTruss5 extends ParallelKTrussBase {

    private final ForkJoinPool forkJoinPool;

    public ParallelKTruss5(Edge[] edges, int minSup, int threads) {
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
        System.out.println("Find degrees in " + (t2 - tStart) + " ms");

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

        int[] partitions = PartitioningUtils.createPartitions(vCount, threads, BATCH_SIZE);

        long tInitFonl = System.currentTimeMillis();
        System.out.println("Initialize fonl " + (tInitFonl - t2) + " ms");

        final VertexCompare vertexCompare = new VertexCompare(d);
        batchSelector = new AtomicInteger(0);
        final int maxFSize = forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().map(partition -> {
            int maxFonlSize = 0;
            for (int u = 0; u < vCount; u++) {
                if (partitions[u] != partition || neighbors[u][0] < 2)
                    continue;
                vertexCompare.quickSort(neighbors[u], 1, neighbors[u][0]);
                if (maxFonlSize < neighbors[u][0])
                    maxFonlSize = neighbors[u][0];
                Arrays.sort(neighbors[u], neighbors[u][0] + 1, neighbors[u].length);

            }
            return maxFonlSize;
        })).get().reduce((a, b) -> Integer.max(a, b)).getAsInt();

        long t3 = System.currentTimeMillis();
        System.out.println("Sort fonl in " + (t3 - t2) + " ms");

        byte[][] fonlOutLinks = new byte[vCount][];
        int[][] counts = new int[vCount][];
        for (int u = 0; u < vCount; u++)
            counts[u] = new int[neighbors[u][0]];

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(partition -> {
            DataOutputBuffer out = new DataOutputBuffer(maxFSize * maxFSize);
            for (int u = 0; u < vCount; u++) {
                if (partitions[u] != partition || neighbors[u][0] < 2)
                    continue;

                out.reset();
                int[] neighborsU = neighbors[u];
                // Find triangle by checking connectivity of neighbors
                for (int vIndex = 1; vIndex < neighborsU[0]; vIndex++) {
                    int v = neighborsU[vIndex];
                    int[] neighborsV = neighbors[v];

                    // intersection on u neighbors and v neighbors
                    int uwIndex = vIndex + 1, vwIndex = 1;
                    while (uwIndex < neighborsU[0] + 1 && vwIndex < neighborsV[0] + 1) {
                        if (neighborsU[uwIndex] == neighborsV[vwIndex]) {
                            counts[u][vIndex - 1]++;
                            counts[u][uwIndex - 1]++;
                            if (partitions[v] == partition) {
                                counts[v][vwIndex - 1]++;
                            } else {
                                try {
                                    WritableUtils.writeVInt(out, v);
                                    WritableUtils.writeVInt(out, vwIndex);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            uwIndex++;
                            vwIndex++;
                        } else if (vertexCompare.compare(neighborsU[uwIndex], neighborsV[vwIndex]) == -1)
                            uwIndex++;
                        else
                            vwIndex++;
                    }
                }

                if (out.getLength() == 0)
                    continue;

                fonlOutLinks[u] = new byte[out.getLength()];
                System.arraycopy(out.getData(), 0, fonlOutLinks[u], 0, out.getLength());
            }
        })).get();

        long tTC = System.currentTimeMillis();
        System.out.println("tc after fonl: " + (tTC - t3) + " ms");
        System.out.println("tc duration: " + (tTC - tStart) + " ms");

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> {
            IntStream.range(0, threads).forEach(partition -> {
                DataInputBuffer in = new DataInputBuffer();
                for (int u = 0; u < vCount; u++) {
                    if (fonlOutLinks[u] == null)
                        continue;
                    in.reset(fonlOutLinks[u], fonlOutLinks[u].length);
                    while (in.getPosition() < fonlOutLinks[u].length) {
                        try {
                            int v = WritableUtils.readVInt(in);
                            int vwIndex = WritableUtils.readVInt(in);
                            if (partitions[v] == partition)
                                counts[v][vwIndex - 1]++;
                        } catch (IOException e) {
                        }
                    }
                }
            });
        }).get();

        long tFinal = System.currentTimeMillis();
        int sum = 0;
        for (int i = 0; i < vCount; i++)
            for (int j = 0; j < counts[i].length; j++)
                sum += counts[i][j];

        System.out.println("tCount: " + sum / 3 + " find eCount in " + (tFinal - tTC) + " ms");
//        System.out.println("tcCount: " + tcCount);

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
