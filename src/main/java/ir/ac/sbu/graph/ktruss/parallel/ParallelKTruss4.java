package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.MultiCoreUtils;
import ir.ac.sbu.graph.VertexCompare;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import java.util.BitSet;
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
        int maxVertexNum = forkJoinPool.submit(() ->
            IntStream.range(0, threads).map(index -> {
                int localMax = Integer.MIN_VALUE;
                while (true) {
                    int start = batchSelector.getAndAdd(BATCH_SIZE);
                    if (start >= edges.length)
                        break;
                    int end = Math.min(edges.length, start + BATCH_SIZE);
                    for (int i = start; i < end; i++) {
                        if (edges[i].v1 > edges[i].v2) {
                            if (edges[i].v1 > localMax)
                                localMax = edges[i].v1;
                        } else {
                            if (edges[i].v2 > localMax)
                                localMax = edges[i].v2;
                        }
                    }
                }
                return localMax;
            })).get().reduce((a, b) -> Math.max(a, b)).getAsInt();
        long tMax = System.currentTimeMillis();
        System.out.println("find maxVertexNum in " + (tMax - tStart) + " ms");

        // Construct degree arrayList such that vertexId is the index of the arrayList.
        final int length = maxVertexNum + 1;
        int[] d = new int[length];  // vertex degree
        for (Edge e : edges) {
            d[e.v1]++;
            d[e.v2]++;
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Find degrees in " + (t2 - tStart) + " ms");

        // Construct fonls and fonlCN
        final int[][] fonls = new int[length][];
        final int[] fl = new int[length];  // Fonl Length

        for (int i = 0 ; i < length ; i ++)
            fonls[i] = new int[Math.min(d[i], length - d[i])];

        long tInitFonl = System.currentTimeMillis();
        System.out.println("Initialize fonl " + (tInitFonl - t2) + " ms");

        // Fill neighbors arrayList
        for (Edge e : edges) {
            int dv1 = d[e.v1];
            int dv2 = d[e.v2];
            if (dv1 == dv2) {
                dv1 = e.v1;
                dv2 = e.v2;
            }
            if (dv2 > dv1)
                fonls[e.v1][fl[e.v1]++] = e.v2;
            else
                fonls[e.v2][fl[e.v2]++] = e.v1;
        }

        final VertexCompare vertexCompare = new VertexCompare(d);
        batchSelector = new AtomicInteger(0);
        final int maxFSize = forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().map(i -> {
            int maxFonlSize = 0;
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= length)
                    break;
                int end = Math.min(length, BATCH_SIZE + start);

                for (int u = start; u < end; u++) {
                    if (fl[u] < 2)
                        continue;
                    vertexCompare.quickSort(fonls[u], 0, fl[u] - 1);
                    if (maxFonlSize < fl[u])
                        maxFonlSize = fl[u];
                }
            }
            return maxFonlSize;
        })).get().reduce((a, b) -> Integer.max(a, b)).getAsInt();

        long t3 = System.currentTimeMillis();
        System.out.println("Create fonl in " + (t3 - t2) + " ms");

        byte[][] fonlNeighborL1 = new byte[length][];
        byte[][] fonlNeighborL2 = new byte[length][];
        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(i -> {
            int[] vIndexes = new int[maxFSize];
            int[] lens = new int[maxFSize];
            DataOutputBuffer outNeighborL1 = new DataOutputBuffer(maxFSize);
            DataOutputBuffer outNeighborL2 = new DataOutputBuffer(maxFSize);
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= length)
                    break;
                int end = Math.min(length, BATCH_SIZE + start);

                for (int u = start; u < end; u++) {
                    if (fl[u] < 2)
                        continue;

                    int jIndex = 0;

                    // Find triangle by checking connectivity of neighbors
                    for (int j = 0; j < fl[u]; j++) {
                        int[] fonl = fonls[u];
                        int v = fonl[j];
                        int[] vNeighbors = fonls[v];

                        int idx = 0;
                        // intersection on u neighbors and v neighbors
                        int f = j + 1, vn = 0;
                        while (f < fl[u] && vn < fl[v]) {
                            if (fonl[f] == vNeighbors[vn]) {
                                if (idx == 0)
                                    outNeighborL2.reset();
                                try {
                                    WritableUtils.writeVInt(outNeighborL2, f);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                                idx++;
                                f++;
                                vn++;
                            } else if (vertexCompare.compare(fonl[f], vNeighbors[vn]) == -1)
                                f++;
                            else
                                vn++;
                        }

                        if (idx == 0)
                            continue;

                        vIndexes[jIndex] = j;
                        lens[jIndex++] = idx;
                    }

                    if (jIndex == 0)
                        continue;

                    outNeighborL1.reset();
                    try {
                        WritableUtils.writeVInt(outNeighborL1, jIndex);
                        for (int j = 0; j < jIndex; j++) {
                            WritableUtils.writeVInt(outNeighborL1, lens[j]);
                            WritableUtils.writeVInt(outNeighborL1, vIndexes[j]);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    fonlNeighborL1[u] = new byte[outNeighborL1.getLength()];
                    System.arraycopy(outNeighborL1.getData(), 0, fonlNeighborL1[u], 0, outNeighborL1.getLength());

                    fonlNeighborL2[u] = new byte[outNeighborL2.getLength()];
                    System.arraycopy(outNeighborL2.getData(), 0, fonlNeighborL2[u], 0, outNeighborL2.getLength());
                }
            }
        })).get();

        long tTC = System.currentTimeMillis();
        System.out.println("tc after fonl: " + (tTC - t3) + " ms");
        System.out.println("tc duration: " + (tTC - tStart) + " ms");

        // ================ Partition fonls ===================

        // initialize partitions to -1
        int[] partitions = new int[length];
        BitSet bitSet = new BitSet(length);
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = -1;
        }

        // create dominate set
        for (int u = 0; u < fonls.length; u++) {

        }

        int tcCount = 0;
        DataInputBuffer in1 = new DataInputBuffer();
        DataInputBuffer in2 = new DataInputBuffer();

        for (int u = 0; u < fonlNeighborL1.length; u++) {
            if (fonlNeighborL1[u] == null)
                continue;

            in1.reset(fonlNeighborL1[u], fonlNeighborL1[u].length);
            in2.reset(fonlNeighborL2[u], fonlNeighborL2[u].length);
            while (true) {
                if (in1.getPosition() >= fonlNeighborL1[u].length)
                    break;

                int size = WritableUtils.readVInt(in1);
                for (int i = 0; i < size; i++) {
                    int len = WritableUtils.readVInt(in1);
                    int vIndex = WritableUtils.readVInt(in1);
//                    int v = fonls[u][vIndex];
//                    for (int j = 0; j < len; j++) {
//                        int uwIndex = WritableUtils.readVInt(in2);
//                        vertexEdges[u].add(uwIndex, v);
//
//                        int vwIndex = WritableUtils.readVInt(in2);
//                        if (vertexEdges[v] == null)
//                            vertexEdges[v] = new VertexEdge(fl[v]);
//                        vertexEdges[v].add(vwIndex, u);
//                    }

                    tcCount += len;
                }
            }
        }

        long tFinal = System.currentTimeMillis();
        System.out.println("fill eSup in " + (tFinal - tTC) + " ms");
        System.out.println("tcCount: " + tcCount);

    }
}
