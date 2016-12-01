package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.ProbabilityRandom;
import ir.ac.sbu.graph.VertexCompare;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import java.text.DecimalFormat;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 *
 */
public class ParallelMethod3 extends ParallelBase {

    private final ForkJoinPool forkJoinPool;

    public ParallelMethod3(Edge[] edges, int minSup, int threads) {
        super(edges, minSup, threads);
        forkJoinPool = new ForkJoinPool(threads);
    }

    @Override
    public void start() throws Exception {
        long tStart = System.currentTimeMillis();
        batchSelector = new AtomicInteger(0);
        int max = forkJoinPool.submit(() ->
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


        // Construct degree arrayList such that vertexId is the index of the arrayList.
        final int length = max + 1;
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
        final int[] partition = new int[length];

        // Initialize neighbors arrayList
        for (int i = 0; i < length; i++) {
            fonls[i] = new int[Math.min(d[i], length - d[i])];
            partition[i] = -1;
        }

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

        int[] pSizes = new int[threads];
        ProbabilityRandom pRandom = new ProbabilityRandom(threads, length);
        int[] pIndex = new int[length];
        for (int i = 0; i < fonls.length; i++) {
            if (fl[i] == 0 || partition[i] != -1)
                continue;
            int p = pRandom.getNextRandom();

            pRandom.increment(p);
            partition[i] = p;
            pIndex[i] = pSizes[p]++;
            for (int v : fonls[i]) {
                if (fl[v] == 0 || partition[v] != 0)
                    continue;
                partition[v] = p;
                pIndex[v] = pSizes[p]++;
                pRandom.increment(p);
            }
        }

        for (int i = 0; i < pSizes.length; i++) {
            System.out.println("Partition " + i + ", Size: " + pSizes[i]);
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

        final DataOutputBuffer[] fonlCN = new DataOutputBuffer[length];  // fonl common neighbors
        final DataOutputBuffer[] fonlVS = new DataOutputBuffer[length];  // fonl vertex size
        final DataOutputBuffer[] internalFU = new DataOutputBuffer[length];  // internal fonl update
        final DataOutputBuffer[][] externalFUs = new DataOutputBuffer[threads][];  // external fonl fonl update

        int maxPartitionSize = 0;
        for (int pSize : pSizes) {
            if (pSize > maxPartitionSize)
                maxPartitionSize = pSize;
        }

        for (int i = 0; i < externalFUs.length; i++) {
            externalFUs[i] = new DataOutputBuffer[maxPartitionSize];
        }

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(p -> {
            int[] lens = new int[maxFSize];
            int[] vIndexes = new int[maxFSize];
            DataOutputBuffer[] externalFU = externalFUs[p];
            try {
                int len = fonls.length;
                for (int u = 0; u < len; u++) {
                    if (fl[u] < 2 || partition[u] != p)
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
                                    fonlCN[u] = new DataOutputBuffer(fl[u] * 2);
                                WritableUtils.writeVInt(fonlCN[u], f);

                                if (partition[v] == p) {
                                    if (internalFU[v] == null)
                                        internalFU[v] = new DataOutputBuffer();
                                    WritableUtils.writeVInt(internalFU[v], vn);
                                    WritableUtils.writeVInt(internalFU[v], u);
                                } else {
                                    if (externalFU[pIndex[v]] == null)
                                        externalFU[pIndex[v]] = new DataOutputBuffer();
                                    WritableUtils.writeVInt(externalFU[pIndex[v]], v);
                                    WritableUtils.writeVInt(externalFU[pIndex[v]], vn);
                                    WritableUtils.writeVInt(externalFU[pIndex[v]], u);
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

                    fonlVS[u] = new DataOutputBuffer(jIndex * 2);

                    WritableUtils.writeVInt(fonlVS[u], jIndex);
                    for (int j = 0; j < jIndex; j++) {
                        WritableUtils.writeVInt(fonlVS[u], lens[j]);
                        WritableUtils.writeVInt(fonlVS[u], vIndexes[j]);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        })).get();


        long tTC = System.currentTimeMillis();
        System.out.println("tc after fonl: " + (tTC - t3) + " ms");
        System.out.println("tc duration: " + (tTC - tStart) + " ms");

        final AtomicInteger updateCount = new AtomicInteger(0);
        // Add external to internal
        for (int i = 0; i < externalFUs.length; i++) {
            final DataOutputBuffer[] externalFU = externalFUs[i];
            final int index = i;
            batchSelector = new AtomicInteger(0);
            final int pSize = externalFU.length;
            IntStream.range(0, threads).forEach(x -> {
                DataInputBuffer in = new DataInputBuffer();

                while (true) {
                    int start = batchSelector.getAndAdd(BATCH_SIZE);
                    if (start >= pSize)
                        break;
                    int len = Math.min(pSize, start + BATCH_SIZE);
                    for (int j = start; j < len; j++) {
                        if (externalFU[j] == null)
                            continue;

                        try {
                            in.reset(externalFU[j].getData(), externalFU[j].getLength());
                            while (in.getPosition() != externalFU[j].getLength()) {
                                int v = WritableUtils.readVInt(in);
                                int vn = WritableUtils.readVInt(in);
                                int u = WritableUtils.readVInt(in);
                                if (internalFU[v] == null)
                                    internalFU[v] = new DataOutputBuffer();
                                WritableUtils.writeVInt(internalFU[v], vn);
                                WritableUtils.writeVInt(internalFU[v], u);
                                updateCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        int sumNeighborSize = 0;
        int maxNeighborSize = 0;
        int countNS = 0;

        for (int i = 0; i < internalFU.length; i++) {
            DataOutputBuffer internal = internalFU[i];
            if (internal == null)
                continue;
            int neighborSize = internal.getLength() / 5;
            if (neighborSize > maxNeighborSize)
                maxNeighborSize = neighborSize;
            sumNeighborSize += neighborSize;
            countNS ++;
        }

        double avgNeighborSize = sumNeighborSize / (float) countNS;
        double sumVarNS = 0.0;
        for (int i = 0; i < internalFU.length; i++) {
            DataOutputBuffer internal = internalFU[i];
            if (internal == null)
                continue;
            int neighborSize = internal.getLength() / 5;
            sumVarNS += Math.sqrt(Math.pow(neighborSize - avgNeighborSize, 2));
        }
        double varNS = sumVarNS / countNS;
        DecimalFormat df = new DecimalFormat("#.00");
        System.out.println("update count: " + updateCount + ", maxNS = " + maxNeighborSize + ", " +
            "avgNS = " + df.format(avgNeighborSize) + ", varNS = " + df.format(varNS));

        long tExternal = System.currentTimeMillis();
        System.out.println("Update internals in " + (tExternal - tTC) + " ms");

        int tcCount = 0;
        DataInputBuffer in1 = new DataInputBuffer();
        DataInputBuffer in2 = new DataInputBuffer();

        for (int u = 0; u < fonlCN.length; u++) {
            if (fonlCN[u] == null)
                continue;

            in1.reset(fonlCN[u].getData(), fonlCN[u].getLength());
            in2.reset(fonlVS[u].getData(), fonlVS[u].getLength());
            while (true) {
                if (in2.getPosition() >= fonlVS[u].getLength())
                    break;

                int size = WritableUtils.readVInt(in2);
                for (int i = 0; i < size; i++) {
                    int len = WritableUtils.readVInt(in2);
                    int vIndex = WritableUtils.readVInt(in2);
//                    int v = fonls[u][vIndex];
//                    for (int j = 0; j < len; j++) {
//                        int uwIndex = WritableUtils.readVInt(in1);
//                        vertexEdges[u].add(uwIndex, v);
//
//                        int vwIndex = WritableUtils.readVInt(in1);
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
