package ir.ac.sbu.graph.others.ktruss.parallel;

import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.utils.VertexCompare;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 *
 */
public class ParallelKTruss4 extends ParallelKTrussBase {

    public static final int INT_SIZE = 4;
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
        final int maxFSize = forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().map(i -> {
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
            return maxFonlSize;
        })).get().reduce((a, b) -> Integer.max(a, b)).getAsInt();

        long t3 = System.currentTimeMillis();
        System.out.println("sort fonl in " + (t3 - t2) + " ms");

        long tsCounts = System.currentTimeMillis();
        AtomicInteger[][] veSups = new AtomicInteger[vCount][];
//        int threadCount = Math.max(Math.min(threads, 2), threads / 2);
//        System.out.println("using " + threadCount + " threads to construct veSups");

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(partition -> {
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
                }
            }
        })).get();
        long tCounts = System.currentTimeMillis();
        System.out.println("construct veSups in " + (tCounts - tsCounts) + " ms");

        byte[][] seconds = new byte[vCount][];
        byte[][][] thirds = new byte[vCount][][];
        int[] veCount = new int[vCount];

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(thread -> {
            int[] vIndexes = new int[maxFSize];
            int[] lens = new int[maxFSize];
            DataOutputBuffer outInternal = new DataOutputBuffer(maxFSize);
            DataOutputBuffer outLocal = new DataOutputBuffer(maxFSize);
            DataOutputBuffer outTmp = new DataOutputBuffer(maxFSize);
            DataInputBuffer in = new DataInputBuffer();
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount)
                    break;
                int end = Math.min(vCount, BATCH_SIZE + start);

                for (int u = start; u < end; u++) {
                    if (flen[u] < 2)
                        continue;

                    int commonSize = 0;
                    outLocal.reset();

                    int[] neighborsU = neighbors[u];
                    // Find triangle by checking connectivity of neighbors
                    for (int vIndex = 0; vIndex < flen[u]; vIndex++) {
                        int v = neighborsU[vIndex];
                        int[] vNeighbors = neighbors[v];

                        int intersection = 0;
                        // intersection on u neighbors and vertex neighbors
                        int uwIndex = vIndex + 1, vwIndex = 0;

                        while (uwIndex < flen[u] && vwIndex < flen[v]) {
                            if (neighborsU[uwIndex] == vNeighbors[vwIndex]) {
                                try {
                                    int num = veSups[u][uwIndex].getAndIncrement();
                                    if (num == 0)
                                        veCount[u] ++;
                                    num = veSups[v][vwIndex].getAndIncrement();
                                    if (num == 0)
                                        veCount[v] ++;
                                    WritableUtils.writeVInt(outLocal, uwIndex);
                                    WritableUtils.writeVInt(outLocal, vwIndex);
                                } catch (Exception e) {
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

                        vIndexes[commonSize] = vIndex;
                        lens[commonSize++] = intersection;
                    }

                    if (commonSize == 0)
                        continue;

                    outInternal.reset();
                    in.reset(outLocal.getData(), outLocal.getLength());
                    try {
                        thirds[u] = new byte[commonSize][];
                        for (int j = 0; j < commonSize; j++) {
                            WritableUtils.writeVInt(outInternal, vIndexes[j]);
                            WritableUtils.writeVInt(outInternal, lens[j]);
                            int num = veSups[u][vIndexes[j]].getAndAdd(lens[j]);
                            outTmp.reset();
                            if (num == 0)
                                veCount[u] ++;
                            for (int k = 0; k < lens[j]; k++) {
                                WritableUtils.writeVInt(outTmp, WritableUtils.readVInt(in));
                                WritableUtils.writeVInt(outTmp, WritableUtils.readVInt(in));
                            }
                            thirds[u][j] = new byte[outTmp.getLength()];
                            System.arraycopy(outTmp.getData(), 0, thirds[u][j], 0, outTmp.getLength());
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    seconds[u] = new byte[outInternal.getLength()];
                    System.arraycopy(outInternal.getData(), 0, seconds[u], 0, outInternal.getLength());
                }
            }
        })).get();

        long tTC = System.currentTimeMillis();
        System.out.println("vTc duration: " + (tTC - tStart) + " ms");

        byte[][] fonlSeconds = new byte[vCount][];
        DataOutputBuffer[][] fonlThirds = new DataOutputBuffer[vCount][];
        int[][] veSupSortedIndex = new int[vCount][];

        long tsorted1 = System.currentTimeMillis();
        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(thread -> {
            DataOutputBuffer out = new DataOutputBuffer(maxFSize * 8);
            int[] tmp = new int[maxFSize];
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount) {
                    break;
                }
                int end = Math.min(vCount, BATCH_SIZE + start);
                for (int u = start; u < end; u++) {
                    if(veCount[u] == 0) {
                        continue;
                    }
                    veSupSortedIndex[u] = new int[veCount[u]];
                    int digitSize = flen[u] / 127;
//                    fonlSeconds[u] = new byte[digitSize * veCount[u]];
                    fonlThirds[u] = new DataOutputBuffer[flen[u]];
                    int idx = 0;
                    for(int i = 0 ; i < flen[u]; i++) {
                        int sup = veSups[u][i].get();
                        if (sup == 0) {
                            continue;
                        }

                        fonlThirds[u][i] = new DataOutputBuffer((3 * digitSize + 4) * sup * 4);
                        tmp[idx ++] = i;
                    }

                    out.reset();
                    for(int i = 0 ; i < veCount[u]; i ++) {
                        int selected = i;
                        int min = veSups[u][tmp[i]].get();
                        for(int j = i + 1 ; j < veCount[u]; j ++) {
                            if (min < veSups[u][tmp[j]].get()) {
                                min = veSups[u][tmp[j]].get();
                                selected = j;
                            }
                        }
                        if (selected != i) {
                            int temp = tmp[i];
                            tmp[i] = tmp[selected];
                            tmp[selected] = temp;
                        }

                        veSupSortedIndex[u][i] = tmp[i];
                        try {
                            WritableUtils.writeVInt(out, tmp[i]);
                            WritableUtils.writeVInt(out, veSups[u][tmp[i]].get());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    fonlSeconds[u] = new byte[out.getLength()];
                    System.arraycopy(out.getData(), 0, fonlSeconds[u], 0, out.getLength());
                }
            }
        })).get();

        long tsorted2 = System.currentTimeMillis();
        System.out.println("initialize and getOrCreateLFonl sort index in " + (tsorted2 - tsorted1) + " ms");
//        int[] partitions = PartitioningUtils.createPartitions(vCount, threads, BATCH_SIZE);
        long tupdate1 = System.currentTimeMillis();
        DataInputBuffer in1 = new DataInputBuffer();
        DataInputBuffer in2 = new DataInputBuffer();
        int[] vIndexes = new int[maxFSize];
        int[] lens = new int[maxFSize];

        for (int u = 0; u < vCount; u++) {
            if (seconds[u] == null)
                continue;

            int index = 0;
            in1.reset(seconds[u], seconds[u].length);
            while (in1.getPosition() < seconds[u].length) {
                vIndexes[index] = WritableUtils.readVInt(in1);
                lens[index] = WritableUtils.readVInt(in1);
                index++;
            }

            for(int i = 0 ; i < index; i ++) {
                int v = neighbors[u][vIndexes[i]];
                int len = lens[i];

                in2.reset(thirds[u][i], thirds[u][i].length);
                for(int j = 0 ; j < len; j ++) {
                    int uwIndex = WritableUtils.readVInt(in2);

                    WritableUtils.writeVInt(fonlThirds[u][vIndexes[i]], uwIndex);

                    int vwIndex = WritableUtils.readVInt(in2);
                    if (vwIndex >= fonlThirds[v].length) {
                        System.out.println("For vertex: " + v + ", vwIndex: " + vwIndex +
                                ", len: " + fonlThirds[v].length +
                                ", neighbor: " + u + " len: " + len);
                    }
                    WritableUtils.writeVInt(fonlThirds[v][vwIndex], -u);
                    WritableUtils.writeVInt(fonlThirds[v][vwIndex], vwIndex);
                    WritableUtils.writeVInt(fonlThirds[v][vwIndex], uwIndex);
                }
            }
        }

        long tupdate2 = System.currentTimeMillis();
        System.out.println("complete fonl in " + (tupdate2 - tupdate1) + " ms");

//        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(partition -> {
//            DataInputBuffer in = new DataInputBuffer();
//            DataInputBuffer in = new DataInputBuffer();
//            ResettableDataOutputBuffer out = new ResettableDataOutputBuffer();
//            ResettableDataOutputBuffer out2 = new ResettableDataOutputBuffer();
//            int[] vIndexes = new int[maxFSize];
//            int[] lens = new int[maxFSize];
//            byte[][] localThirds = new byte[maxFSize][];
//            for (int u = 0; u < vCount; u++) {
//                int[] uNeighbors = neighbors[u];
//                if (uNeighbors[0] < 2)
//                    continue;
//
//                int lastIndex = 0;
//                boolean local = partitions[u] == partition;
//                in.reset(seconds[u], seconds[u].length);
//                for(int i = 0; i < lcCount[u]; i ++) {
//                    try {
//                        vIndexes[lastIndex] = WritableUtils.readVInt(in);
//                        int vertex = uNeighbors[vIndexes[lastIndex]];
//                        if (!local && partitions[vertex] != partition)
//                            continue;
//                        lens[lastIndex] = WritableUtils.readVInt(in);
//                        localThirds[lastIndex] = thirds[u][lastIndex];
//                        lastIndex ++;
//                    } catch (IOException e) {
//                    }
//                }
//
//                if (lastIndex == 0)
//                    continue;
//
//                int c = 0;
//                for (int i = 0 ; i < veCount[u] ; i ++) {
//                    int index = -1;
//                    for (int j = 0 ; j < lastIndex; j ++) {
//                        if (veSupSortedIndex[u][i] == vIndexes[j]) {
//                            index = j;
//                        }
//                    }
//                    if (index == -1)
//                        continue;
//
//                    if (local)
//                        out.reset(fonlThirds[u][i]);
//                    int vertex = uNeighbors[vIndexes[index]];
//                    boolean update = partitions[vertex] == partition;
//                    try {
//                        in.reset(localThirds[index], localThirds[index].length);
//                        if (local)
//                            WritableUtils.writeVInt(out, lens[index]);
//                        for(int j = 0 ; j < lens[index]; j ++) {
//                            int uwIndex = WritableUtils.readVInt(in);
//                            int vwIndex = WritableUtils.readVInt(in);
//                            if (update) {
//                                out2.reset(fonlThirds[vertex][vwIndex]);
//                                WritableUtils.writeVInt(out2, -1);
//                                WritableUtils.writeVInt(out2, u);
//                                WritableUtils.writeVInt(out2, vwIndex);
//                                WritableUtils.writeVInt(out2, uwIndex);
//                            }
//                            if (local)
//                                WritableUtils.writeVInt(out, uwIndex);
//                        }
//                    } catch (IOException e) {}
//                }
//            }
//        })).get();

//        int sum = 0;
//        for(int i = 0 ; i < vCount; i ++) {
//            for (int j = 1 ; j < support[i].length; j ++)
//                sum += support[i][j];
//        }
//        System.out.println("vTc: " + sum / 3);
    }

    private int[] findPartition(int threads, int[][] fonls, int[] fl, byte[][] fonlNeighborL1, int[] pSizes) throws IOException {
        int[] partitions = new int[fonls.length];
        BitSet bitSet = new BitSet(fonls.length);
        for (int u = 0; u < partitions.length; u++) {
            partitions[u] = -1;
        }

        // getOrCreateLFonl dominate set
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

            // find target findPartition => the smallest support findPartition with maximum maxScore
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
