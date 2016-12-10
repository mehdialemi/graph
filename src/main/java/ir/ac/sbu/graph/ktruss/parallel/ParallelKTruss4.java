package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.ResettableDataOutputBuffer;
import ir.ac.sbu.graph.VertexCompare;
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

        long tsCounts = System.currentTimeMillis();
        AtomicInteger[][] veSups = new AtomicInteger[vCount][];
        int threadCount = Math.max(Math.min(threads, 2), threads / 2);
        System.out.println("using " + threadCount + " threads to construct veSups");

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
                    veSups[u] = new AtomicInteger[neighbors[u][0]];
                    for (int i = 0; i < neighbors[u][0]; i++)
                        veSups[u][i] = new AtomicInteger(0);
                }
            }
        })).get();
        long tCounts = System.currentTimeMillis();
        System.out.println("construct veSups in " + (tCounts - tsCounts) + " ms");

        byte[][] seconds = new byte[vCount][];
        byte[][][] thirds = new byte[vCount][][];
        int[] veCount = new int[vCount];

//        int[] partitions = PartitioningUtils.createPartitions(vCount, threads, BATCH_SIZE);

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
                    if (neighbors[u][0] < 2)
                        continue;

                    int commonSize = 0;
                    outLocal.reset();

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
                                    int num = veSups[u][uwIndex - 1].getAndIncrement();
                                    if (num == 0)
                                        veCount[u] ++;
                                    num = veSups[v][vwIndex - 1].getAndIncrement();
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
                            int num = veSups[u][vIndexes[j] - 1].getAndAdd(lens[j]);
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
        System.out.println("tc duration: " + (tTC - tStart) + " ms");

        DataInputBuffer in1 = new DataInputBuffer();
        DataInputBuffer in2 = new DataInputBuffer();

        byte[][] fonlSeconds = new byte[vCount][];
        byte[][][] fonlThirds = new byte[vCount][][];
        int[][] veSupSortedIndex = new int[vCount][];
        int[] tmp = new int[maxFSize];

        long tsorted1 = System.currentTimeMillis();
        for(int u = 0 ; u < vCount; u ++) {
            if(veCount[u] == 0)
                continue;
            veSupSortedIndex[u] = new int[veCount[u]];
            int digitSize = neighbors[u][0] / 127;
            fonlSeconds[u] = new byte[digitSize * veCount[u]];
            fonlThirds[u] = new byte[neighbors[u][0]][];
            int idx = 0;
            for(int i = 0 ; i < neighbors[u][0]; i++) {
                int sup = veSups[u][i].get();
                if (sup == 0)
                    continue;

                fonlThirds[u][i] = new byte[(3 * digitSize + 4) * sup];
                tmp[idx ++] = i;
            }

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
            }
        }

        long tsorted2 = System.currentTimeMillis();
        System.out.println("initialize and create sort index in " + (tsorted2 - tsorted1) + " ms");



//        for(int u = 0 ; u < vCount; u ++) {
//            if(veCount[u] == 0)
//                continue;


//            int index = 0;
//            in1.reset(seconds[u], seconds[u].length);
//            in2.reset(thirds[u], thirds[u].length);
//            while (in1.getPosition() < seconds[u].length) {
//                vIndexes[index] = WritableUtils.readVInt(in1);
//                lens[index] = WritableUtils.readVInt(in1);
//                out.reset();
//                for (int i = 0; i < lens[index]; i++) {
//                    WritableUtils.writeVInt(out, WritableUtils.readVInt(in2));
//                    WritableUtils.writeVInt(out, WritableUtils.readVInt(in2));
//                }
//                localThirds[index] = new byte[out.getLength()];
//                System.arraycopy(out.getData(), 0, localThirds[index], 0, out.getLength());
//                index++;
//            }
//        }

//        batchSelector = new AtomicInteger(0);
//        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(thread -> {
//            BitSet bitSet = new BitSet(maxFSize);
//            DataOutputBuffer out = new DataOutputBuffer(maxFSize);
//
//            while (true) {
//                int start = batchSelector.getAndAdd(BATCH_SIZE);
//                if (start >= vCount)
//                    break;
//                int end = Math.min(vCount, BATCH_SIZE + start);
//
//                for (int u = start; u < end; u++) {
//                    if (neighbors[u][0] < 2 || veCount[u] == 0)
//                        continue;
//
//                    bitSet.clear();
//
//                    veSupSortedIndex[u] = new int[veCount[u]];
//
//                    int index = 0;
//                    int minIdx = 0;
//                    int min = Integer.MAX_VALUE;
//                    int first = 0;
//                    for (int i = 0 ; i < neighbors[u][0]; i ++) {
//                        if (veSups[u][i].get() == 0 || bitSet.get(i))
//                            continue;
//                        first = i;
//                        minIdx = i;
//                        min = veSups[u][i].get();
//                        for (int j = 0; j < neighbors[u][0]; j ++) {
//                            if (veSups[u][i].get() == 0 || bitSet.get(j) || j == first)
//                                continue;
//                            if (veSups[u][j].get() < min) {
//                                minIdx = j;
//                                min = veSups[u][j].get();
//                            }
//                        }
//                        bitSet.set(minIdx);
//                        veSupSortedIndex[u][index ++] = minIdx;
//                    }
//
//                    out.reset();
//                    for (int i = 0 ; i < veCount[u] ; i ++) {
//                        try {
//                            index = veSupSortedIndex[u][i];
//                            WritableUtils.writeVInt(out, veSups[u][index].get()); // write count
//                            WritableUtils.writeVInt(out, index);  // write index of vertex (v)
//                        } catch (IOException e) {
//                        }
//                    }
//
//                    fonlSeconds[u] = new byte[out.getLength()];
//                    System.arraycopy(out.getData(), 0, fonlSeconds[u], 0, out.getLength());
//
//                    int digitSize = neighbors[u][0] / 127 + 1;
//                    fonlThirds[u] = new byte[neighbors[u][0]][];
//                    for (int i = 0 ; i < veCount[u]; i ++) {
//                        index = veSupSortedIndex[u][i];
//                        int size = veSups[u][index].get();
//                        fonlThirds[u][index] =
//                            new byte[INT_SIZE + digitSize * lcCount[u] + size * (INT_SIZE + 1 + digitSize + size)];
//                        //         hold count of DataOutputBuffer + ...
//                    }
//                }
//            }
//        })).get();

        DataOutputBuffer out = new DataOutputBuffer(maxFSize * maxFSize);
        long tupdate1 = System.currentTimeMillis();
        ResettableDataOutputBuffer out1 = new ResettableDataOutputBuffer();
        ResettableDataOutputBuffer out2 = new ResettableDataOutputBuffer();
        int[] vIndexes = new int[maxFSize];
        int[] lens = new int[maxFSize];
        byte[][] localThirds = new byte[maxFSize][];
        for (int u = 0; u < vCount; u++) {
            if (veCount[u] == 0)
                continue;

            int index = 0;
            in1.reset(seconds[u], seconds[u].length);
            while (in1.getPosition() < seconds[u].length) {
                vIndexes[index] = WritableUtils.readVInt(in1);
                lens[index] = WritableUtils.readVInt(in1);
                localThirds[index] = thirds[u][index];
                index++;
            }

            for (int i = 0; i < veCount[u]; i++) {
                int idx = -1;
                for (int j = 0; j < index; j++) {
                    if (veSupSortedIndex[u][i] == vIndexes[j]) {
                        index = j;
                        break;
                    }
                }

                if (idx == -1)
                    continue;

                int vIndex = vIndexes[index];
                out1.reset(fonlThirds[u][vIndex]);
                int v = neighbors[u][vIndex];
                in2.reset(localThirds[index], localThirds[index].length);
                WritableUtils.writeVInt(out1, lens[index]);
                for (int j = 0; j < lens[index]; j++) {
                    int uwIndex = WritableUtils.readVInt(in2);
                    WritableUtils.writeVInt(out1, uwIndex);

                    int vwIndex = WritableUtils.readVInt(in2);
                    out2.reset(fonlThirds[v][vwIndex]);
                    WritableUtils.writeVInt(out2, -1);
                    WritableUtils.writeVInt(out2, u);
                    WritableUtils.writeVInt(out2, vwIndex);
                    WritableUtils.writeVInt(out2, uwIndex);
                }
            }
        }
        long tupdate2 = System.currentTimeMillis();
        System.out.println("complete fonl in " + (tupdate2 - tupdate1) + " ms");

//        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(partition -> {
//            DataInputBuffer in = new DataInputBuffer();
//            DataInputBuffer in2 = new DataInputBuffer();
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
//                        int v = uNeighbors[vIndexes[lastIndex]];
//                        if (!local && partitions[v] != partition)
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
//                    int v = uNeighbors[vIndexes[index]];
//                    boolean update = partitions[v] == partition;
//                    try {
//                        in2.reset(localThirds[index], localThirds[index].length);
//                        if (local)
//                            WritableUtils.writeVInt(out, lens[index]);
//                        for(int j = 0 ; j < lens[index]; j ++) {
//                            int uwIndex = WritableUtils.readVInt(in2);
//                            int vwIndex = WritableUtils.readVInt(in2);
//                            if (update) {
//                                out2.reset(fonlThirds[v][vwIndex]);
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
//            for (int j = 1 ; j < sup[i].length; j ++)
//                sum += sup[i][j];
//        }
//        System.out.println("tc: " + sum / 3);
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
