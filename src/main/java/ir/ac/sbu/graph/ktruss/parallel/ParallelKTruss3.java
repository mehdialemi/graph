package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.utils.PartitioningUtils;
import ir.ac.sbu.graph.utils.VertexCompare;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 *
 */
public class ParallelKTruss3 extends ParallelKTrussBase {

    private final ForkJoinPool forkJoinPool;

    public ParallelKTruss3(Edge[] edges, int minSup, int threads) {
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
        final int vCount = max + 1;
        int[] d = new int[vCount];  // vertex degree
        for (Edge e : edges) {
            d[e.v1]++;
            d[e.v2]++;
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Find degrees in " + (t2 - tStart) + " ms");

        // Construct fonls and fonlCN
        final int[][] fonls = new int[vCount][];
        final int[] fl = new int[vCount];  // Fonl Length

        // Initialize neighbors arrayList
        for (int i = 0; i < vCount; i++) {
            fonls[i] = new int[Math.min(d[i], vCount - d[i])];
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

        int[] partitions = PartitioningUtils.createPartitions(vCount, threads, BATCH_SIZE);

//        ProbabilityRandom pRandom = new ProbabilityRandom(threads, vCount);
//        for (int i = 0; i < fonls.vCount; i++) {
//            if (fl[i] == 0 || partitions[i] != -1)
//                continue;
//            int p = pRandom.getNextRandom();
//
//            pRandom.increment(p);
//            partitions[i] = p;
//            pIndex[i] = pSizes[p]++;
//            for (int v : fonls[i]) {
//                if (fl[v] == 0 || partitions[v] != 0)
//                    continue;
//                partitions[v] = p;
//                pIndex[v] = pSizes[p]++;
//                pRandom.increment(p);
//            }
//        }

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

        final DataOutputBuffer[][] externalFUs = new DataOutputBuffer[threads][];  // external fonl update
        final byte[][] internalFUs = new byte[vCount][];

        for (int i = 0; i < externalFUs.length; i++) {
            externalFUs[i] = new DataOutputBuffer[vCount];
        }

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(p -> {
            DataOutputBuffer[] externalFU = externalFUs[p];
            DataOutputBuffer out = new DataOutputBuffer(maxFSize * (maxFSize - 1) * 2 * (maxFSize / 128 + 1));
            try {
                int len = fonls.length;
                for (int u = 0; u < len; u++) {
                    if (fl[u] < 2 || partitions[u] != p)
                        continue;

                    out.reset();

                    // Find triangle by checking connectivity of neighbors
                    for (int j = 0; j < fl[u]; j++) {
                        int[] fonl = fonls[u];
                        int v = fonl[j];
                        int[] vNeighbors = fonls[v];

                        // intersection on u neighbors and v neighbors
                        int f = j + 1, vn = 0;
                        while (f < fl[u] && vn < fl[v]) {
                            if (fonl[f] == vNeighbors[vn]) {
                                WritableUtils.writeVInt(out, j); // second vertex index from neighbor
                                WritableUtils.writeVInt(out, f); // third vertex index from neighbor

                                if (externalFU[v] == null)
                                    externalFU[v] = new DataOutputBuffer((d[v] - fl[v]) * (4 + fl[v] / 128 + 1));

                                WritableUtils.writeVInt(externalFU[v], vn); // second vertex index
                                WritableUtils.writeVInt(externalFU[v], u); // thir vertex num
                                f++;
                                vn++;
                            } else if (vertexCompare.compare(fonl[f], vNeighbors[vn]) == -1)
                                f++;
                            else
                                vn++;
                        }
                    }

                    if (out.getLength() == 0)
                        continue;

                    byte[] bytes = new byte[out.getLength()];
                    System.arraycopy(out.getData(), 0, bytes, 0, bytes.length);
                    internalFUs[u] = bytes;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        })).get();

        long tTC = System.currentTimeMillis();
        System.out.println("tc after fonl: " + (tTC - t3) + " ms");
        System.out.println("tc duration: " + (tTC - tStart) + " ms");

        // aggregate internal and external fonl updates
        Int2ObjectOpenHashMap<IntList>[] maps = new Int2ObjectOpenHashMap[vCount];
        batchSelector = new AtomicInteger(0);
        IntStream.range(0, threads).parallel().forEach(i -> {
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount)
                    break;
                int end = Math.min(vCount, start + BATCH_SIZE);
                DataInputBuffer in = new DataInputBuffer();
                for(int u = start ; u < end ;  u ++) {
                    if (fl[u] == 0) {
                        continue;
                    }
                    Int2ObjectOpenHashMap<IntList> map = new Int2ObjectOpenHashMap<>();
                    if (internalFUs[u] != null) {
                        in.reset(internalFUs[u], internalFUs[u].length);
                        while (true) {
                            if (in.getPosition() >= internalFUs[u].length)
                                break;
                            try {
                                int indexN1 = WritableUtils.readVInt(in);
                                int indexN2 = WritableUtils.readVInt(in);
                                IntList list = map.get(indexN1);
                                if (list == null) {
//                                    list = new IntArrayList();
//                                    map.put(indexN1, list);
                                }
//                                list.add(fonls[u][indexN2]);

                                list = map.get(indexN2);
                                if (list == null) {
//                                    list = new IntArrayList();
//                                    map.put(indexN2, list);
                                }
//                                list.add(fonls[u][indexN2]);

                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    for(int j = 0 ; j < threads; j ++) {
                        if (externalFUs[j][u] == null)
                            continue;
                        in.reset(externalFUs[j][u].getData(), externalFUs[j][u].getLength());
                        while (true) {
                            if (in.getPosition() >= externalFUs[j][u].getLength())
                                break;
                            try {
                                int indexN = WritableUtils.readVInt(in);
                                int w = WritableUtils.readVInt(in);
                                IntList list = map.get(indexN);
                                if (list == null) {
//                                    list = new IntArrayList();
//                                    map.put(indexN, list);
                                }
//                                list.add(w);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    if (map.isEmpty())
                        continue;

                    maps[u] = map;
                }
            }
        });

        long tAgg = System.currentTimeMillis();
        System.out.println("Aggregate time " + (tAgg - tTC) + " ms");

        int tcCount = 0;
        DataInputBuffer in1 = new DataInputBuffer();

        for (int u = 0; u < internalFUs.length; u++) {
            if (internalFUs[u] == null)
                continue;

            in1.reset(internalFUs[u], internalFUs[u].length);
            while (true) {
                if (in1.getPosition() >= internalFUs[u].length)
                    break;

                WritableUtils.readVInt(in1);
                WritableUtils.readVInt(in1);
                tcCount++;
            }
        }

        long tFinal = System.currentTimeMillis();
        System.out.println("fill eSup in " + (tFinal - tTC) + " ms");
        System.out.println("tcCount: " + tcCount);

    }
}
