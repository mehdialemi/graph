package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.VertexCompare;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        final int[] partition = new int[vCount];

        // Initialize neighbors arrayList
        for (int i = 0; i < vCount; i++) {
            fonls[i] = new int[Math.min(d[i], vCount - d[i])];
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
        int cp = 0;
        int batchSize = BATCH_SIZE;
        int rem = fonls.length - threads * batchSize * 2;
        for (int u = 0; u < fonls.length; u++) {
            int p = cp % threads;
            pSizes[p]++;
            partition[u] = p;
            if ((u + 1) % batchSize == 0)
                cp++;
            if (p == 0 && u > rem) {
                batchSize = Math.max(batchSize / 2, 10);
                rem = fonls.length - threads * batchSize * 2;
            }
        }
//        ProbabilityRandom pRandom = new ProbabilityRandom(threads, vCount);
//        for (int i = 0; i < fonls.vCount; i++) {
//            if (fl[i] == 0 || partition[i] != -1)
//                continue;
//            int p = pRandom.getNextRandom();
//
//            pRandom.increment(p);
//            partition[i] = p;
//            pIndex[i] = pSizes[p]++;
//            for (int v : fonls[i]) {
//                if (fl[v] == 0 || partition[v] != 0)
//                    continue;
//                partition[v] = p;
//                pIndex[v] = pSizes[p]++;
//                pRandom.increment(p);
//            }
//        }

        for (int i = 0; i < pSizes.length; i++) {
            System.out.println("Partition " + i + ", Size: " + pSizes[i]);
        }

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

        final DataOutputBuffer[][] externalFUs = new DataOutputBuffer[vCount][];  // external fonl update
        final byte[][] internalFUs = new byte[vCount][];

        int maxPartitionSize = 0;
        for (int pSize : pSizes) {
            if (pSize > maxPartitionSize)
                maxPartitionSize = pSize;
        }

        for (int i = 0; i < externalFUs.length; i++) {
            externalFUs[i] = new DataOutputBuffer[threads];
        }

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(p -> {
            DataOutputBuffer out = new DataOutputBuffer(maxFSize * (maxFSize - 1) * 2 * (maxFSize / 128 + 1));
            try {
                int len = fonls.length;
                for (int u = 0; u < len; u++) {
                    if (fl[u] < 2 || partition[u] != p) {
                        continue;
                    }

//                    DataOutputBuffer[] externalFU = externalFUs[p];
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

                                if (externalFUs[v][p] == null)
                                    externalFUs[v][p] = new DataOutputBuffer((d[v] - fl[v]) * (4 + fl[v] / 128 + 1));

                                WritableUtils.writeVInt(externalFUs[v][p], vn); // second vertex index
                                WritableUtils.writeVInt(externalFUs[v][p], u); // thir vertex num
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
        Int2ObjectOpenHashMap<List<byte[]>>[] maps = new Int2ObjectOpenHashMap[vCount];
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
                    Int2ObjectOpenHashMap<List<byte[]>> map = new Int2ObjectOpenHashMap<>();
                    if (internalFUs[u] != null) {
                        in.reset(internalFUs[u], internalFUs[u].length);
                        while (true) {
                            if (in.getPosition() >= internalFUs[u].length)
                                break;
                            try {
                                int index1 = WritableUtils.readVInt(in);
                                int p1 = in.getPosition();
                                int index2 = WritableUtils.readVInt(in);
                                int p2 = in.getPosition();
                                List<byte[]> list = map.get(index1);
                                if (list == null) {
                                    list = new ArrayList<>();
                                    map.put(index1, list);
                                }
                                byte[] bytes = new byte[1 + p2 - p1];
                                bytes[0] = 0;
                                System.arraycopy(in.getData(), p1, bytes, 1, p2 - p1);
                                list.add(bytes);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    for(int p = 0 ; p < threads; p ++) {
                        if (externalFUs[u][p] == null)
                            continue;
                        in.reset(externalFUs[u][p].getData(), externalFUs[u][p].getLength());
                        while (true) {
                            if (in.getPosition() >= externalFUs[u][p].getLength())
                                break;
                            try {
                                int index1 = WritableUtils.readVInt(in);
                                int p1 = in.getPosition();
                                int index2 = WritableUtils.readVInt(in);
                                int p2 = in.getPosition();
                                List<byte[]> list = map.get(index1);
                                if (list == null) {
                                    list = new ArrayList<>();
                                    map.put(index1, list);
                                }
                                byte[] bytes = new byte[1 + p2 - p1];
                                bytes[0] = 1;
                                System.arraycopy(in.getData(), p1, bytes, 1, p2 - p1);
                                list.add(bytes);
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
