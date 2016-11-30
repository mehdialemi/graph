package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.VertexCompare;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

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
        long t1 = System.currentTimeMillis();
        int min = Math.min(edges[0].v1, edges[0].v2);
        int max = Math.max(edges[0].v1, edges[0].v2);

        for (int i = 1 ; i < edges.length ; i ++) {
            if (edges[i].v1 > edges[i].v2) {
                if (edges[i].v1 > max)
                    max = edges[i].v1;
                if (edges[i].v2 < min)
                    min = edges[i].v2;
            } else {
                if (edges[i].v2 > max)
                    max = edges[i].v2;
                if (edges[i].v1 < min)
                    min = edges[i].v1;
            }
        }

        // Construct degree array such that vertexId is the index of the array.
        final int length = max + 1;
        int[] d = new int[length];  // vertex degree
        for (Edge e : edges) {
            d[e.v1]++;
            d[e.v2]++;
        }

        // Construct fonls and fonlCN
        final int[][] fonls = new int[length][];
        final int[] fl = new int[length];  // Fonl Length

        // Initialize neighbors array
        for(int i = 0; i < length; i ++)
            fonls[i] = new int[Math.min(d[i], length - d[i])];

        // Fill neighbors array
        for(Edge e : edges) {
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

        int maxFonlSize = 0;
        final VertexCompare vertexCompare = new VertexCompare(d);
        for (int u = 0 ; u < fonls.length; u ++) {
            if (fl[u] < 2)
                continue;
            vertexCompare.quickSort(fonls[u], 0, fl[u] - 1);
            if (maxFonlSize  < fl[u])
                maxFonlSize  = fl[u];
        }
        final int maxFSize = maxFonlSize;


        final byte[][] fonlCN = new byte[length][];  // fonl common neighbors
        final byte[][] fonlVS = new byte[length][];  // fonl vertex size

        AtomicInteger batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(index -> {
            int[] lens = new int[maxFSize];
            int[] vIndexes = new int[maxFSize];
            DataOutputBuffer out1 = new DataOutputBuffer();
            DataOutputBuffer out2 = new DataOutputBuffer();
            try {
                while (true) {
                    int start = batchSelector.getAndAdd(BATCH_SIZE);
                    if (start >= length)
                        break;
                    int end = Math.min(length, BATCH_SIZE + start);

                    for (int u = start; u < end; u++) {
                        if (fl[u] < 2)
                            continue;

                        out1.reset();
                        out2.reset();
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
                                    WritableUtils.writeVInt(out1, f);
                                    WritableUtils.writeVInt(out1, vn);
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

                        WritableUtils.writeVInt(out2, jIndex);
                        for (int j = 0; j < jIndex; j++) {
                            WritableUtils.writeVInt(out2, lens[j]);
                            WritableUtils.writeVInt(out2, vIndexes[j]);
                        }

                        byte[] bytes = new byte[out1.getLength()]; // encoded triangle
                        System.arraycopy(out1.getData(), 0, bytes, 0, out1.getLength());
                        fonlCN[u] = bytes;

                        bytes = new byte[out2.getLength()];
                        System.arraycopy(out2.getData(), 0, bytes, 0, out2.getLength());
                        fonlVS[u] = bytes;
                    }
                }
            } catch (Exception e) { e.printStackTrace(); }
        })).get();

        long t2 = System.currentTimeMillis();
        System.out.println("tc duration: " + (t2 - t1) + " ms");

        int tcCount = 0;
        DataInputBuffer in1 = new DataInputBuffer();
        DataInputBuffer in2 = new DataInputBuffer();

        int[][] eSups = new int[length][];
        for(int u = 0 ; u < fonlCN.length ; u ++) {
            if (fonlCN[u] == null)
                continue;

            in1.reset(fonlCN[u], fonlCN[u].length);
            in2.reset(fonlVS[u], fonlVS[u].length);
            if (eSups[u] == null)
                eSups[u] = new int[fl[u]];
            while (true) {
                if (in2.getPosition() >= fonlVS[u].length)
                    break;

                int size = WritableUtils.readVInt(in2);
                for (int i = 0 ; i < size ; i ++) {
                    int len = WritableUtils.readVInt(in2);
                    int vIndex = WritableUtils.readVInt(in2);
                    int v = fonls[u][vIndex];
                    eSups[u][vIndex] ++;
                    for (int j = 0 ; j < len; j ++) {
                        int uwIndex = WritableUtils.readVInt(in1);
                        eSups[u][uwIndex] ++;

                        int vwIndex = WritableUtils.readVInt(in1);
                        if (eSups[v] == null)
                            eSups[v] = new int[fl[v]];
                        eSups[v][vwIndex] ++;
                    }

                    tcCount += len;
                }
            }
        }

        long t3 = System.currentTimeMillis();
        System.out.println("fill eSup in " + (t3 - t2) + " ms");
        System.out.println("tcCount: " + tcCount);

    }
}
