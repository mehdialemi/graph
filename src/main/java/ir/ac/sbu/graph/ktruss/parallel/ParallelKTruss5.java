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
        long tPartition = System.currentTimeMillis();
        System.out.println("partition in " + (tPartition - tStart) + " ms");

        final VertexCompare vertexCompare = new VertexCompare(d);
        int[] fLens = new int[vCount];
        final int maxFSize = forkJoinPool.submit(() -> IntStream.range(0, threads).map(partition -> {
            int maxLen = 0;
            for (int u = 0; u < vCount; u++) {
                if (d[u] < 2 || partitions[u] != partition)
                    continue;

                int index = -1;
                int[] uNeighbors = neighbors[u];
                for (int uvIndex = 0; uvIndex < uNeighbors.length; uvIndex++) {
                    if (vertexCompare.compare(u, uNeighbors[uvIndex]) == -1) {
                        index++;
                        if (index == uvIndex)
                            continue;

                        int hv = uNeighbors[uvIndex];
                        uNeighbors[uvIndex] = uNeighbors[index];
                        uNeighbors[index] = hv;
                    }
                }
                fLens[u] = index + 1;
                if (fLens[u] == 0)
                    continue;
                vertexCompare.quickSort(uNeighbors, 0, fLens[u] - 1);
                if (fLens[u] > maxLen)
                    maxLen = fLens[u];
            }
            return maxLen;
        })).get().reduce((a, b) -> Math.max(a, b)).getAsInt();

        long tFonl = System.currentTimeMillis();
        System.out.println("construct fonl in " + (tFonl - tPartition) + " ms");
        System.out.println("max fonl size: " + maxFSize);

        byte[][] externals = new byte[vCount][];
        int[][] counts = new int[vCount][];
        for (int u = 0; u < vCount; u++) {
            if (fLens[u] == 0)
                continue;
            counts[u] = new int[fLens[u]];
        }

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(partition -> {
                DataOutputBuffer out = new DataOutputBuffer(maxFSize * maxFSize);
                for (int u = 0; u < vCount; u++) {
                    if (partitions[u] != partition || fLens[u] == 0)
                        continue;

                    out.reset();
                    int[] uNeighbors = neighbors[u];

                    // Find triangle by checking connectivity of neighbors
                    for (int uvIndex = 0; uvIndex < fLens[u]; uvIndex++) {
                        int v = uNeighbors[uvIndex];
                        int[] vNeighbors = neighbors[v];

                        int uwIndex = uvIndex + 1, vwIndex = 0;

                        // intersection on u neighbors and v neighbors
                        while (uwIndex < fLens[u] && vwIndex < fLens[v]) {
                            if (uNeighbors[uwIndex] == vNeighbors[vwIndex]) {
                                counts[u][uvIndex]++;
                                counts[u][uwIndex]++;
                                if (partitions[v] == partition)
                                    counts[v][vwIndex]++;
                                else {
                                    try {
                                        WritableUtils.writeVInt(out, v);
                                        WritableUtils.writeVInt(out, vwIndex);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                uwIndex++;
                                vwIndex++;
                            } else if (vertexCompare.compare(uNeighbors[uwIndex], vNeighbors[vwIndex]) == -1)
                                uwIndex++;
                            else
                                vwIndex++;
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
        for (int i = 0; i < vCount; i++) {
            if (fLens[i] == 0)
                continue;
            for (int j = 0; j < counts[i].length; j++) {
                sum += counts[i][j];
            }
        }

        System.out.println("tc: " + sum / 3 + " update counts in " + (tUpdateCounts - tTC) + " ms");
    }
}
