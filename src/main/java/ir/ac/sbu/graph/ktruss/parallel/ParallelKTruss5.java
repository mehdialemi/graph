package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.GraphUtils;
import ir.ac.sbu.graph.PartitioningUtils;
import ir.ac.sbu.graph.VertexCompare;
import org.apache.hadoop.io.DataOutputBuffer;
import scala.Tuple3;

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
        Tuple3<int[][], int[], int[][]> result = GraphUtils.createNeighborWithEdgeIndex(edges);
        int[][] neighbors = result._1();
        final int[] d = result._2();
        int[][] neighborsEdge = result._3();

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
                int[] uNeighborsEdges = neighborsEdge[u];
                for (int uvIndex = 0; uvIndex < uNeighbors.length; uvIndex++) {
                    if (vertexCompare.compare(u, uNeighbors[uvIndex]) == -1) {
                        index++;
                        if (index == uvIndex)
                            continue;

                        int tmp = uNeighbors[uvIndex];
                        uNeighbors[uvIndex] = uNeighbors[index];
                        uNeighbors[index] = tmp;

                        tmp = uNeighborsEdges[uvIndex];
                        uNeighborsEdges[uvIndex] = uNeighborsEdges[index];
                        uNeighborsEdges[index] = tmp;
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

        AtomicInteger[] edgeCount = new AtomicInteger[edges.length];

        for(int i = 0 ; i < edges.length ; i ++) {
            edgeCount[i] = new AtomicInteger(0);
        }

        long tEdgeCount = System.currentTimeMillis();
        System.out.println("construct edge count in " + (tEdgeCount - tFonl) + " ms");

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(partition -> {
                DataOutputBuffer out = new DataOutputBuffer(maxFSize * maxFSize);
                for (int u = 0; u < vCount; u++) {
                    if (partitions[u] != partition || fLens[u] == 0)
                        continue;

                    out.reset();
                    int[] uNeighbors = neighbors[u];
                    int[] uNeighborsEdges = neighborsEdge[u];

                    // Find triangle by checking connectivity of neighbors
                    for (int uvIndex = 0; uvIndex < fLens[u]; uvIndex++) {
                        int v = uNeighbors[uvIndex];
                        int[] vNeighbors = neighbors[v];

                        int uwIndex = uvIndex + 1, vwIndex = 0;

                        // intersection on u neighbors and v neighbors
                        while (uwIndex < fLens[u] && vwIndex < fLens[v]) {
                            if (uNeighbors[uwIndex] == vNeighbors[vwIndex]) {
                                edgeCount[uNeighborsEdges[uvIndex]].incrementAndGet();
                                edgeCount[uNeighborsEdges[uwIndex]].incrementAndGet();
                                edgeCount[neighborsEdge[v][vwIndex]].incrementAndGet();
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
                }
            }
        )).get();

        long tTC = System.currentTimeMillis();
        System.out.println("tc in " + (tTC - tStart) + " ms");

        int sum = 0;
        for(int i = 0 ; i < edgeCount.length; i ++) {
            sum += edgeCount[i].get();
        }

        System.out.println("tc: " + sum / 3);
    }
}
