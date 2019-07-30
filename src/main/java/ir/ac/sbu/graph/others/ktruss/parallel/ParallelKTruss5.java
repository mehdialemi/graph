package ir.ac.sbu.graph.others.ktruss.parallel;

import ir.ac.sbu.graph.utils.GraphUtils;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.utils.VertexCompare;
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
        final int[][] neighbors = result._1();
        long tStart = System.currentTimeMillis();
        final int[] d = result._2();
        final int[][] neighborsEdge = result._3();

        final AtomicInteger[] edgeSup = new AtomicInteger[edges.length];
        for (int i = 0; i < edges.length; i++) {
            edgeSup[i] = new AtomicInteger(0);
        }
        long tEdgeCount = System.currentTimeMillis();
        System.out.println("construct edge count in " + (tEdgeCount - tStart) + " ms");

        int vCount = d.length;

        final VertexCompare vertexCompare = new VertexCompare(d);
        int[] fLens = new int[vCount];
        batchSelector = new AtomicInteger(0);
        final int maxFSize = forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().map(thread -> {
            int maxLen = 0;
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= vCount)
                    break;
                int end = Math.min(vCount, BATCH_SIZE + start);
                for (int u = start; u < end; u++) {
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
            }
            return maxLen;
        })).get().reduce((a, b) -> Math.max(a, b)).getAsInt();

        long tFonl = System.currentTimeMillis();
        System.out.println("construct fonl in " + (tFonl - tEdgeCount) + " ms");
        System.out.println("max fonl support: " + maxFSize);

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(partition -> {
                while (true) {
                    int start = batchSelector.getAndAdd(BATCH_SIZE);
                    if (start >= vCount)
                        break;
                    int end = Math.min(vCount, BATCH_SIZE + start);
                    for (int u = start; u < end; u++) {
                        if (fLens[u] == 0)
                            continue;

                        int[] uNeighbors = neighbors[u];
                        int[] uNeighborsEdges = neighborsEdge[u];

                        // Find triangle by checking connectivity of neighbors
                        for (int uvIndex = 0; uvIndex < fLens[u]; uvIndex++) {
                            int v = uNeighbors[uvIndex];
                            int[] vNeighbors = neighbors[v];

                            int uwIndex = uvIndex + 1, vwIndex = 0;

                            // intersection on u neighbors and vertex neighbors
                            while (uwIndex < fLens[u] && vwIndex < fLens[v]) {
                                if (uNeighbors[uwIndex] == vNeighbors[vwIndex]) {
                                    edgeSup[uNeighborsEdges[uvIndex]].incrementAndGet();
                                    edgeSup[uNeighborsEdges[uwIndex]].incrementAndGet();
                                    edgeSup[neighborsEdge[v][vwIndex]].incrementAndGet();
                                    uwIndex++;
                                    vwIndex++;
                                } else if (vertexCompare.compare(uNeighbors[uwIndex], vNeighbors[vwIndex]) == -1)
                                    uwIndex++;
                                else
                                    vwIndex++;
                            }
                        }
                    }
                }
            }
        )).get();


        long tTC = System.currentTimeMillis();
        System.out.println("vTc in " + (tTC - tStart) + " ms");

        int sum = 0;
        for (int i = 0; i < edgeSup.length; i++) {
            sum += edgeSup[i].get();
        }

        System.out.println("vTc: " + sum / 3);
    }
}
