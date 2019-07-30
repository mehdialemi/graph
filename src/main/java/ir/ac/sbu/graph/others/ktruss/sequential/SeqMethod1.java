package ir.ac.sbu.graph.others.ktruss.sequential;

import ir.ac.sbu.graph.types.Edge;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class SeqMethod1 extends SequentialBase {

    public SeqMethod1(Edge[] edges, int minSup) {
        super(edges, minSup);
    }

    @Override
    public void start() throws Exception {
        long t1 = System.currentTimeMillis();
        Tuple2<List<int[]>, Set<Integer>[]> result = findEdgeTriangles(edges);
        long triangleTime = System.currentTimeMillis() - t1;
        Set<Integer>[] eTriangles = result._2;
        List<int[]> triangles = result._1;
        System.out.println("TriangleSubgraph time: " + triangleTime);

        int[] sorted = null;
        int iteration = 0;
        int lastIndex = 0;
        int prev = eTriangles.length;
        while (true) {
            System.out.println("iteration: " + ++iteration);
            long t1_sort = System.currentTimeMillis();
            sorted = SequentialUtils.sort(eTriangles, sorted, lastIndex);
            int invalids = prev - sorted.length;
            prev = sorted.length;
            long t2_sort = System.currentTimeMillis();
            System.out.println("Valid edges: " + sorted.length + ", invalid: " + invalids +
                ", quickSort time: " + (t2_sort - t1_sort) + " ms");
            lastIndex = 0;
            for (; lastIndex < sorted.length; lastIndex++) {
                int eIndex = sorted[lastIndex];
                Set<Integer> tSet = eTriangles[eIndex]; // triangle set
                int sup = tSet.size();
                if (sup >= minSup)
                    break;
                for (int tIndex : tSet) {
                    for (int e : triangles.get(tIndex))
                        if (eTriangles[e] != null && e != eIndex)
                            eTriangles[e].remove(tIndex);
                }
                eTriangles[eIndex] = null;
            }

            if (lastIndex == 0)
                break;

            long t2_findInvalids = System.currentTimeMillis();
            System.out.println("invalid time: " + (t2_findInvalids - t2_sort) + " ms");
        }

        long duration = System.currentTimeMillis() - t1;
        System.out.println("Number of edges is " + sorted.length + ", in " + duration + " ms");
    }

    public Tuple2<List<int[]>, Set<Integer>[]> findEdgeTriangles(final Edge[] edges) throws Exception {
        // find max vertex Id in parallel
        long t1 = System.currentTimeMillis();
        int max = -1;
        for(Edge e : edges) {
            if (e.v1 > max)
                max = e.v1;
            if (e.v2 > max)
                max = e.v2;
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Finding max in " + (t2 - t1) + " ms");

        int[] degArray = new int[max + 1];
        long t3 = System.currentTimeMillis();
        System.out.println("Construct degArray (AtomicInteger) in " + (t3 - t2) + " ms");

        // Construct degree array such that vertexId is the index of the array in parallel
        for (Edge e : edges) {
            degArray[e.v1]++;
            degArray[e.v2]++;
        }
        long t4 = System.currentTimeMillis();
        System.out.println("Fill degArray in " + (t4 - t3) + " ms");

        // Fill and quickSort vertices array.
        int[] vertices = SequentialUtils.sortDegrees(degArray);
        long t5 = System.currentTimeMillis();
        System.out.println("Sort degArray in " + (t5 - t4) + " ms");

        // Construct neighborhood
        List<Integer>[] neighbors = new List[vertices.length];

        // Initialize neighbors array
        for(int i = 0; i < vertices.length; i ++) {
            neighbors[vertices[i]] = new ArrayList<>();
        }
        long t6 = System.currentTimeMillis();
        System.out.println("Construct neighbors in " + (t6 - t5) + " ms");

        // Fill neighbors array
        for(int i = 0 ; i < edges.length ; i ++) {
            Edge e = edges[i];
            float diff = (degArray[e.v2] - degArray[e.v1]) + (e.v2 - e.v1) / (float) (e.v2 + e.v1);
            if (diff >= 0) {
                neighbors[e.v1].add(e.v2);
                neighbors[e.v1].add(i);
            } else {
                neighbors[e.v2].add(e.v1);
                neighbors[e.v2].add(i);
            }
        }
        long t7 = System.currentTimeMillis();
        System.out.println("Fill neighbors in " + (t7 - t6) + " ms");

        Set<Integer>[] eTriangles = new Set[edges.length];
        long t8 = System.currentTimeMillis();
        System.out.println("Construct eTriangles in " + (t8 - t7) + " ms");

        List<int[]> triangles = new ArrayList<>();

        long t9 = System.currentTimeMillis();
        System.out.println("Ready to triangle in " + (t9 - t8) + " ms");
        int triangleIndex = 0;
        for(int i = vertices.length - 1 ; i >= 0 ; i --) {
            int u = vertices[i]; // get current vertex id as u

            // since vertices are sorted based on degrees in async order, don't process all degree = 0 or degree = 1 vertices
            if (degArray[u] == 1)
                break;

            // construct a map of nodes to their edges
            List<Integer> uNeighbors = neighbors[u];
            Map<Integer, Integer> unEdges = new HashMap<>(uNeighbors.size() / 2);
            for(int ni = 0 ; ni < uNeighbors.size() ; ni += 2)
                unEdges.put(uNeighbors.get(ni), uNeighbors.get(ni + 1));

            // iterate over neighbors of u using edge info
            for(int j = 0 ; j < uNeighbors.size() ; j += 2) {
                int v = uNeighbors.get(j);
                int uv = uNeighbors.get(j + 1);
                // iterate over neighbors of vertex
                List<Integer> vNeighbors = neighbors[v];
                for(int k = 0 ; k < vNeighbors.size() ; k += 2) {
                    int w = vNeighbors.get(k);
                    Integer uw = unEdges.get(w);
                    if (uw != null) {
                        int vw = vNeighbors.get(k + 1);
                        if (eTriangles[uv] == null)
                            eTriangles[uv] = new HashSet<>();
                        eTriangles[uv].add(triangleIndex);

                        if (eTriangles[vw] == null)
                            eTriangles[vw] = new HashSet<>();
                        eTriangles[vw].add(triangleIndex);

                        if (eTriangles[uw] == null)
                            eTriangles[uw] = new HashSet<>();
                        eTriangles[uw].add(triangleIndex);

                        triangles.add(new int[]{uv, vw, uw});
                        triangleIndex ++;
                    }
                }
            }
        }

        long t10 = System.currentTimeMillis();
        System.out.println("TriangleSubgraph finished in " + (t10 - t9) + " ms");
        return new Tuple2<>(triangles, eTriangles);
    }
}
