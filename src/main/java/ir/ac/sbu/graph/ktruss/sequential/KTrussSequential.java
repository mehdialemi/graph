package ir.ac.sbu.graph.ktruss.sequential;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Truss Decomposition based on Edge TriangleParallel list.
 */
public class KTrussSequential {

    public static final double MAX_CHECK_RATIO = 0.3;

    public static void main(String[] args) throws Exception {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
//        String inputPath = "/home/mehdi/graph-data/Email-EuAll.txt";
        if (args.length > 0)
            inputPath = args[0];

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[1]);
        int minSup = k - 2;

        FileInputStream inputStream = new FileInputStream(inputPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        List<Edge> list = reader.lines().filter(line -> !line.startsWith("#"))
            .map(line -> line.split("\\s+"))
            .map(split -> new Edge(Integer.parseInt(split[0]), Integer.parseInt(split[1])))
            .filter(e -> e.v1 != e.v2)
            .collect(Collectors.toList());

        long t1 = System.currentTimeMillis();
        final Edge[] edges = list.toArray(new Edge[0]);
        System.out.println("Graph loaded, edges: " + edges.length);
        Tuple2<List<int[]>, Set<Integer>[]> result = TriangleSequential.findEdgeTriangles(edges);
        Set<Integer>[] eTriangles = result._2;
        List<int[]> triangles = result._1;
        System.out.println("After triangle, #edgeTriangles: " + eTriangles.length);

        int[] eSorted = null;
        int iteration = 0;
        int lastIndex = 0;
        while (true) {
            System.out.println("iteration: " + ++iteration);
            eSorted = sort(eTriangles, eSorted, lastIndex);

            lastIndex = 0;
            System.out.println("Valid edges: " + eSorted.length);
            HashSet<Integer> tInvalids = new HashSet<>();
            for (; lastIndex < eSorted.length; lastIndex++) {
                int eIndex = eSorted[lastIndex];
                Set<Integer> tSet = eTriangles[eIndex]; // triangle set
                int sup = tSet.size();
                if (sup >= minSup)
                    break;

                for (int tIndex : tSet)
                    tInvalids.add(tIndex);

                eTriangles[eIndex] = null;
            }

            for (int tIndex : tInvalids) {
                for (int e : triangles.get(tIndex)) {
                    if (eTriangles[e] != null)
                        eTriangles[e].remove(tIndex);
                }
            }

            System.out.println("remove: " + lastIndex);
            if (lastIndex == 0)
                break;
        }

        long duration = System.currentTimeMillis() - t1;
        System.out.println("Number of edges is " + eSorted.length + ", in " + duration + " ms");
    }

    public static int[] sort(Set<Integer>[] eTriangles, int[] eSorted, int eIndex) {
        if (eSorted != null && eIndex < eSorted.length * MAX_CHECK_RATIO)
            eSorted = null;

        int minSup = eTriangles.length;
        int maxSup = 0;
        int length = maxSup;
        if (eSorted == null) {
            eIndex = 0;
            for (int i = eIndex; i < eTriangles.length; i++) {
                if (eTriangles[i] == null)
                    continue;
                int num = eTriangles[i].size();
                if (num < minSup) {
                    minSup = num;
                } else if (num > maxSup) {
                    maxSup = num;
                }
                length++;
            }
        } else {
            for (int i = eIndex; i < eSorted.length; i ++) {
                int num = eTriangles[eSorted[i]].size();
                if (num < minSup) {
                    minSup = num;
                } else if (num > maxSup) {
                    maxSup = num;
                }
                length++;
            }
        }
        // init array of frequencies
        int[] counts = new int[maxSup - minSup + 1];

        // init the frequencies
        int[] edges = new int[length];
        if (eSorted != null) {
            for (int i = eIndex ; i < eSorted.length ; i ++) {
                int index = eTriangles[eSorted[i]].size() - minSup;
                counts[index] ++;
            }
        } else {
            for (int i = eIndex; i < eTriangles.length; i++) {
                if (eTriangles[i] == null)
                    continue;
                int index = eTriangles[i].size() - minSup;
                counts[index]++;
            }
        }

        counts[0]--;
        for (int i = 1; i < counts.length; i++) {
            counts[i] = counts[i] + counts[i - 1];
        }

        if (eSorted != null) {
            for(int i = eIndex ; i < eSorted.length ; i ++) {
                int index = counts[eTriangles[eSorted[i]].size() - minSup]--;
                edges[index] = eSorted[i];
            }
        } else {
            for (int i = eTriangles.length - 1; i >= 0; i--) {
                if (eTriangles[i] == null)
                    continue;
                int index = counts[eTriangles[i].size() - minSup]--;
                edges[index] = i;
            }
        }

        return edges;
    }

}
