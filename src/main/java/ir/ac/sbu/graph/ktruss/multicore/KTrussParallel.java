package ir.ac.sbu.graph.ktruss.multicore;

import ir.ac.sbu.graph.ktruss.sequential.Edge;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static ir.ac.sbu.graph.ktruss.multicore.MultiCoreUtils.createBuckets;

/**
 * Truss Decomposition based on Edge TriangleParallel list.
 */
public class KTrussParallel {

    public static final double MAX_CHECK_RATIO = 0.3;
    private static final Tuple3<Integer, Integer, Integer> INVALID_TUPLE3 = new Tuple3<>(-1, -1, -1);

    public static void main(String[] args) throws Exception {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
//        String inputPath = "/home/mehdi/graph-data/Email-EuAll.txt";
        if (args.length > 0)
            inputPath = args[0];

        int k = 4; // k-truss
        if (args.length > 1)
            k = Integer.parseInt(args[1]);
        int minSup = k - 2;

        int threads = 4;
        if (args.length > 2)
            threads = Integer.parseInt(args[2]);

        int sequentialBucket = 100000;
        if (args.length > 3)
            sequentialBucket = Integer.parseInt(args[3]);


        FileInputStream inputStream = new FileInputStream(inputPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        List<Edge> list = reader.lines().parallel().filter(line -> !line.startsWith("#"))
            .map(line -> line.split("\\s+"))
            .map(split -> new Edge(Integer.parseInt(split[0]), Integer.parseInt(split[1])))
            .filter(e -> e.v1 != e.v2)
            .collect(Collectors.toList());

        final Edge[] edges = list.toArray(new Edge[0]);
        System.out.println("Graph loaded, edges: " + edges.length);
        long t1 = System.currentTimeMillis();
        Tuple2<Map<Integer, int[]>, Set<Integer>[]> result = TriangleParallel.findEdgeTriangles(edges, threads);
        Set<Integer>[] eTriangles = result._2;
        Map<Integer, int[]> triangles = result._1;
        long triangleTime = System.currentTimeMillis() - t1;
        System.out.println("Triangle time: " + triangleTime + " , count: " + eTriangles.length);

        Tuple2<Integer, int[]> sorted = null;
        int iteration = 0;
        while (true) {
            System.out.println("iteration: " + ++iteration);
            sorted = sort(eTriangles, sorted, threads, minSup);
            System.out.println("Valid edges: " + sorted._2.length + ", to remove" + sorted._1);
            List<Tuple2<Integer, Integer>> buckets = MultiCoreUtils.createBuckets(threads, sorted._1);
            final int[] etIndex = sorted._2;
            HashSet<Integer> tInvalids = buckets.parallelStream().map(bucket -> {
                HashSet<Integer> tInvalidLocal = new HashSet<>();
                for (int i = bucket._1; i < bucket._2; i++) {
                    for (int tIndex : eTriangles[etIndex[i]])
                        tInvalidLocal.add(tIndex);
                    eTriangles[etIndex[i]] = null;
                }
                return tInvalidLocal;
            }).reduce((s1, s2) -> {
                s1.addAll(s2);
                return s1;
            }).orElse(new HashSet<>());

            tInvalids.parallelStream().forEach(tIndex -> {
                for (int e : triangles.get(tIndex)) {
                    if (eTriangles[e] != null)
                        eTriangles[e].remove(tIndex);
                }
            });
            if (sorted._1 == 0)
                break;
        }

        long duration = System.currentTimeMillis() - t1;
        System.out.println("Number of edges is " + sorted._2.length + ", in " + duration + " ms");
    }

    public static Tuple2<Integer, int[]> sort(final Set<Integer>[] eTriangles, final Tuple2<Integer, int[]> prevEdges, int threads, int minSup) throws Exception {
        final boolean usePrev = prevEdges == null || prevEdges._2 == null ||
            (prevEdges != null && prevEdges._1 < prevEdges._2.length * MAX_CHECK_RATIO) ? false : true;
        List<Tuple2<Integer, Integer>> buckets =
            usePrev ? createBuckets(threads, prevEdges._2, prevEdges._1) : createBuckets(threads, eTriangles);
        Tuple3<Integer, Integer, Integer> minMaxLen = buckets.parallelStream().map(bucket -> {
            int min = eTriangles.length;
            int max = 0;
            int len = 0;
            if (usePrev) {
                for (int i = bucket._1 ; i < bucket._2; i ++) {
                    int num = eTriangles[prevEdges._2[i]].size();
                    if (num < min) {
                        min = num;
                    } else if (num > max) {
                        max = num;
                    }
                    len ++;
                }
            } else {
                for (int i = bucket._1; i < bucket._2; i++) {
                    if (eTriangles[i] == null || eTriangles[i].isEmpty())
                        continue;
                    int num = eTriangles[i].size();
                    if (num < min) {
                        min = num;
                    } else if (num > max) {
                        max = num;
                    }
                    len ++;
                }
            }
            return new Tuple3<>(min, max, len);
        }).reduce((r1 , r2) -> new Tuple3<>(Math.min(r1._1(), r2._1()), Math.max(r1._2(), r2._2()), r1._3() + r2._3()))
            .orElse(INVALID_TUPLE3);

        if (minMaxLen == INVALID_TUPLE3)
            throw new Exception("Invalid tuple");

        int min = minMaxLen._1();
        int max = minMaxLen._2();
        int length = minMaxLen._3();

        // init the frequencies
        AtomicInteger[] counts = new AtomicInteger[max - min + 1];
        for(int i = 0 ; i < counts.length ; i ++)
            counts[i] = new AtomicInteger(0);

        buckets.parallelStream().forEach(bucket -> {
            if (usePrev) {
                for (int i = bucket._1 ; i < bucket._2 ; i ++)
                    counts[eTriangles[prevEdges._2[i]].size() - min].incrementAndGet();
            } else {
                for (int i = bucket._1; i < bucket._2; i++) {
                    if (eTriangles[i] == null || eTriangles[i].isEmpty())
                        continue;
                    counts[eTriangles[i].size() - min].incrementAndGet();
                }
            }
        });

        counts[0].decrementAndGet();
        for (int i = 1; i < counts.length; i++) {
            counts[i].addAndGet(counts[i - 1].get());
        }

        int minSupIndex = 0;
        int[] edges = new int[length];
        if (usePrev) {
            for(int i = prevEdges._1 ; i < prevEdges._2.length ; i ++) {
                int sup = eTriangles[prevEdges._2[i]].size();
                int index = counts[sup - min].getAndDecrement();
                edges[index] = prevEdges._2[i];
                if (sup < minSup)
                    minSupIndex ++;
            }
        } else {
            for (int i = eTriangles.length - 1; i >= 0; i--) {
                if (eTriangles[i] == null || eTriangles[i].isEmpty())
                    continue;
                int sup = eTriangles[i].size();
                int index = counts[sup - min].getAndDecrement();
                edges[index] = i;
                if (sup < minSup)
                    minSupIndex ++;
            }
        }
        return new Tuple2<>(minSupIndex, edges);
    }
}
