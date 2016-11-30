package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.GraphLoader;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

import static ir.ac.sbu.graph.MultiCoreUtils.createBuckets;

/**
 * Truss Decomposition based on Edge TriangleParallel list.
 */
public class KTrussParallel {

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

        int method = 2;
        if (args.length > 3)
            method = Integer.parseInt(args[3]);

        Edge[] edges = GraphLoader.loadFromLocalFile(inputPath);

        long t1 = System.currentTimeMillis();
        ParallelBase parallelBase = null;
        switch (method) {
            case 1: parallelBase = new Method1(edges, minSup, threads);
                break;
            case 2: parallelBase = new Method2(edges, minSup, threads);
                break;
        }

        System.out.println("Start ktruss with k = " + k + ", threads = " + threads + ", input: " + inputPath);

        parallelBase.start();
        long t2 = System.currentTimeMillis();

        System.out.println("Duration: " + (t2- t1) + " ms");
    }
}
