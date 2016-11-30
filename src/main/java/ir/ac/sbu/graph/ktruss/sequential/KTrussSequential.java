package ir.ac.sbu.graph.ktruss.sequential;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.GraphLoader;
import scala.Tuple2;

import java.util.List;
import java.util.Set;

/**
 * Truss Decomposition based on Edge TriangleParallel list.
 */
public class KTrussSequential {

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
        
        int method = 3;
        if (args.length > 1)
            method = Integer.parseInt(args[2]);
        
        System.out.println("Start ktruss with k = " + k + ", input: " + inputPath + " method: " + method);
        final Edge[] edges = GraphLoader.loadFromLocalFile(inputPath);

        long t1 = System.currentTimeMillis();
        SequentialBase sequentialMethod = null;
        switch (method) {
            case 1: sequentialMethod = new Method1(edges, minSup);
                break;
            case 2: sequentialMethod = new Method2(edges, minSup);
                break;
            case 3: sequentialMethod = new Method3(edges, minSup);
        }

        sequentialMethod.start();
        long t2 = System.currentTimeMillis();
        System.out.println("Duration: " + (t2 - t1) + " ms");
    }
}
