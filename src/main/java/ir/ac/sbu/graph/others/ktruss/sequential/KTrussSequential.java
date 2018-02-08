package ir.ac.sbu.graph.others.ktruss.sequential;

import ir.ac.sbu.graph.others.GraphLoader;
import ir.ac.sbu.graph.types.Edge;

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
        
        int method = 1;
        if (args.length > 1)
            method = Integer.parseInt(args[2]);
        
        System.out.println("Start ktruss with k = " + k + ", input: " + inputPath + " method: " + method);
        final Edge[] edges = GraphLoader.loadFromLocalFile(inputPath);

        long t1 = System.currentTimeMillis();
        SequentialBase sequentialMethod = null;
        switch (method) {
            case 1: sequentialMethod = new SeqMethod1(edges, minSup);
                break;
            case 2: sequentialMethod = new SeqMethod2(edges, minSup);
                break;
            case 3: sequentialMethod = new SeqMethod3(edges, minSup);
        }

        sequentialMethod.start();
        long t2 = System.currentTimeMillis();
        System.out.println("Duration: " + (t2 - t1) + " ms");
    }
}
