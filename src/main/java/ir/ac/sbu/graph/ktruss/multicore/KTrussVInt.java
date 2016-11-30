package ir.ac.sbu.graph.ktruss.multicore;

import ir.ac.sbu.graph.GraphLoader;
import ir.ac.sbu.graph.VertexCompare;
import ir.ac.sbu.graph.Edge;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;

/**
 *
 */
public class KTrussVInt {

    public static final int SIGHN = -1;

    public static void main(String[] args) throws IOException {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int k = 4; // k-truss
        if (args.length > 1)
            k = Integer.parseInt(args[1]);
        int minSup = k - 2;
        System.out.println("Start ktruss with k = " + k + ", input: " + inputPath);

        Edge[] edges = GraphLoader.loadFromLocalFile(inputPath);
        ktruss(edges, minSup);
    }

    public static void ktruss(Edge[] edges, int minSup) throws IOException {

    }
}
