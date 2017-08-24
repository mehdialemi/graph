package ir.ac.sbu.graph.utils;

import java.io.Serializable;

/**
 * Print standard outputs in order we could extract required information for output console.
 */
public class OutUtils implements Serializable {

    public static void printOutputLCC(long nodes, float sumLCC, float avgLCC) {
        System.out.println("Nodes = " + nodes + ", Sum_LCC = " + sumLCC + ", AVG_LCC = " + avgLCC);
    }

    public static void printOutputGCC(long nodes, long triangles, float gcc) {
        System.out.println("Nodes = " + nodes + ", Triangles = " + triangles + ", GCC = " + gcc);
    }

    public static void printOutputTC(long triangles) {
        System.out.println("Total triangles = " + triangles);
    }
}
