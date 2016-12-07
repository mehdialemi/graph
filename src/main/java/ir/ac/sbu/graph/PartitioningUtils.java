package ir.ac.sbu.graph;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 *
 */
public class PartitioningUtils {

    public static void printStatus(int partitionNum, int[] partitions, int[][] fonls, byte[][] fonlNeighborL1) throws IOException {
        DataInputBuffer in = new DataInputBuffer();
        int[] inE = new int[partitionNum];
        int[] outE = new int[partitionNum];

        int[] inV = new int[partitionNum];
        int[] outV = new int[partitionNum];

        int[][] vs = new int[partitionNum][];
        for (int i = 0; i < vs.length; i++) {
            vs[i] = new int[fonls.length];
        }

        int[] outVone = new int[partitionNum];
        int[] outVmore = new int[partitionNum];

        for (int u = 0; u < fonlNeighborL1.length; u++) {
            if (fonlNeighborL1[u] == null)
                continue;

            in.reset(fonlNeighborL1[u], fonlNeighborL1[u].length);
            while (true) {
                if (in.getPosition() >= fonlNeighborL1[u].length)
                    break;

                int size = WritableUtils.readVInt(in);
                for (int i = 0; i < size; i++) {
                    int len = WritableUtils.readVInt(in);
                    int vIndex = WritableUtils.readVInt(in);
                    int v = fonls[u][vIndex];
                    if (partitions[u] == partitions[v]) {
                        inE[partitions[u]]++;
                        if (vs[partitions[u]][v] == 0) {
                            inV[partitions[u]]++;
                            vs[partitions[u]][v] = 1;
                        }
                        if (vs[partitions[u]][u] == 0) {
                            inV[partitions[u]]++;
                            vs[partitions[u]][u] = 1;
                        }
                    } else {
                        if (vs[partitions[u]][v] == 0) {
                            outV[partitions[u]]++;
                            vs[partitions[u]][v] = 1;
                            outVone[partitions[u]] ++;
                        }
                        outVmore[partitions[u]] ++;
                        outE[partitions[u]]++;
                    }
                }
            }
        }

        DecimalFormat df = new DecimalFormat("#.##");
        int totalIn = 0;
        int totalOut = 0;
        for (int i = 0; i < partitionNum; i++) {
            System.out.println("partition " + i + " => sizeE: " + (inE[i] + outE[i]) +
                " inE: " + inE[i] + ", outE: " + outE[i] + ", " +
                "inRatioE: " + df.format(inE[i] / (double)(inE[i] + outE[i])));
            System.out.println("partition " + i + " => sizeV: " + (inV[i] + outV[i]) +
                " inV: " + inV[i] + ", outV: " + outV[i] + ", " +
                "total inRatioV: " + df.format(inV[i] / (double)(inV[i] + outV[i])));
            System.out.println("partition " + i + " => outVmore/outVone: " + df.format(outVmore[i] / (double) outVone[i]));
            totalIn += inE[i];
            totalOut += outE[i];
        }

        System.out.println("total inE: " + totalIn + ", total outE: " + totalOut + ", " +
            "total ratio: " + df.format(totalIn / (double) (totalIn + totalOut)));

    }

    public static int[] createPartitions(int vCount, int threads, int batchSize) {
        int[] partition = new int[vCount];
        int cp = 0;
        int rem = vCount - threads * batchSize * 2;
        for (int u = 0; u < vCount; u++) {
            int p = cp % threads;
            partition[u] = p;
            if ((u + 1) % batchSize == 0)
                cp++;
            if (p == 0 && u > rem) {
                batchSize = Math.max(batchSize / 2, 10);
                rem = vCount - threads * batchSize * 2;
            }
        }
        return partition;
    }
}
