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
        int[] inside = new int[partitionNum];
        int[] outside = new int[partitionNum];

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
                    if (partitions[u] == partitions[v])
                        inside[partitions[u]] ++;
                    else
                        outside[partitions[u]] ++;
                }
            }
        }

        DecimalFormat df = new DecimalFormat("#.##");
        int totalIn = 0;
        int totalOut = 0;
        for (int i = 0; i < partitionNum; i++) {
            System.out.println("partition " + i + " => size: " + (inside[i] + outside[i]) +
                " inside: " + inside[i] + ", outside: " + outside[i] + ", " +
                "inRatio: " + df.format(inside[i] / (double)(inside[i] + outside[i])));
            totalIn += inside[i];
            totalOut += outside[i];
        }

        System.out.println("total inside: " + totalIn + ", total outside: " + totalOut + ", " +
            "total ratio: " + df.format(totalIn / (double) (totalIn + totalOut)));

    }
}
