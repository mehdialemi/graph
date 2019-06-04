package ir.ac.sbu.graph.utils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class OrderedNeighborList {

    public static IntList intersection(int[] hDegs, int[] forward, int hIndex, int fIndex) {
        int fLen = forward.length;
        int hLen = hDegs.length;
        IntList intersects = null;

        if (hDegs.length == 0 || fLen == 0)
            return intersects;

        boolean leftRead = true;
        boolean rightRead = true;

        int h = 0;
        int f = 0;

        boolean finish = false;
        while (!finish) {

            if (hIndex >= hLen && fIndex >= fLen)
                break;

            if ((hIndex >= hLen && !rightRead) || (fIndex >= fLen && !leftRead))
                break;

            if (leftRead && hIndex < hLen) {
                h = hDegs[hIndex++];
            }

            if (rightRead && fIndex < fLen) {
                f = forward[fIndex++];
            }

            if (h == f) {
                if (intersects == null)
                    intersects = new IntArrayList();
                intersects.add(h);

                leftRead = true;
                rightRead = true;
            } else if (h < f) {
                leftRead = true;
                rightRead = false;
            } else {
                leftRead = false;
                rightRead = true;
            }
        }

        return intersects;
    }
}
