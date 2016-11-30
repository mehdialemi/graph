package ir.ac.sbu.graph.ktruss.parallel;

import java.util.*;

/**
 *
 */
public class VertexEdge {
    List<Integer>[] neighbors;

    public VertexEdge(int neighborSize) {
        neighbors = new List[neighborSize];
    }

    public void add(int nIndex, int otherNeighbor) {
        if (neighbors[nIndex] == null) {
            neighbors[nIndex] = new ArrayList<>();
        }
        neighbors[nIndex].add(otherNeighbor);
    }
}
