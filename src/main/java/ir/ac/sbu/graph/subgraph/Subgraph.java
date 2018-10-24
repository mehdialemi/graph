package ir.ac.sbu.graph.subgraph;

import java.io.Serializable;
import java.util.List;

/**
 * A subgraph with limited number of vertex
 */
public class Subgraph implements Serializable {
    public final static short MIN_SIZE = Short.MIN_VALUE;
    public final static short MAX_SIZE = Short.MAX_VALUE;

    int id;
    short[] localNeighbors;
    List<Subgraph> inPartitionNeighbors;
    List<Subgraph> outPartitionNeighbors;
}
