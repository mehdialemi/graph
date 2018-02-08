package ir.ac.sbu.graph.others.ktruss.sequential;

import ir.ac.sbu.graph.types.Edge;

/**
 * Base class for all methods which are going to find ktruss using just a single core.
 */
public abstract class SequentialBase {

    protected final Edge[] edges;
    protected final int minSup;

    public SequentialBase(Edge[] edges, int minSup) {
        this.edges = edges;
        this.minSup = minSup;
    }

    public abstract void start() throws Exception;
}
