package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;

/**
 * Base class for all parallel methods to find ktruss
 */
public abstract class ParallelBase {

    public static final float BATCH_RATIO = 0.05f;
    public static final int BATCH_SIZE = 10000;

    protected final Edge[] edges;
    protected final int minSup;
    protected final int threads;

    public ParallelBase(Edge[] edges, int minSup, int threads) {
        this.edges = edges;
        this.minSup = minSup;
        this.threads = threads;
    }

    public abstract void start() throws Exception;
}
