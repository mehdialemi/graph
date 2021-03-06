package ir.ac.sbu.graph.others.ktruss.parallel;

import ir.ac.sbu.graph.types.Edge;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all parallel methods to find ktruss
 */
public abstract class ParallelKTrussBase {

    public static final float BATCH_RATIO = 0.05f;
    public static final int BATCH_SIZE = 10000;

    protected final Edge[] edges;
    protected final int minSup;
    protected final int threads;
    protected AtomicInteger batchSelector;

    public ParallelKTrussBase(Edge[] edges, int minSup, int threads) {
        this.edges = edges;
        this.minSup = minSup;
        this.threads = threads;
    }

    public abstract void start() throws Exception;
}
