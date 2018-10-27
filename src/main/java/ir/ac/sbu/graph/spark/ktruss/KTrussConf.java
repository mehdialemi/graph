package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.kcore.KCoreConf;

/**
 *
 */
public class KTrussConf extends KCoreConf {

    private int kt;
    private int ktMaxIter;

    public KTrussConf(ArgumentReader argumentReader) {
        super(argumentReader);
        kt = argumentReader.nextInt(3);
        kc = kt - 1;
        ktMaxIter = argumentReader.nextInt(10);
    }

    @Override
    protected String createAppName() {
        return "KTruss-" + kt + "-" + ktMaxIter + "--" + super.createAppName();
    }

    public int getKt() {
        return kt;
    }

    public int getKtMaxIter() {
        return ktMaxIter;
    }
}
