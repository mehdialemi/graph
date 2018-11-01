package ir.ac.sbu.graph.spark.kcore;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.SparkAppConf;

/**
 * Conf class for iterative app which consider a 'kc' in their algorithm
 */
public class KCoreConf extends SparkAppConf {

    protected int kc;
    private int kcMaxIter;

    public KCoreConf(ArgumentReader argumentReader) {
        super(argumentReader);
        kcMaxIter = argumentReader.nextInt(1000);
    }

    public KCoreConf(ArgumentReader argumentReader, boolean readK) {
        super(argumentReader);
        if (readK)
            kc = argumentReader.nextInt(2);
        kcMaxIter = argumentReader.nextInt(10);
    }

    public void setKcMaxIter(int iter) {
        this.kcMaxIter = iter;
    }

    public void setKc(int kc) {
        this.kc = kc;
    }

    @Override
    protected String createAppName() {
        return "KCore-" + kc + "-" + kcMaxIter + " (" + super.createAppName() + ")";
    }

    public int getKcMaxIter() {
        return this.kcMaxIter;
    }

    public int getKc() {
        return kc;
    }

}
