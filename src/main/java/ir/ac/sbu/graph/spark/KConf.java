package ir.ac.sbu.graph.spark;

/**
 * Conf class for iterative app which consider a 'k' in their algorithm
 */
public class KConf extends SparkAppConf {

    private int k;
    private int maxIter;

    public KConf(SparkAppConf conf) {
        super(conf);
    }

    public KConf(ArgumentReader argumentReader, String name) {
        super(argumentReader, name);
        k = argumentReader.nextInt(4);
        maxIter = argumentReader.nextInt(10);
    }

    public KConf create(int k, int maxIter) {
        KConf kConf = new KConf(this);
        kConf.k = k;
        kConf.maxIter = maxIter;
        return kConf;
    }

    public int getMaxIter() {
        return maxIter;
    }

    public void setMaxIter(int maxIter) {
        this.maxIter = maxIter;
    }

    public void setK(int k) {
        this.k = k;
    }

    public int getK() {
        return k;
    }

}
