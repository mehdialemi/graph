package ir.ac.sbu.graph.spark;

/**
 * All spark applications inherit this class
 */
public class SparkApp {

    protected final SparkAppConf conf;

    public SparkApp(SparkAppConf conf) {
        this.conf = conf;
    }

    public SparkApp(SparkApp sparkApp) {
        this.conf = sparkApp.conf;
    }

    public void close() {
        this.conf.getSc().close();
    }
}
