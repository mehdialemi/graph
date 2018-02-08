package ir.ac.sbu.graph.spark.clusteringco;

import ir.ac.sbu.graph.spark.*;
import ir.ac.sbu.graph.spark.triangle.Triangle;

import static ir.ac.sbu.graph.utils.Log.log;

public class GlobalCC extends SparkApp {

    private Triangle triangle;

    public GlobalCC(Triangle triangle) {
        super(triangle);
        this.triangle = triangle;
    }

    public float getGCC() {
        long tc = triangle.triangleCount();
        long nodes = triangle.getOrCreateFonl().count();
        return tc / (float) (nodes * (nodes - 1));
    }

    public static void main(String[] args) {

        long t1 = System.currentTimeMillis();
        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args));
        conf.init();

        GlobalCC gcc = new GlobalCC(new Triangle(new NeighborList(new EdgeLoader(conf))));
        long t2 = System.currentTimeMillis();
        log("GCC: " + gcc.getGCC(), t1, t2);

        gcc.close();
    }
}
