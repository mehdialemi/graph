package ir.ac.sbu.graph.utils;

import ir.ac.sbu.graph.spark.*;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class EdgeListUndirector extends SparkApp  {

    private NeighborList neighborList;

    public EdgeListUndirector(NeighborList neighborList) {
        super(neighborList);
        this.neighborList = neighborList;
    }

    public void generate() {
        String outPath = conf.getInputPath() + ".edges";
        System.out.println("Deleting " + outPath);
        FileUtils.deleteQuietly(new File(outPath));
        System.out.println("New vertex edge list file in " + outPath);

        JavaPairRDD<Integer, int[]> neighbors = neighborList.getOrCreate();
        JavaRDD<String> edges = neighbors.flatMap(kv -> {
            List<String> out = new ArrayList<>();
            int v1 = kv._1.intValue();
            for (int v2 : kv._2) {
                out.add(v1 + "\t" + v2);
            }
            return out.iterator();
        });

        edges.saveAsTextFile(outPath);
        conf.getSc().close();
    }

    public static void main(String[] args) {
        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args));
        conf.init();
        EdgeListUndirector eToV = new EdgeListUndirector(new NeighborList(new EdgeLoader(conf)));
        eToV.generate();
    }
}
