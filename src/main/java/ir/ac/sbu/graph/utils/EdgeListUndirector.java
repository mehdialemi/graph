package ir.ac.sbu.graph.utils;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.SparkAppConf;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;

/**
 *
 */
public class EdgeListUndirector extends SparkApp  {

    public EdgeListUndirector(SparkAppConf conf) {
        super(conf);
    }

    public void generate() throws FileNotFoundException {
        JavaRDD<String> input = conf.getSc().textFile(conf.getInputPath());
        JavaRDD<String> edges = input.filter(l -> !l.startsWith("#"))
                .map(s -> s.split("\\s+"))
                .flatMap(array -> Arrays.asList(array[0] + "\t" + array[1], array[1] + "\t" + array[0]).iterator());

        String outPath = conf.getInputPath() + ".edges";
        System.out.println("Deleting " + outPath);
        FileUtils.deleteQuietly(new File(outPath));
        System.out.println("New vertex edge list file in " + outPath);
        PrintWriter pw = new PrintWriter(new File(outPath));
        for (String edge : edges.collect()) {
            pw.println(edge);
            pw.flush();
        }
        pw.flush();
    }

    public static void main(String[] args) throws FileNotFoundException {
        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args));
        conf.init();
        EdgeListUndirector eToV = new EdgeListUndirector(conf);
        eToV.generate();
    }
}
