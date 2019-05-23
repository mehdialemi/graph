package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.*;
import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TSupMatcher extends SparkApp {

    private FSearchConf conf;
    private EdgeLoader edgeLoader;
    private JavaPairRDD<Integer, String> labels;

    public TSupMatcher(FSearchConf conf, EdgeLoader edgeLoader, JavaPairRDD <Integer, String> labels) {
        super(edgeLoader);
        this.conf = conf;
        this.edgeLoader = edgeLoader;
        this.labels = labels;
    }

    public void search(LocalTriangle localTriangle) {

        NeighborList neighborList = new NeighborList(edgeLoader);
        JavaPairRDD <Integer, Fvalue <LabelMeta>> lFonl = SparkFonlCreator.createLabelFonl(neighborList, labels);
        printFonl(lFonl);

        JavaPairRDD <Integer, Fvalue <TriangleMeta>> triangleLFonl = SparkFonlCreator.createLabelTriangles(lFonl);
        Broadcast <LocalTriangle> broadcast = conf.getSc().broadcast(localTriangle);

        triangleLFonl.flatMapToPair(kv -> {
            LocalTriangle qTriangle = broadcast.getValue();
            String label = kv._2.meta.label;
            int deg = kv._2.meta.deg;
            int[] degs = kv._2.meta.degs;
            int[] fonl = kv._2.fonl;

            for (int i = 0; i < qTriangle.vArray.length; i++) {
                if (!label.equals(qTriangle.labels[i]) ||  deg < qTriangle.dIndex[i])
                    continue;

                if (qTriangle.vArray[i] == null)
                    continue;

                int[] nArray = qTriangle.nArray[i];
                int[] vArray = qTriangle.vArray[i];
                for (int k = 0; k < fonl.length; k++) {
                    for (int w = 0; w < fonl.length; w++) {

                    }
                }
                for (int j = 0; j < qTriangle.nArray[i].length; j++) {
                    int vIdx = qTriangle.vArray[i][j];
                    int vDeg = qTriangle.dIndex[vIdx];


                }
            }
            for (int i = 0; i < qTriangle.labels.length; i++) {

            }
        })
    }

    public static void main(String[] args) {
        FSearchConf conf = new FSearchConf(new ArgumentReader(args));
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        JavaPairRDD <Integer, String> labels = getLables(conf.getSc(), conf.getLablePath(), conf.getPartitionNum());

        TSupMatcher matcher = new TSupMatcher(conf, edgeLoader, labels);

        Map<Integer, List<Integer>> neighbors = new HashMap<>();
        neighbors.put(1, Arrays.asList(4, 2, 3));
        neighbors.put(2, Arrays.asList(3, 1));
        neighbors.put(3, Arrays.asList(2, 1));
        neighbors.put(4, Arrays.asList(1));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "B");
        labelMap.put(3, "C");
        labelMap.put(4, "A");

        LocalTriangle localTriangle = LocalFonlCreator.createTriangle(neighbors, labelMap);
        matcher.search(localTriangle);

        matcher.close();
    }

    private static JavaPairRDD <Integer, String> getLables(JavaSparkContext sc, String path, int pNum) {
        return sc
                .textFile(path, pNum)
                .map(line -> line.split("\\s+"))
                .mapToPair(split -> new Tuple2<>(Integer.parseInt(split[0]), split[1]));
    }

    private void printFonl(JavaPairRDD <Integer, Fvalue<LabelMeta>> labelFonl) {
        List <Tuple2 <Integer, Fvalue <LabelMeta>>> collect = labelFonl.collect();
        for (Tuple2 <Integer, Fvalue <LabelMeta>> t : collect) {
            System.out.println(t);
        }
    }
}
