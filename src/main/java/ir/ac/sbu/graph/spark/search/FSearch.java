package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.FonlCreator;
import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.fonl.LabelMeta;
import ir.ac.sbu.graph.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class FSearch extends SparkApp {

    private FSearchConf conf;
    private EdgeLoader edgeLoader;

    public FSearch(FSearchConf conf, EdgeLoader edgeLoader) {
        super(edgeLoader);
        this.conf = conf;
        this.edgeLoader = edgeLoader;
    }

    public void search() {
        // load label
        JavaPairRDD <Integer, String> lables = conf.getSc()
                .textFile(conf.getLablePath(), conf.getPartitionNum())
                .map(line -> line.split("\\s+"))
                .mapToPair(split -> new Tuple2 <>(Integer.parseInt(split[0]), split[1]));

        JavaPairRDD <Integer, Fvalue <LabelMeta>> labelFonl = FonlCreator.createLabelFonl(
                new NeighborList(edgeLoader), lables);

        print(labelFonl);

        JavaPairRDD <Integer, Fvalue <LabelMeta>> nodes = startNodes(labelFonl);

        print(nodes);


    }

    private void print(JavaPairRDD <Integer, Fvalue <LabelMeta>> labelFonl ) {
        List <Tuple2 <Integer, Fvalue <LabelMeta>>> collect = labelFonl.collect();
        for (Tuple2 <Integer, Fvalue <LabelMeta>> t : collect) {
            System.out.println(t);
        }
    }

    private JavaPairRDD <Integer, Fvalue <LabelMeta>> startNodes(JavaPairRDD <Integer, Fvalue <LabelMeta>> labelFonl) {
        JavaPairRDD <Integer, Iterable <Integer>> neighborMsg = labelFonl.flatMapToPair(kv -> {
            List <Tuple2 <Integer, Integer>> list = new ArrayList <>();
            for (int v : kv._2.fonl) {
                list.add(new Tuple2 <>(v, kv._1));
            }
            return list.iterator();
        }).groupByKey(labelFonl.getNumPartitions());

        return labelFonl.leftOuterJoin(neighborMsg)
                .filter(kv -> !kv._2._2.isPresent())
                .mapValues(value -> value._1);
    }

    public static void main(String[] args) {
        long t1 = System.currentTimeMillis();
        FSearchConf conf = new FSearchConf(new ArgumentReader(args));
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        FSearch fSearch = new FSearch(conf, edgeLoader);


        fSearch.search();

        fSearch.close();
    }
}
