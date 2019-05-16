package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.FonlCreator;
import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.fonl.LabelMeta;
import ir.ac.sbu.graph.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

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
        List <Tuple2 <Integer, Fvalue <LabelMeta>>> collect = labelFonl.collect();
        for (Tuple2 <Integer, Fvalue <LabelMeta>> t : collect) {
            System.out.println(t);
        }
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
