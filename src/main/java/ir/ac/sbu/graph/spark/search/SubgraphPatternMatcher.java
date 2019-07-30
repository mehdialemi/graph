package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.SparkAppConf;
import ir.ac.sbu.graph.spark.search.fonl.creator.LabelTriangleFonl;
import ir.ac.sbu.graph.spark.search.fonl.creator.LocalFonlCreator;
import ir.ac.sbu.graph.spark.search.fonl.creator.TriangleFonl;
import ir.ac.sbu.graph.spark.search.fonl.local.QFonl;
import ir.ac.sbu.graph.spark.search.fonl.local.SubQuery;
import ir.ac.sbu.graph.spark.search.fonl.value.LabelDegreeTriangleFonlValue;
import ir.ac.sbu.graph.spark.search.patterns.PatternReaderUtils;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SubgraphPatternMatcher extends SparkApp {
    private SearchConfig searchConfig;
    private SparkAppConf sparkConf;
    private EdgeLoader edgeLoader;
    private JavaPairRDD<Integer, String> labels;

    public SubgraphPatternMatcher(SearchConfig searchConfig, SparkAppConf sparkConf, EdgeLoader edgeLoader,
                                  JavaPairRDD <Integer, String> labels) {
        super(edgeLoader);
        this.searchConfig = searchConfig;
        this.sparkConf = sparkConf;
        this.edgeLoader = edgeLoader;
        this.labels = labels;
    }

    public long search(QFonl qFonl) {
        List<SubQuery> subQueries = LocalFonlCreator.getSubQueries(qFonl);
        if (subQueries.isEmpty())
            return 0;
        Queue<SubQuery> queue = new LinkedList<>(subQueries);

        NeighborList neighborList = new NeighborList(edgeLoader);
        TriangleFonl triangleFonl = new TriangleFonl(neighborList);
        LabelTriangleFonl labelTriangleFonl = new LabelTriangleFonl(triangleFonl, labels);
        JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> labelTrianlgeRDD = labelTriangleFonl.create();


    }

    public static void main(String[] args) throws FileNotFoundException {
        SearchConfig searchConfig = SearchConfig.load(args[0]);
        SparkAppConf sparkConf = searchConfig.getSparkAppConf();
        sparkConf.init();

        QFonl qFonl = PatternReaderUtils.loadSample(searchConfig.getSampleName());
        System.out.println("qFonl" + qFonl);

        EdgeLoader edgeLoader = new EdgeLoader(sparkConf);

        JavaPairRDD <Integer, String> labels = PatternCounter.getLabels(sparkConf.getSc(),
                searchConfig.getGraphLabelPath(), sparkConf.getPartitionNum());

    }
}
