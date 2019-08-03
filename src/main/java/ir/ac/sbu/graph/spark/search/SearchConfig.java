package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.spark.SparkAppConf;
import ir.ac.sbu.graph.spark.search.fonl.creator.VLabel;
import ir.ac.sbu.graph.spark.search.fonl.creator.VLabelDeg;
import ir.ac.sbu.graph.spark.search.fonl.local.Subquery;
import ir.ac.sbu.graph.spark.search.fonl.value.*;
import ir.ac.sbu.graph.types.*;
import it.unimi.dsi.fastutil.Function;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class SearchConfig {
    private String graphPath;
    private String graphLabelPath;
    private String sampleName;
    private String master;
    private int partitionNum;
    private int cores;
    private boolean single;

    private SearchConfig(){}

    public static SearchConfig load(String file) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        return yaml.loadAs(new FileReader(file), SearchConfig.class);
    }

    public SparkAppConf getSparkAppConf() {

        return new SparkAppConf() {

            @Override
            public void init() {
                graphInputPath = SearchConfig.this.graphPath;
                cores = SearchConfig.this.cores;
                partitionNum = SearchConfig.this.partitionNum;
                master = SearchConfig.this.master;

                sparkConf = new SparkConf();
                sparkConf.setMaster(master);
                sparkConf.setAppName("PatternCounter");
                sparkConf.set("spark.driver.memory", "10g");
                sparkConf.set("spark.driver.maxResultSize", "9g");
                sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                sparkConf.registerKryoClasses(new Class[] {
                        int[].class,
                        LabelFonlValue.class,
                        FonlValue.class,
                        LabelMeta.class,
                        DegreeMeta.class,
                        Meta.class,
                        LabelTriangleFonlValue.class,
                        TriangleDegreeMeta.class,
                        Subquery.class,
                        Int2IntMap.class,
                        Int2IntFunction.class,
                        Map.class,
                        HashMap.class,
                        Function.class,
                        int[][].class,
                        VLabel.class,
                        VLabelDeg.class,
                        Edge.class,
                        VertexDeg.class,
                        List.class,
                        byte[].class,
                        IntCollection.class,
                        Set.class
                });

                sc = new JavaSparkContext(sparkConf);
            }
        };

    }

    public String getGraphPath() {
        return graphPath;
    }

    public void setGraphPath(String graphPath) {
        this.graphPath = graphPath;
    }

    public String getGraphLabelPath() {
        return graphLabelPath;
    }

    public void setGraphLabelPath(String graphLabelPath) {
        this.graphLabelPath = graphLabelPath;
    }

    public String getSampleName() {
        return sampleName;
    }

    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public int getCores() {
        return cores;
    }

    public void setCores(int cores) {
        this.cores = cores;
    }

    public boolean isSingle() {
        return single;
    }

    public void setSingle(boolean single) {
        this.single = single;
    }
}
