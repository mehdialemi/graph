package ir.ac.sbu.graph.spark.pattern;

import com.typesafe.config.Config;
import ir.ac.sbu.graph.spark.SparkAppConf;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.*;
import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import ir.ac.sbu.graph.types.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

public class PatternConfig {
    private static final Logger logger = LoggerFactory.getLogger(PatternConfig.class);

    private final String inputDir;
    private final String targetGraph;
    private final String targetLabel;

    private final String querySample;

    private final String indexDir;
    private final String indexName;

    private final String sparkMaster;
    private final int partitionNum;
    private final int cores;
    private final int driverMemoryGB;
    private String app;

    private final String hdfsMaster;

    private final SparkAppConf sparkAppConf;
    private final Configuration hadoopConf;

    public PatternConfig(Config conf, String app) {
        this.app = app;
        inputDir = conf.getString("inputDir");
        targetGraph = conf.getString("targetGraph");
        targetLabel = conf.getString("targetLabel");

        querySample = conf.getString("querySample");

        indexDir = conf.getString("indexDir");
        indexName = conf.getString("indexName");

        sparkMaster = conf.getString("sparkMaster");
        partitionNum = conf.getInt("partitionNum");
        cores = conf.getInt("cores");
        driverMemoryGB = conf.getInt("driverMemoryGB");

        sparkAppConf = createSparkAppConf();
        sparkAppConf.init();

        hdfsMaster = conf.getString("hdfsMaster");
        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", hdfsMaster);
        hadoopConf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");

        logger.info("****************** Config Properties ******************");
        logger.info(toString());
        logger.info("*******************************************************");
    }

    private SparkAppConf createSparkAppConf() {

        return new SparkAppConf() {

            @Override
            public void init() {

                sparkConf = new SparkConf();
                graphInputPath = PatternConfig.this.inputDir + PatternConfig.this.targetGraph;
                partitionNum = PatternConfig.this.partitionNum;
                cores = PatternConfig.this.cores;
                sparkConf.setMaster(PatternConfig.this.sparkMaster);
                sparkConf.setAppName("pattern-" + app + "[" + PatternConfig.this.targetGraph + "]");
                sparkConf.set("spark.driver.memory", PatternConfig.this.driverMemoryGB + "g");
                sparkConf.set("spark.driver.maxResultSize", PatternConfig.this.driverMemoryGB + "g");
                sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                sparkConf.registerKryoClasses(new Class[]{
                        FonlValue.class,
                        LabelDegreeTriangleFonlValue.class,
                        TriangleFonlValue.class,
                        Meta.class,
                        LabelDegreeTriangleMeta.class,
                        TriangleMeta.class,
                        Edge.class,
                        List.class,
                        Iterable.class,
                        long[].class,
                        int[].class,
                        String[].class,
                        Subquery.class,
                        Tuple2[].class,
                });

                sc = new JavaSparkContext(sparkConf);
            }
        };
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }

    public SparkAppConf getSparkAppConf() {
        return sparkAppConf;
    }

    public String getInputDir() {
        return inputDir;
    }

    public String getTargetGraph() {
        return targetGraph;
    }

    public String getGraphLabelPath() {
        if ("".equals(targetLabel))
            return "";

        return inputDir + targetLabel;
    }

    public String getTargetLabel() {
        return targetLabel;
    }

    public String getQuerySample() {
        return querySample;
    }

    public String getIndexPath() {
        return indexDir + indexName;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public int getCores() {
        return cores;
    }

    public int getDriverMemoryGB() {
        return driverMemoryGB;
    }

    @Override
    public String toString() {

        return  "inputDir: " + inputDir + "\n" +
                "targetGraph: " + targetGraph + "\n" +
                "targetLabel: " + targetLabel + "\n" +
                "querySample: " + querySample + "\n" +
                "indexDir: " + indexDir + "\n" +
                "indexName: " + indexName + "\n" +
                "sparkMaster: " + sparkMaster + "\n" +
                "partitionNum: " + partitionNum + "\n" +
                "cores: " + cores + "\n" +
                "driverMemoryGB: " + driverMemoryGB + "\n";
//                "hdfsMaster: " + hdfsMaster + "\n";
    }

}
