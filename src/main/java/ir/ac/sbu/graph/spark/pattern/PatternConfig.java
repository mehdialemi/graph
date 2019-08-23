package ir.ac.sbu.graph.spark.pattern;

import com.typesafe.config.Config;
import ir.ac.sbu.graph.spark.SparkAppConf;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.*;
import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.VertexDeg;
import it.unimi.dsi.fastutil.Function;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private final String hdfsMaster;

    private final SparkAppConf sparkAppConf;
    private final Configuration hadoopConf;

    public PatternConfig(Config conf) {
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
                sparkConf.setAppName("QueryMatcher[" + PatternConfig.this.targetGraph + "]");
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

    public String getHdfsMaster() {
        return hdfsMaster;
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
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
                "driverMemoryGB: " + driverMemoryGB + "\n" +
                "hdfsMaster: " + hdfsMaster + "\n";
    }

}
