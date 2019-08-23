package ir.ac.sbu.graph.spark.pattern;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ir.ac.sbu.graph.spark.SparkAppConf;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.FonlValue;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.Meta;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.VertexDeg;
import it.unimi.dsi.fastutil.Function;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PatternConfig {

    private final String graphPath;
    private final String graphLabelPath;
    private final String querySample;
    private final String sparkMaster;
    private final int partitionNum;
    private final int cores;
    private final String indexPath;
    private final int driverMemoryGB;
    private final SparkAppConf sparkAppConf;

    public PatternConfig(Config conf) {
        graphPath = conf.getString("graphPath");
        graphLabelPath = conf.getString("graphLabelPath");
        querySample = conf.getString("querySample");
        sparkMaster = conf.getString("sparkMaster");
        partitionNum = conf.getInt("partitionNum");
        cores = conf.getInt("cores");
        driverMemoryGB = conf.getInt("driverMemoryGB");
        indexPath = conf.getString("indexPath");
        sparkAppConf = createSparkAppConf();
        sparkAppConf.init();
    }

    private SparkAppConf createSparkAppConf() {

        return new SparkAppConf() {

            @Override
            public void init() {

                sparkConf = new SparkConf();
                graphInputPath = PatternConfig.this.graphPath;
                partitionNum = PatternConfig.this.partitionNum;
                cores = PatternConfig.this.cores;
                sparkConf.setMaster(PatternConfig.this.sparkMaster);
                sparkConf.setAppName("QueryMatcher");
                sparkConf.set("spark.driver.memory", PatternConfig.this.driverMemoryGB + "g");
                sparkConf.set("spark.driver.maxResultSize", PatternConfig.this.driverMemoryGB + "g");
                sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                sparkConf.registerKryoClasses(new Class[]{
                        int[].class,
                        FonlValue.class,
                        Meta.class,
                        Int2IntMap.class,
                        Int2IntFunction.class,
                        Map.class,
                        HashMap.class,
                        Function.class,
                        int[][].class,
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

    public SparkAppConf getSparkAppConf() {
        return sparkAppConf;
    }

    public String getGraphPath() {
        return graphPath;
    }

    public String getGraphLabelPath() {
        return graphLabelPath;
    }

    public String getQuerySample() {
        return querySample;
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

    public String getIndexPath() {
        return indexPath;
    }

    public int getDriverMemoryGB() {
        return driverMemoryGB;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("graphPath: ").append(graphPath).append("\n");
        sb.append("graphLabelPath: ").append(graphLabelPath).append("\n");
        sb.append("querySample: ").append(querySample).append("\n");
        sb.append("sparkMaster: ").append(sparkMaster).append("\n");
        sb.append("partitionNum: ").append(partitionNum).append("\n");
        sb.append("cores: ").append(cores).append("\n");
        sb.append("indexPath: ").append(indexPath).append("\n");
        sb.append("driverMemoryGB: ").append(driverMemoryGB).append("\n");

        return sb.toString();
    }
}
