package ir.ac.sbu.graph.spark.pattern.label;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.pattern.PatternConfig;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class LabelManager {
    private static final Logger logger = LoggerFactory.getLogger(LabelManager.class);

    private PatternConfig config;

    public LabelManager(PatternConfig config) {

        this.config = config;
    }

    public void createRandomLabel() throws IOException {
        if (config.isUseDefaultLabel()) {
            logger.warn("Creating random label is disabled, defaultLabel has been set to false");
            return;
        }

        FileSystem fileSystem = FileSystem.newInstance(config.getHadoopConf());
        fileSystem.delete(new Path(config.getLabelPath()), true);

        logger.info("Creating random labels for graph: {} in path: {}",
                config.getTargetGraph(), config.getLabelPath());

        List<String> labels = config.getLabels();
        Broadcast<List<String>> broadcast = config.getSparkContext().broadcast(labels);

        EdgeLoader edgeLoader = new EdgeLoader(config.getSparkAppConf());
        NeighborList neighborList = new NeighborList(edgeLoader);
        JavaPairRDD<Integer, int[]> neighbors = neighborList.getOrCreate();
        logger.info("Creating random label for vertex count: {}", neighbors.count());

        neighbors.map(kv -> kv._1 + " " +
                broadcast.getValue().get(RandomUtils.nextInt(0, broadcast.getValue().size())
                ))
                .saveAsTextFile(config.getLabelPath());

    }

    public JavaPairRDD<Integer, String> loadLabels(JavaPairRDD<Integer, int[]> neighborRDD) {
        if (config.isUseDefaultLabel()) {
            logger.info("(SBM) Loading default label for all vertices");
            return neighborRDD.mapValues(v -> "_")
                    .persist(config.getSparkAppConf().getStorageLevel());
        }

        logger.info("(SBM) Loading labels from hdfs");
        return config.getSparkAppConf().getJavaSparkContext()
                .wholeTextFiles(config.getLabelPath(), config.getPartitionNum())
                .map(kv -> kv._2)
                .filter(line -> !line.startsWith("#"))
                .map(line -> line.split("\\s+"))
                .mapToPair(split -> new Tuple2<>(Integer.parseInt(split[0]), split[1]))
                .persist(config.getSparkAppConf().getStorageLevel());
    }

    public static void main(String[] args) throws IOException {
        Config conf = ConfigFactory.load();
        if (args.length > 0)
            conf = ConfigFactory.parseFile(new File(args[0]));

        PatternConfig config = new PatternConfig(conf, "label");
        LabelManager labelManager = new LabelManager(config);
        labelManager.createRandomLabel();

        config.getSparkContext().close();
    }
}
