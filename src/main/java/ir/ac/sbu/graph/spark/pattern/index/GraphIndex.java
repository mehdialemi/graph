package ir.ac.sbu.graph.spark.pattern.index;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.pattern.PatternConfig;
import ir.ac.sbu.graph.spark.pattern.index.fonl.creator.LabelTriangleFonl;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleFonlValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;

public class GraphIndex {
    private static final Logger logger = LoggerFactory.getLogger(GraphIndex.class);

    private PatternConfig config;

    public GraphIndex(PatternConfig config) {
        this.config = config;
    }

    public void constructIndex() throws IOException {
        FileSystem fileSystem = FileSystem.newInstance(config.getHadoopConf());
        fileSystem.delete(new Path(config.getIndexPath()));

        EdgeLoader edgeLoader = new EdgeLoader(config.getSparkAppConf());
        JavaPairRDD<Integer, Integer> edges = edgeLoader.create();

        NeighborList neighborList = new NeighborList(config.getSparkAppConf());
        JavaPairRDD<Integer, int[]> neighbors = neighborList.createNeighbors(edges);
        logger.info("vertex count: {}", neighbors.count());

        LabelTriangleFonl labelTriangleFonl = new LabelTriangleFonl(config);
        JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> ldtFonlRDD =
                labelTriangleFonl.create(neighbors);

        ldtFonlRDD.saveAsObjectFile(config.getIndexPath());
    }

    private JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> indexRDD;

    public JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> indexRDD() {
        logger.info("loading index from hdfs");
        if (indexRDD == null) {
            JavaRDD <Object> file = config.getSparkAppConf()
                    .getSc()
                    .objectFile(config.getIndexPath());

            indexRDD = file.mapToPair(o -> (Tuple2 <Integer, LabelDegreeTriangleFonlValue>) o)
                    .repartition(config.getPartitionNum())
                    .persist(config.getSparkAppConf().getStorageLevel());
        }

        logger.info("index row count: {}", indexRDD.count());
        return indexRDD;
    }

    public static void main(String[] args) throws IOException {
        Config conf = ConfigFactory.load();
        if (args.length > 0)
            conf = ConfigFactory.parseFile(new File(args[0]));

        PatternConfig config = new PatternConfig(conf, "index");
        GraphIndex graphIndex = new GraphIndex(config);
        graphIndex.constructIndex();
        logger.info("graph index is constructed successfully, index path: {}", config.getIndexPath());
    }
}
