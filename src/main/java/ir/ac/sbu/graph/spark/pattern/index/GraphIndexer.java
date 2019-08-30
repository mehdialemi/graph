package ir.ac.sbu.graph.spark.pattern.index;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.pattern.PatternConfig;
import ir.ac.sbu.graph.spark.pattern.index.fonl.creator.LabelTriangleFonl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class GraphIndexer {
    private static final Logger logger = LoggerFactory.getLogger(GraphIndexer.class);

    private PatternConfig config;

    public GraphIndexer(PatternConfig config) {
        this.config = config;
    }

    private void constructIndex() throws IOException {
        FileSystem fileSystem = FileSystem.newInstance(config.getHadoopConf());
        fileSystem.delete(new Path(config.getIndexPath()), true);

        EdgeLoader edgeLoader = new EdgeLoader(config.getSparkAppConf());
        JavaPairRDD<Integer, Integer> edges = edgeLoader.create();

        NeighborList neighborList = new NeighborList(config.getSparkAppConf());
        JavaPairRDD<Integer, int[]> neighbors = neighborList.createNeighbors(edges);
        logger.info("vertex count: {}", neighbors.count());

        LabelTriangleFonl labelTriangleFonl = new LabelTriangleFonl(config);
        JavaRDD<IndexRow> indexRows = labelTriangleFonl.create(neighbors);

        SQLContext sqlContext = config.getSparkAppConf().getSqlContext();
        Encoder<IndexRow> indexRowEncoder = Encoders.bean(IndexRow.class);
        Dataset<IndexRow> dataset = sqlContext.createDataset(indexRows.rdd(), indexRowEncoder);
        dataset.write().parquet(config.getIndexPath());
    }

    private JavaPairRDD <Integer, IndexRow> indexRDD;

    public JavaPairRDD <Integer, IndexRow> getIndex() {
        logger.info("loading index from hdfs");
        if (indexRDD == null) {

            Encoder<IndexRow> indexRowEncoder = Encoders.bean(IndexRow.class);
            SQLContext sqlContext = config.getSparkAppConf().getSqlContext();

            indexRDD = sqlContext.read()
                    .parquet(config.getIndexPath())
                    .as(indexRowEncoder)
                    .toJavaRDD()
                    .mapToPair(IndexRow::toTuple)
                    .repartition(config.getPartitionNum())
                    .persist(config.getSparkAppConf().getStorageLevel());
        }

        return indexRDD;
    }

    public static void main(String[] args) throws IOException {
        Config conf = ConfigFactory.load();
        if (args.length > 0)
            conf = ConfigFactory.parseFile(new File(args[0]));

        PatternConfig config = new PatternConfig(conf, "index");
        GraphIndexer graphIndexer = new GraphIndexer(config);
        graphIndexer.constructIndex();
        logger.info("graph index is constructed successfully, index path: {}", config.getIndexPath());
    }
}
