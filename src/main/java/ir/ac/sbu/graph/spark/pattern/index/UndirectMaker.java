package ir.ac.sbu.graph.spark.pattern.index;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.pattern.PatternConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UndirectMaker {
    private static final Logger logger = LoggerFactory.getLogger(UndirectMaker.class);
    private PatternConfig config;

    public UndirectMaker(PatternConfig config) {

        this.config = config;
    }

    public void createUndirectedFile() throws IOException {

        String uPath = config.getInputDir() + "u_" + config.getTargetGraph();
        FileSystem fileSystem = FileSystem.newInstance(config.getHadoopConf());
        fileSystem.delete(new Path(config.getLabelPath()), true);

        EdgeLoader edgeLoader = new EdgeLoader(config.getSparkAppConf());
        JavaPairRDD<Integer, Integer> edges = edgeLoader.create();
        NeighborList neighborList = new NeighborList(config.getSparkAppConf());
        JavaPairRDD<Integer, int[]> neighbors = neighborList.createNeighbors(edges);
        logger.info("(SBM) vertex count: {}", neighbors.count());

        neighbors.flatMapToPair(kv -> {
            List<Tuple2<Integer, Integer>> out = new ArrayList<>();
            for (int v : kv._2) {
                out.add(new Tuple2<>(kv._1, v));
            }
            return out.iterator();
        }).saveAsTextFile(uPath);
    }

    public static void main(String[] args) throws IOException {
        Config conf = ConfigFactory.load();
        if (args.length > 0)
            conf = ConfigFactory.parseFile(new File(args[0]));

        PatternConfig config = new PatternConfig(conf, "UndirectMaker");
        UndirectMaker undirectMaker = new UndirectMaker(config);
        undirectMaker.createUndirectedFile();

        config.getSparkContext().close();
    }
}
