package ir.ac.sbu.graph.spark.pattern.index;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.pattern.PatternConfig;
import ir.ac.sbu.graph.spark.pattern.index.fonl.creator.LabelTriangleFonl;
import ir.ac.sbu.graph.spark.pattern.index.fonl.creator.TriangleFonl;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleFonlValue;
import org.apache.avro.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;

public class GraphIndex {

    private PatternConfig config;

    public GraphIndex(PatternConfig config) {
        this.config = config;
    }

    public void constructIndex() {
        EdgeLoader edgeLoader = new EdgeLoader(config.getSparkAppConf());
        NeighborList neighborList = new NeighborList(edgeLoader);
        TriangleFonl triangleFonl = new TriangleFonl(neighborList);
        JavaPairRDD <Integer, String> labels = getLabels(neighborList);

        LabelTriangleFonl labelTriangleFonl = new LabelTriangleFonl(triangleFonl, labels);
        JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> ldtFonlRDD = labelTriangleFonl.create();

        ldtFonlRDD.saveAsHadoopFile(config.getIndexPath(), Integer.class, LabelDegreeTriangleFonlValue.class,
                SequenceFileOutputFormat.class);
    }

    private JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> indexRDD;

    public JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> indexRDD() {
        if (indexRDD == null) {
            indexRDD = config.getSparkAppConf()
                    .getSc()
                    .hadoopFile(config.getIndexPath(), SequenceFileInputFormat.class,
                            Integer.class, LabelDegreeTriangleFonlValue.class)
                    .repartition(config.getPartitionNum())
                    .persist(StorageLevel.MEMORY_AND_DISK());
        }

        return indexRDD;
    }

    private JavaPairRDD <Integer, String> getLabels(NeighborList neighborList) {
        if (config.getGraphLabelPath().isEmpty()) {
            return neighborList.getOrCreate().mapValues(v -> "_");
        }

        return config.getSparkAppConf().getSc()
                .textFile(config.getGraphLabelPath(), neighborList.getOrCreate().getNumPartitions())
                .map(line -> line.split("\\s+"))
                .mapToPair(split -> new Tuple2 <>(Integer.parseInt(split[0]), split[1]));
    }

    public static void main(String[] args) {
        Config conf = ConfigFactory.load();
        if (args.length > 0)
            conf = ConfigFactory.parseFile(new File(args[0]));

        PatternConfig config = new PatternConfig(conf);
        System.out.println("loading config .... ");
        System.out.println(config);

        GraphIndex graphIndex = new GraphIndex(config);
        graphIndex.constructIndex();
        System.out.println("graph index is constructed successfully, index path: " + config.getIndexPath());
    }
}
