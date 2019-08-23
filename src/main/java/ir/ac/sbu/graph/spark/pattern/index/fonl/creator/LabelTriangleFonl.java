package ir.ac.sbu.graph.spark.pattern.index.fonl.creator;

import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.pattern.PatternConfig;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleFonlValue;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleMeta;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.TriangleFonlValue;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class LabelTriangleFonl {
    private static final Logger logger = LoggerFactory.getLogger(LabelTriangleFonl.class);
    private PatternConfig config;

    public LabelTriangleFonl(PatternConfig config) {
        this.config = config;
    }

    public JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> create(NeighborList neighborList, TriangleFonl triangleFonl) {
        JavaPairRDD<Integer, TriangleFonlValue> triangleFonlRDD = triangleFonl.create(neighborList);

        // make an RDD containing degree and labels of each vertex
        JavaPairRDD<Integer, int[]> neighborRDD = neighborList.getOrCreate();
        JavaPairRDD<Integer, String> labelRDD = loadLabels(neighborRDD);

        JavaPairRDD<Integer, Iterable<Tuple3<Integer, Integer, String>>> degreeLabelMessage = neighborRDD
                .leftOuterJoin(labelRDD)
                .mapValues(v -> new Tuple2<>(v._1, v._2.or("_")))
                .flatMapToPair(kv -> {

                    List<Tuple2<Integer, Tuple3<Integer, Integer, String>>> out = new ArrayList<>();
                    Tuple3<Integer, Integer, String> neighborDegreeLabel = new Tuple3<>(kv._1, kv._2._1.length, kv._2._2);
                    for (int neighbor : kv._2._1) {
                        out.add(new Tuple2<>(neighbor, neighborDegreeLabel));
                    }
                    // add itself
                    out.add(new Tuple2<>(kv._1, neighborDegreeLabel));
                    return out.iterator();
                }).groupByKey(config.getPartitionNum());

        JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> ldtFonlRDD =
                triangleFonlRDD
                        .join(degreeLabelMessage, config.getPartitionNum())
                        .mapToPair(kv -> {
                            TriangleFonlValue triangleFonlValue = kv._2._1;
                            Int2IntSortedMap v2Index = new Int2IntAVLTreeMap();
                            for (int i = 0; i < triangleFonlValue.fonl.length; i++) {
                                v2Index.put(triangleFonlValue.fonl[i], i);
                            }

                            LabelDegreeTriangleMeta meta =
                                    new LabelDegreeTriangleMeta(triangleFonlValue.meta, triangleFonlValue.fonl.length);

                            for (Tuple3<Integer, Integer, String> neighborDegreeLabel : kv._2._2) {
                                if (neighborDegreeLabel._1().equals(kv._1)) {
                                    meta.setLabel(neighborDegreeLabel._3());
                                } else {
                                    int index = v2Index.getOrDefault(neighborDegreeLabel._1(), -1);
                                    if (index >= 0) {
                                        meta.setLabelDegree(index, neighborDegreeLabel._3(), neighborDegreeLabel._2());
                                    }
                                }
                            }

                            LabelDegreeTriangleFonlValue value =
                                    new LabelDegreeTriangleFonlValue(kv._1, triangleFonlValue.fonl, meta);

                            return new Tuple2<>(kv._1, value);
                        })
                        .repartition(config.getPartitionNum())
                        .persist(config.getSparkAppConf().getStorageLevel());

        return ldtFonlRDD;
    }

    private JavaPairRDD<Integer, String> loadLabels(JavaPairRDD<Integer, int[]> neighborRDD) {
        if (config.getGraphLabelPath() == null || config.getGraphLabelPath().equals("") ||
                config.getGraphLabelPath().isEmpty()) {
            logger.info("Loading default label for all vertices");
            return neighborRDD.mapValues(v -> "_")
                    .persist(config.getSparkAppConf().getStorageLevel());
        }

        logger.info("Loading labels from hdfs");
        return config.getSparkAppConf().getSc()
                .textFile(config.getGraphLabelPath(), config.getPartitionNum())
                .filter(line -> !line.startsWith("#"))
                .map(line -> line.split("\\s+"))
                .mapToPair(split -> new Tuple2<>(Integer.parseInt(split[0]), split[1]))
                .persist(config.getSparkAppConf().getStorageLevel());
    }
}
