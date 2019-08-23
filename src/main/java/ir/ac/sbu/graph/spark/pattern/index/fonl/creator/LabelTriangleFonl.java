package ir.ac.sbu.graph.spark.pattern.index.fonl.creator;

import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkAppConf;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleFonlValue;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleMeta;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.TriangleFonlValue;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class LabelTriangleFonl {

    private final TriangleFonl triangleFonl;
    private final SparkAppConf conf;
    private final JavaPairRDD <Integer, String> labelRDD;

    public LabelTriangleFonl(TriangleFonl triangleFonl, JavaPairRDD <Integer, String> labelRDD) {
        this.conf = triangleFonl.getConf();
        this.triangleFonl = triangleFonl;
        this.labelRDD = labelRDD;
    }

    public JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> create(NeighborList neighborList) {
        JavaPairRDD <Integer, TriangleFonlValue> triangleFonlRDD = triangleFonl.create();

        // make an RDD containing degree and labels of each vertex
        JavaPairRDD <Integer, int[]> neighborRDD = neighborList.getOrCreate();
        JavaPairRDD <Integer, Iterable <Tuple3 <Integer, Integer, String>>> degreeLabelMessage = neighborRDD
                .leftOuterJoin(labelRDD)
                .mapValues(v -> new Tuple2 <>(v._1, v._2.or("_")))
                .flatMapToPair(kv -> {

                    List <Tuple2 <Integer, Tuple3 <Integer, Integer, String>>> out = new ArrayList <>();
                    Tuple3 <Integer, Integer, String> neighborDegreeLabel = new Tuple3 <>(kv._1, kv._2._1.length, kv._2._2);
                    for (int neighbor : kv._2._1) {
                        out.add(new Tuple2 <>(neighbor, neighborDegreeLabel));
                    }
                    // add itself
                    out.add(new Tuple2 <>(kv._1, neighborDegreeLabel));
                    return out.iterator();
                }).groupByKey(conf.getPartitionNum());
        ;

        // broadcast vertex label and degree to its fonl neighbors


        JavaPairRDD <Integer, LabelDegreeTriangleFonlValue> ldtFonlRDD =
                triangleFonlRDD
                        .join(degreeLabelMessage, conf.getPartitionNum())
                        .mapToPair(kv -> {
                            TriangleFonlValue triangleFonlValue = kv._2._1;
                            Int2IntSortedMap v2Index = new Int2IntAVLTreeMap();
                            for (int i = 0; i < triangleFonlValue.fonl.length; i++) {
                                v2Index.put(triangleFonlValue.fonl[i], i);
                            }

                            LabelDegreeTriangleMeta meta =
                                    new LabelDegreeTriangleMeta(triangleFonlValue.meta, triangleFonlValue.fonl.length);

                            for (Tuple3 <Integer, Integer, String> neighborDegreeLabel : kv._2._2) {
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

                            return new Tuple2 <>(kv._1, value);
                        })
                        .repartition(conf.getPartitionNum())
                        .persist(conf.getStorageLevel());

        return ldtFonlRDD;
    }
}
