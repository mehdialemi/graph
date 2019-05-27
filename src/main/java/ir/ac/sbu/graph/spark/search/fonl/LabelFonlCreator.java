package ir.ac.sbu.graph.spark.search.fonl;

import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.search.VLabelDeg;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LabelFonlCreator {

    public final static String EMPTY_LABEL = "_";

    public static JavaPairRDD <Integer, LabledFonlValue> createLabeledFonl(NeighborList neighborList,
                                                                           JavaPairRDD <Integer, String> labels) {
        JavaPairRDD <Integer, int[]> neighbors = neighborList.getOrCreate();

        JavaPairRDD <Integer, VLabelDeg> labelDegMsg = neighbors
                .leftOuterJoin(labels)
                .flatMapToPair(kv -> {
                    int deg = kv._2._1.length;
                    if (deg == 0)
                        return Collections.emptyIterator();

                    VLabelDeg vLabelDeg = new VLabelDeg(kv._1, kv._2._2.orElse(EMPTY_LABEL), deg);
                    List <Tuple2 <Integer, VLabelDeg>> list = new ArrayList <>(deg);

                    for (int neighbor : kv._2._1) {
                        list.add(new Tuple2 <>(neighbor, vLabelDeg));
                    }

                    return list.iterator();
                });

        JavaPairRDD <Integer, LabledFonlValue> fonlLabels = labelDegMsg.groupByKey(neighbors.getNumPartitions()).mapToPair(kv -> {
            int degree = 0;

            for (VLabelDeg vLabelDeg : kv._2) {
                degree++;
            }

            List <VLabelDeg> list = new ArrayList <>();
            for (VLabelDeg vd : kv._2) {
                if (vd.degree > degree || (vd.degree == degree && vd.vertex > kv._1)) {
                    list.add(vd);
                }
            }

            list.sort((a, b) -> {
                int x, y;
                if (a.degree != b.degree) {
                    x = a.degree;
                    y = b.degree;
                } else {
                    x = a.vertex;
                    y = b.vertex;
                }
                return x - y;
            });

            LabledFonlValue value = new LabledFonlValue(degree, list);
            return new Tuple2 <>(kv._1, value);
        }).cache();

        return fonlLabels.leftOuterJoin(labels).mapValues(v -> {
            v._1.meta.label = v._2.orElse(EMPTY_LABEL);
            return v._1;
        });
    }
}
