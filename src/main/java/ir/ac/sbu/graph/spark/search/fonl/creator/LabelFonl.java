package ir.ac.sbu.graph.spark.search.fonl.creator;

import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.search.fonl.value.LabledFonlValue;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LabelFonl {

    public final static String EMPTY_LABEL = "_";
    protected final NeighborList neighborList;
    protected final JavaPairRDD <Integer, String> labels;
    private JavaPairRDD <Integer, LabledFonlValue> lfonl;

    public LabelFonl(NeighborList neighborList, JavaPairRDD <Integer, String> labels) {
        this.neighborList = neighborList;
        this.labels = labels;
    }

    public JavaPairRDD <Integer, LabledFonlValue> getOrCreateLFonl() {
        if (lfonl == null) {
            lfonl = createLFonl();
        }
        return lfonl;
    }

    public JavaPairRDD <Integer, LabledFonlValue> createLFonl() {
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

        JavaPairRDD <Integer, LabledFonlValue> fonlLabels = labelDegMsg
                .groupByKey(neighbors.getNumPartitions())
                .mapToPair(kv -> {
                    int degree = 0;

                    if (kv._1 == 1) {
                        System.out.println("test");
                    }

                    for (VLabelDeg value : kv._2) {
                        degree++;
                    }

                    List <VLabelDeg> list = new ArrayList <>();
                    for (VLabelDeg vd : kv._2) {
                        if (vd.degree > degree || (vd.degree == degree && vd.vertex > kv._1)) {
                            list.add(vd);
                        }
                    }

                    list.sort((a, b) -> {
                        if (a.degree != b.degree) {
                            return a.degree - b.degree;
                        } else {
                            return a.vertex - b.vertex;
                        }
                    });

                    LabledFonlValue value = new LabledFonlValue(degree, list);
                    return new Tuple2 <>(kv._1, value);
                }).cache();

        return fonlLabels.leftOuterJoin(labels).mapValues(v -> {
            v._1.meta.label = v._2.orElse(EMPTY_LABEL);

            return v._1;
        }).repartition(fonlLabels.getNumPartitions())
                .persist(StorageLevel.MEMORY_AND_DISK());
    }
}
