package ir.ac.sbu.graph.fonl;

import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.types.VertexDeg;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

public class SparkFonlCreator {

    public static JavaPairRDD <Integer, Fvalue <DegreeMeta>> createDegFonl(NeighborList neighborList) {
        JavaPairRDD <Integer, int[]> neighbors = neighborList.getOrCreate();

        return neighbors.flatMapToPair(t -> {
            int deg = t._2.length;
            if (deg == 0)
                return Collections.emptyIterator();

            VertexDeg vd = new VertexDeg(t._1, deg);
            List <Tuple2 <Integer, VertexDeg>> degreeList = new ArrayList <>(deg);

            // Add degree information of the current vertex to its neighbor
            for (int neighbor : t._2) {
                degreeList.add(new Tuple2 <>(neighbor, vd));
            }

            return degreeList.iterator();
        }).groupByKey(neighbors.getNumPartitions()).mapToPair(kv -> {
            int degree = 0;
            // Iterate over higherIds to calculate degree of the current vertex
            if (kv._2 == null)
                return new Tuple2 <>(kv._1, DegreeMeta.createEmptyFValue());

            for (VertexDeg vd : kv._2) {
                degree++;
            }

            List <VertexDeg> list = new ArrayList <>();
            for (VertexDeg vd : kv._2)
                if (vd.degree > degree || (vd.degree == degree && vd.vertex > kv._1))
                    list.add(vd);

            Collections.sort(list, (a, b) -> {
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

            Fvalue <DegreeMeta> fValue = new Fvalue <>();
            fValue.meta = new DegreeMeta(degree, list.size());
            ;
            fValue.fonl = new int[list.size()];

            for (int i = 0; i < list.size(); i++) {
                fValue.fonl[i] = list.get(i).vertex;
                fValue.meta.degs[i] = list.get(i).degree;
            }

            return new Tuple2 <>(kv._1, fValue);
        }).cache();
    }

    public static JavaPairRDD <Integer, Fvalue <LabelMeta>> createLabelFonl(NeighborList neighborList,
                                                                            JavaPairRDD <Integer, String> labels) {
        JavaPairRDD <Integer, Fvalue <DegreeMeta>> degFonl = createDegFonl(neighborList);

        JavaPairRDD <Integer, Iterable <Tuple2 <Integer, String>>> labelMsg = degFonl.flatMapToPair(kv -> {
            List <Tuple2 <Integer, Integer>> list = new ArrayList <>();
            for (int v : kv._2.fonl) {
                list.add(new Tuple2 <>(v, kv._1));
            }
            return list.iterator();
        })
                .groupByKey()
                .join(labels)
                .flatMapToPair(kv -> {
                    List <Tuple2 <Integer, Tuple2 <Integer, String>>> list = new ArrayList <>();
                    String label = kv._2._2;
                    for (Integer v : kv._2._1) {
                        list.add(new Tuple2 <>(v, new Tuple2 <>(kv._1, label)));
                    }
                    return list.iterator();
                }).groupByKey(degFonl.getNumPartitions());

        return degFonl.join(labels).join(labelMsg)
                .mapValues(value -> {
                    Map <Integer, String> labelMap = new HashMap <>();
                    for (Tuple2 <Integer, String> vLabel : value._2) {
                        labelMap.put(vLabel._1, vLabel._2);
                    }

                    Fvalue <LabelMeta> fvalue = new Fvalue <>();
                    Fvalue <DegreeMeta> dfonl = value._1._1;

                    fvalue.fonl = dfonl.fonl;
                    fvalue.meta = new LabelMeta();
                    fvalue.meta.label = value._1._2;
                    fvalue.meta.deg = dfonl.meta.deg;
                    fvalue.meta.degs = dfonl.meta.degs;
                    fvalue.meta.labels = new String[fvalue.fonl.length];
                    for (int i = 0; i < dfonl.fonl.length; i++) {
                        fvalue.meta.labels[i] = labelMap.get(dfonl.fonl[i]);
                    }

                    return fvalue;
                }).cache();
    }
}
