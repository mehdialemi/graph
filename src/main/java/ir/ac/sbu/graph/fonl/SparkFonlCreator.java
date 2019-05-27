package ir.ac.sbu.graph.fonl;

import ir.ac.sbu.graph.fonl.matcher.DegreeMeta;
import ir.ac.sbu.graph.fonl.matcher.LabelMeta;
import ir.ac.sbu.graph.fonl.matcher.TFonlValue;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.VertexDeg;
import org.apache.commons.collections.iterators.CollatingIterator;
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
            fValue.fonl = new int[list.size()];

            for (int i = 0; i < list.size(); i++) {
                fValue.fonl[i] = list.get(i).vertex;
                fValue.meta.degs[i] = list.get(i).degree;
            }
            fValue.ifonl = new int[fValue.fonl.length];
            System.arraycopy(fValue.fonl, 0, fValue.ifonl, 0, fValue.ifonl.length);
            Arrays.sort(fValue.ifonl);

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

        return degFonl.join(labels).leftOuterJoin(labelMsg)
                .mapValues(value -> {
                    Map <Integer, String> labelMap = new HashMap <>();
                    for (Tuple2 <Integer, String> vLabel : value._2.orElse(Collections.emptyList())) {
                        labelMap.put(vLabel._1, vLabel._2);
                    }

                    Fvalue <LabelMeta> fvalue = new Fvalue <>();
                    Fvalue <DegreeMeta> dfonl = value._1._1;

                    fvalue.fonl = dfonl.fonl;
                    fvalue.ifonl = dfonl.ifonl;
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

    public static JavaPairRDD<Integer, int[]> createCandidates(JavaPairRDD <Integer, Fvalue <LabelMeta>>  labelFonl) {
        return labelFonl.filter(t -> t._2.fonl.length > 1) // Select vertices having more than 2 items in their values
                .flatMapToPair(t -> {

                    if (t._1.equals(7)) {
                        System.out.println("cannddand");
                    }
//                    int[] ifonl = t._2.ifonl;
                    int[] fonl = t._2.fonl;
                    int size = fonl.length; // one is for the first index holding node's degree

                    if (size == 1)
                        return Collections.emptyIterator();

                    List<Tuple2<Integer, int[]>> output;
                    output = new ArrayList<>(size);

                    for (int index = 0; index < size; index++) {
                        int cVertex = fonl[index];
                        int len = size - index;
                        int[] cValue = new int[len];
                        cValue[0] = t._1; // First vertex in the triangle
                        System.arraycopy(fonl, index + 1, cValue, 1, len - 1);
                        output.add(new Tuple2<>(cVertex, cValue));
                    }

                    return output.iterator();
                });
    }

    public static JavaPairRDD <Integer, TFonlValue> createTFonl(JavaPairRDD <Integer, Fvalue <LabelMeta>>  labelFonl) {
        JavaPairRDD <Integer, int[]> candidates = createCandidates(labelFonl);
        JavaPairRDD <Integer, Iterable <Edge[]>> tEdges = candidates.join(labelFonl).flatMapToPair(kv -> {
            int[] cArray = kv._2._1;
            Fvalue <LabelMeta> lFValue = kv._2._2;

            int fVertex = cArray[0];

            if (fVertex == 7) {
                System.out.println("hhh");
            }
            List <Edge> list = new ArrayList <>();
            int vertex = kv._1;
            for (int i = 1; i < cArray.length; i++) {
                int r = Arrays.binarySearch(lFValue.ifonl, cArray[i]);
                if (r < 0)
                    continue;

                list.add(new Edge(vertex, cArray[i]));
//                list.add(new Edge(fVertex, vertex));
//                list.add(new Edge(fVertex, cArray[i]));
            }

            if (list.isEmpty())
                return Collections.emptyIterator();

            return Collections.singleton(new Tuple2 <>(fVertex, list.toArray(new Edge[0]))).iterator();
        }).groupByKey(labelFonl.getNumPartitions());

        JavaPairRDD <Integer, TFonlValue> triangleFonl = labelFonl.leftOuterJoin(tEdges).mapValues(v -> {

            TFonlValue tFonlValue = new TFonlValue();
            tFonlValue.meta = new TriangleMeta(v._1.meta);
            tFonlValue.ifonl = v._1.ifonl;
            tFonlValue.fonl = v._1.fonl;
            for (Edge[] edges : v._2.orElse(CollatingIterator::new)) {
                for (Edge edge : edges) {
                    tFonlValue.meta.edges.add(edge);
                }
            }

            return tFonlValue;
        }).cache();

        return triangleFonl;
    }
}
