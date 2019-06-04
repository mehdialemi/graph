package ir.ac.sbu.graph.spark.search.fonl.creator;

import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.search.fonl.value.LabledFonlValue;
import ir.ac.sbu.graph.spark.search.fonl.value.TriangleFonlValue;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.utils.OrderedNeighborList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

/**
 * Triangle Based Fonl
 */
public class TriangleFonl extends LabelFonl {

    protected JavaPairRDD <Integer, TriangleFonlValue> tfonl;

    public TriangleFonl(NeighborList neighborList, JavaPairRDD<Integer, String> labels) {
        super(neighborList, labels);
    }

    public JavaPairRDD <Integer, TriangleFonlValue> getOrCreateTFonl() {
        if (tfonl == null) {
            tfonl = createTFonl();
        }
        return tfonl;
    }

    public JavaPairRDD <Integer, TriangleFonlValue> createTFonl() {

        JavaPairRDD <Integer, LabledFonlValue> labelFonl = super.getOrCreateLFonl();

        JavaPairRDD <Integer, int[]> candidates = createCandidates(labelFonl);

        JavaPairRDD <Integer, Iterable <Edge>> edgeMsg = candidates.cogroup(labelFonl).flatMapToPair(kv -> {
            List <Tuple2 <Integer, Edge>> output = new ArrayList <>();

            Iterator <LabledFonlValue> iterator = kv._2._2.iterator();
            if (!iterator.hasNext())
                return output.iterator();

            LabledFonlValue labledFonlValue = iterator.next();
            int[] hDegs = labledFonlValue.fonl;

            kv._2._1.iterator();
            Iterator <int[]> cIterator = kv._2._1.iterator();
            if (!cIterator.hasNext())
                return output.iterator();

            Arrays.sort(hDegs, 0, hDegs.length);

            int v = kv._1;
            do {
                int[] forward = cIterator.next();
                int u = forward[0];

                IntList intersects = OrderedNeighborList.intersection(hDegs, forward, 0, 1);
                if (intersects == null)
                    continue;

                IntListIterator iter = intersects.intListIterator();
                while (iter.hasNext()) {
                    int w = iter.nextInt();
                    output.add(new Tuple2 <>(u, new Edge(v, w)));
                    output.add(new Tuple2 <>(v, new Edge(w, -1)));
                }

            } while (iterator.hasNext());

            return output.iterator();
        }).groupByKey(labelFonl.getNumPartitions());

        return labelFonl.leftOuterJoin(edgeMsg).mapValues(join -> {
            TriangleFonlValue triangleFonlValue = new TriangleFonlValue(join._1);
            if (!join._2.isPresent())
                return triangleFonlValue;

            IntSet removes = null;
            for (Edge edge : join._2.get()) {
                if (edge.v2 != -1) {
                    triangleFonlValue.addNeighborEdge(edge);
                } else {
                    if (removes == null)
                        removes = new IntOpenHashSet();
                    removes.add(edge.v1);
                }
            }

            if (removes != null)
                triangleFonlValue.remove(removes);

            return triangleFonlValue;
        }).cache();
    }

    private JavaPairRDD<Integer, int[]> createCandidates(JavaPairRDD <Integer, LabledFonlValue>  labelFonl) {
        return labelFonl.filter(t -> t._2.fonl.length > 1) // Select vertices having more than 2 items in their values
                .flatMapToPair(t -> {

                    int[] fonl = t._2.fonl;
                    int size = fonl.length; // one is for the first index holding node's degree

                    List<Tuple2<Integer, int[]>> output;
                    output = new ArrayList<>(size);

                    for (int index = 0; index < size; index++) {
                        int cVertex = fonl[index];
                        int len = size - index;
                        int[] cValue = new int[len];
                        cValue[0] = t._1; // First vertex in the triangle
                        System.arraycopy(fonl, index + 1, cValue, 1, len - 1);
                        Arrays.sort(cValue, 1, cValue.length);
                        output.add(new Tuple2<>(cVertex, cValue));
                    }

                    return output.iterator();
                });
    }
}
