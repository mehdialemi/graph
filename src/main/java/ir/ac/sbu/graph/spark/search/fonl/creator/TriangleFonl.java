package ir.ac.sbu.graph.spark.search.fonl.creator;

import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.search.fonl.value.TriangleFonlValue;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.utils.OrderedNeighborList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

public class TriangleFonl {

    private NeighborList neighborList;
    private JavaPairRDD <Integer, int[]> neighborRDD;

    public TriangleFonl(NeighborList neighborList) {
        this.neighborList = neighborList;
    }

    public JavaPairRDD <Integer, int[]> getNeighborRDD() {
        return neighborRDD;
    }

    public JavaPairRDD <Integer, TriangleFonlValue> create() {

        neighborRDD = neighborList.getOrCreate();

        Triangle triangle = new Triangle(neighborList, neighborRDD);
        JavaPairRDD <Integer, int[]> fonlRDD = triangle.createFonl();
        JavaPairRDD <Integer, int[]> candidates = triangle.createCandidates(fonlRDD);

        JavaPairRDD <Integer, Iterable <Edge>> triangleInfo = candidates.cogroup(fonlRDD)
                .flatMapToPair(kv -> {
                    List <Tuple2 <Integer, Edge>> output = new ArrayList <>();

                    // Iterator for fonlRDD
                    Iterator <int[]> fonlIterator = kv._2._2.iterator();
                    if (!fonlIterator.hasNext())
                        return output.iterator();

                    int[] hDegs = fonlIterator.next();

                    Iterator <int[]> candidateIterator = kv._2._1.iterator();
                    if (!candidateIterator.hasNext())
                        return output.iterator();

                    // Consider the assumption u < v < w
                    int v = kv._1;
                    Arrays.sort(hDegs, 1, hDegs.length);
                    do {
                        int[] forward = candidateIterator.next();
                        int u = forward[0];

                        IntList wList = OrderedNeighborList.intersection(hDegs, forward, 1, 1);
                        if (wList == null)
                            continue;

                        IntListIterator iter = wList.listIterator();
                        while (iter.hasNext()) {
                            int w = iter.nextInt();

                            // add edge (v, w) to TriangleFonlValue to recognize a triangle in the fonl kv with key = u
                            output.add(new Tuple2 <>(u, new Edge(v, w)));
                        }
                    } while (candidateIterator.hasNext());

                    return output.iterator();
                }).groupByKey();

        // update fonlRDD based on triangle information
        return fonlRDD.leftOuterJoin(triangleInfo).mapValues(value -> {
            int[] fonl = new int[value._1.length - 1];
            System.arraycopy(value._1, 1, fonl, 0, fonl.length);

            return new TriangleFonlValue(value._1[0], fonl, value._2.orElse(Collections.emptyList()));
        });
    }
}
