package ir.ac.sbu.graph.spark.search.fonl.creator;

import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.search.fonl.value.TriangleFonlValue;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.utils.OrderedNeighborList;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
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

    JavaPairRDD <Integer, int[]> getNeighborRDD() {
        return neighborRDD;
    }

    public JavaPairRDD <Integer, TriangleFonlValue> create() {

        neighborRDD = neighborList.getOrCreate();

        Triangle triangle = new Triangle(neighborList);

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

                            // add edge (v, w) to TriangleFonlValue of key=u
                            output.add(new Tuple2 <>(u, new Edge(v, w)));

                            // remove vertex w from the corresponding value of key=v
                            output.add(new Tuple2 <>(v, new Edge(w, -1)));
                        }

                    } while (candidateIterator.hasNext());

                    return output.iterator();
                }).groupByKey();

        // update fonlRDD based on triangle information
        return fonlRDD.leftOuterJoin(triangleInfo).mapValues(value -> {
            int degree = value._1[0];
            int[] fonl = null;

            Iterable<Edge> edges;
            if (value._2.isPresent()) {
                Int2IntSortedMap v2Index = new Int2IntAVLTreeMap();
                for (int i = 1; i < value._1.length; i++) {
                    v2Index.put(value._1[i], i - 1);
                }

                List<Edge> list = new ArrayList <>();
                for (Edge edge : value._2.get()) {
                    if (edge.v2 < 0)
                        v2Index.remove(edge.v1);
                    else
                        list.add(edge);
                }
                edges = list;

                // check neighbor removing from the fonl value
                if (v2Index.size() < value._1.length - 1) {
                    // some vertices were removed from the fonl
                    fonl = new int[v2Index.size()];
                    for (Map.Entry <Integer, Integer> entry : v2Index.entrySet()) {
                        fonl[entry.getValue()] = entry.getKey();
                    }
                }
            } else {
                edges = Collections.emptyList();
            }

            if (fonl == null) {
                fonl = new int[value._1.length - 1];
                System.arraycopy(value._1, 1, fonl, 0, fonl.length);
            }

            return new TriangleFonlValue(degree, fonl, edges);
        });
    }
}
