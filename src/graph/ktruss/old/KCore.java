package graph.ktruss.old;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * This class get Fdonl and a k, then remove all vertices that their degrees are smaller than k
 * This is an iterative process, until all remaining vertices have degree higher or equal to k
 */
public class KCore implements Serializable {
    private static final long serialVersionUID = 1L;

    private JavaPairRDD<Long, FdValue> fdonl;

    private Broadcast<Integer> minDegree;

    public KCore(JavaPairRDD<Long, FdValue> fdonl, JavaSparkContext sc, int k) {
        this.fdonl = fdonl;
        minDegree = sc.broadcast(k);
    }

    public JavaPairRDD<Long, FdValue> findKCore() {
        JavaPairRDD<Long, FdValue> currentFdonl = fdonl;
        currentFdonl.cache();

        int i = 0;
        while (true) {
            System.out.println("Step " + ++i + " ============================================= ");
            // Find vertices that should be removed from the graph
            JavaPairRDD<Long, FdValue> expiredVertices = currentFdonl.filter(v -> v._2.degree < minDegree.getValue());
            expiredVertices = expiredVertices.cache();
            JavaPairRDD<Long, Iterable<Long>> subtractList = expiredVertices.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, FdValue>, Long, Long>() {
                @Override
                public Iterable<Tuple2<Long, Long>> call(Tuple2<Long, FdValue> v) throws Exception {
                    List<Tuple2<Long, Long>> affectedNeighbors = new ArrayList<>();
                    for (long vertex : v._2.lowDegs)
                        affectedNeighbors.add(new Tuple2<>(vertex, v._1));
                    for (long vertex : v._2.highDegs)
                        affectedNeighbors.add(new Tuple2<>(vertex, v._1));
                    return affectedNeighbors;
                }
            }).groupByKey(); // All expired neighbors related to a vertex are grouped together

            // If there was no vertex having degree lesser than minimum threshold the quite.
            if (subtractList.count() == 0)
                break;

            currentFdonl = currentFdonl.subtractByKey(expiredVertices);

            // Join our current Fdonl with the subtract list to update Fdonl information
            currentFdonl.cogroup(subtractList)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Iterable<FdValue>, Iterable<Iterable<Long>>>>, Long, FdValue>() {
                    @Override
                    public Tuple2<Long, FdValue> call(Tuple2<Long, Tuple2<Iterable<FdValue>, Iterable<Iterable<Long>>>>
                                                          v) throws Exception {
                        Iterator<FdValue> fdonlItemIterator = v._2._1.iterator();
                        if (!fdonlItemIterator.hasNext())
                            return null;

                        FdValue fdonlItem = fdonlItemIterator.next();

                        fdonlItem.removeNeighbors(v._2._2.iterator());

                        if (fdonlItem.degree == 0)
                            return null;

                        return new Tuple2<>(v._1, fdonlItem);
                    }
                }).filter(t -> t != null);

            currentFdonl = currentFdonl.cache();
        }

        return currentFdonl;
    }
}
