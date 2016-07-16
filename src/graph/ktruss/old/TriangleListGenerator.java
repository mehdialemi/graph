package graph.ktruss.old;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * This class generate all list of triangles
 */
public class TriangleListGenerator implements Serializable {
    private static final long serialVersionUID = 1L;

    private JavaPairRDD<Long, FdValue> fdonl;

    public TriangleListGenerator(JavaPairRDD<Long, FdValue> fdonl) {
        this.fdonl = fdonl;
    }

    public JavaRDD<Triangle> findTriangles() {
        JavaPairRDD<Long, long[]> fonl = fdonl.filter(t -> (t._2.degree > 1))
            .mapToPair(new PairFunction<Tuple2<Long, FdValue>, Long, long[]>() {
                @Override
                public Tuple2<Long, long[]> call(Tuple2<Long, FdValue> t) throws Exception {
                    return new Tuple2<>(t._1, t._2.highDegs);
                }
            });

        fonl = fonl.cache();

        JavaPairRDD<Long, CandidateState> candidates = fonl
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, long[]>, Long, CandidateState>() {
                @Override
                public Iterable<Tuple2<Long, CandidateState>> call(Tuple2<Long, long[]> t) throws Exception {
                    List<Tuple2<Long, CandidateState>> output = new ArrayList<>();

                    long[] candidateNeighbors = new long[t._2.length];
                    System.arraycopy(t._2, 0, candidateNeighbors, 0, t._2.length);
                    Arrays.sort(candidateNeighbors);
                    CandidateState candidateState = new CandidateState(t._1, candidateNeighbors);

                    for (long neighbor : t._2) {
                        output.add(new Tuple2<>(neighbor, candidateState));
                    }

                    return output;
                }
            });

        JavaRDD<Triangle> allTriangles = candidates.cogroup(fonl).flatMap(t -> {
            List<Triangle> triangles = new ArrayList<>();

            Iterator<long[]> fonlItemIter = t._2._2.iterator();
            if (!fonlItemIter.hasNext())
                return triangles;

            long[] higherDegreeNeighbors = fonlItemIter.next();
            Arrays.sort(higherDegreeNeighbors);

            Iterable<CandidateState> allCandidates = t._2._1;
            for (CandidateState candidateState : allCandidates) {
                updateTriangles(triangles, candidateState.vertexId, t._1, higherDegreeNeighbors, candidateState.candidateNeighbors);
            }
            return triangles;

        });

        return allTriangles;
    }

    private void updateTriangles(List<Triangle> triangles, long first, long second, long[] neighbors, long[] check) {
        if (neighbors.length == 0 || check.length == 0)
            return;

        int nIndex = 0;
        int cIndex = 0;

        if (neighbors[0] >= check[0]) {
            cIndex = Arrays.binarySearch(check, neighbors[0]);
            cIndex = cIndex >= 0 ? cIndex : -1 * (cIndex - 1);
        } else {
            nIndex = Arrays.binarySearch(neighbors, check[0]);
            nIndex = nIndex >= 0 ? nIndex : -1 * (nIndex - 1);
        }

        while (true) {
            if (nIndex >= neighbors.length || cIndex >= check.length)
                break;

            if (neighbors[nIndex] == check[cIndex]) {
                triangles.add(new Triangle(first, second, neighbors[nIndex]));
                nIndex++;
                cIndex++;
            } else if (neighbors[nIndex] > check[cIndex])
                cIndex++;
            else
                nIndex++;
        }
    }

    public static class CandidateState implements Serializable {
        private static final long serialVersionUID = 1L;

        public long[] candidateNeighbors;
        public long vertexId;

        public CandidateState() {
        }

        public CandidateState(long vertexId, long[] neighbors) {
            this.candidateNeighbors = neighbors;
            this.vertexId = vertexId;
        }
    }
}
