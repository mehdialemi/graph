package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Create triangle from neighbor list
 */
public class Triangle extends SparkApp {

    private final NeighborList neighborList;

    public Triangle(NeighborList neighborList) {
        super(neighborList);
        this.neighborList = neighborList;
        conf.getSparkConf().registerKryoClasses(new Class[] {GraphUtils.VertexDegreeInt.class});
    }

    public JavaPairRDD<Integer, int[]> createFonl() {
        return createFonl(conf.getPartitionNum());
    }

    public JavaPairRDD<Integer, int[]> createFonl(int partitionNum) {
        return neighborList.create().flatMapToPair(t -> {
            int deg = t._2.length;
            if (deg == 0)
                return Collections.emptyIterator();

            GraphUtils.VertexDegreeInt vd = new GraphUtils.VertexDegreeInt(t._1, deg);
            List<Tuple2<Integer, GraphUtils.VertexDegreeInt>> degreeList = new ArrayList<>(deg);

            // Add degree information of the current vertex to its neighbor
            for (int neighbor : t._2) {
                degreeList.add(new Tuple2<>(neighbor, vd));
            }

            return degreeList.iterator();
        }).groupByKey(partitionNum).mapToPair(v -> {
            int degree = 0;
            // Iterate over higherIds to calculate degree of the current vertex
            if (v._2 == null)
                return new Tuple2<>(v._1, new int[]{0});

            for (GraphUtils.VertexDegreeInt vd : v._2) {
                degree++;
            }

            List<GraphUtils.VertexDegreeInt> list = new ArrayList<>();
            for (GraphUtils.VertexDegreeInt vd : v._2)
                if (vd.degree > degree || (vd.degree == degree && vd.vertex > v._1))
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

            int[] higherDegs = new int[list.size() + 1];
            higherDegs[0] = degree;
            for (int i = 1; i < higherDegs.length; i++)
                higherDegs[i] = list.get(i - 1).vertex;

            return new Tuple2<>(v._1, higherDegs);
        }).persist(StorageLevel.MEMORY_AND_DISK_2());
    }

    public JavaPairRDD<Integer, int[]> generateCandidates(JavaPairRDD<Integer, int[]> fonl) {
        return fonl
                .filter(t -> t._2.length > 2) // Select vertices having more than 2 items in their values
                .flatMapToPair(t -> {

                    int size = t._2.length - 1; // one is for the first index holding node's degree

                    if (size == 1)
                        return Collections.emptyIterator();

                    List<Tuple2<Integer, int[]>> output;
                    output = new ArrayList<>(size);

                    for (int index = 1; index < size; index++) {
                        int len = size - index;
                        int[] cvalue = new int[len + 1];
                        cvalue[0] = t._1; // First vertex in the triangle
                        System.arraycopy(t._2, index + 1, cvalue, 1, len);
                        Arrays.sort(cvalue, 1, cvalue.length); // quickSort to comfort with fonl
                        output.add(new Tuple2<>(t._2[index], cvalue));
                    }

                    return output.iterator();
                });
    }
}
