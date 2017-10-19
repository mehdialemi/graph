package ir.ac.sbu.graph.utils;

import ir.ac.sbu.graph.clusteringco.FonlUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Required graph utilities by different graph algorithms
 */
public class IntGraphUtils {

    public static JavaPairRDD<Integer, Integer> loadEdges(JavaSparkContext sc, String inputPath,
                                                   int partitionNum) {
        JavaRDD<String> input = sc.textFile(inputPath);
        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);
        return edges.repartition(partitionNum).cache();
    }

    public static JavaPairRDD<Integer, int[]> createNeighborList(Partitioner partitioner, JavaPairRDD<Integer, Integer> edges) {
        return edges.groupByKey(partitioner).mapToPair(t -> {
            IntSet set = new IntOpenHashSet();
            for (Integer v : t._2) {
                set.add(v.intValue());
            }
            return new Tuple2<>(t._1, set.toIntArray());
        }).cache();
    }

    /**
     * copied from {@link FonlUtils#createWith2ReduceDegreeSortInt(JavaPairRDD, Partitioner)}
     * @param neighborList
     * @param smallPartitioner
     * @param bigPartitioner
     * @return
     */
    public static JavaPairRDD<Integer, int[]> createFonl(JavaPairRDD<Integer, int[]> neighborList,
                                                         Partitioner smallPartitioner, Partitioner bigPartitioner) {

        return neighborList.flatMapToPair(vnl -> {
            int degree = vnl._2.length;
            List<Tuple2<Integer, Tuple2<Integer,Integer>>> degList = new ArrayList<>(degree);
            for (int neighbor : vnl._2) {
                degList.add(new Tuple2<>(neighbor, new Tuple2<>(vnl._1, degree)));
            }

            return degList.iterator();
        }).groupByKey(smallPartitioner).mapToPair(vdl -> {

            int degree = 0;

            // Iterate over higherIds to calculate degree of the current vertex
            if (vdl._2 == null)
                return new Tuple2<>(vdl._1, new int[]{0});

            for (Tuple2<Integer, Integer> vd : vdl._2) {
                degree++;
            }

            List<Tuple2<Integer, Integer>> list = new ArrayList<>();
            for (Tuple2<Integer, Integer> vd : vdl._2)
                if (vd._2 > degree || (vd._2 == degree && vd._1 > vdl._1))
                    list.add(vd);

            Collections.sort(list, (a, b) -> {
                int x, y;
                if (a._2 != b._2) {
                    x = a._2;
                    y = b._2;
                } else {
                    x = a._1;
                    y = b._1;
                }
                return x - y;
            });

            int[] higherDegs = new int[list.size() + 1];
            higherDegs[0] = degree;
            for (int i = 1; i < higherDegs.length; i++)
                higherDegs[i] = list.get(i - 1)._1;

            return new Tuple2<>(vdl._1, higherDegs);
        }).partitionBy(bigPartitioner).persist(StorageLevel.MEMORY_AND_DISK_2());
    }
}
