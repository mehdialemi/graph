package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * Create triangle from neighbor list
 */
public class Triangle extends SparkApp {

    public static final int P_MULTIPLIER = 7;
    private final NeighborList neighborList;
    private JavaPairRDD<Integer, int[]> fonl;
    private JavaPairRDD<Integer, int[]> candidates;
    private JavaPairRDD<Integer, Integer> vertexTC;

    public Triangle(NeighborList neighborList) {
        super(neighborList);
        this.neighborList = neighborList;
        conf.getSparkConf().registerKryoClasses(new Class[] {GraphUtils.VertexDegreeInt.class, int[].class});
    }

    public JavaPairRDD<Integer, int[]> getFonl() {
        if (fonl == null)
            fonl = createFonl(P_MULTIPLIER).persist(StorageLevel.MEMORY_AND_DISK_2());
        return fonl;
    }

    public JavaPairRDD<Integer, int[]> getCandidates() {
        if (candidates == null)
            candidates = createCandidates(getFonl())
                    .persist(StorageLevel.MEMORY_AND_DISK());
        return candidates;
    }

    public JavaPairRDD<Integer, Integer> getVertexTC() {
        if (vertexTC == null)
            createVertexTC();

        return vertexTC;
    }

    public JavaPairRDD<Integer, int[]> createFonl(final int pMultiplier) {
        JavaPairRDD<Integer, int[]> neighborList = this.neighborList.create();
        int partitions = neighborList.getNumPartitions() * pMultiplier;
        return neighborList.flatMapToPair(t -> {
            int deg = t._2.length;
            if (deg == 0)
                return Collections.emptyIterator();

            VertexDeg vd = new VertexDeg(t._1, deg);
            List<Tuple2<Integer, VertexDeg>> degreeList = new ArrayList<>(deg);

            // Add degree information of the current vertex to its neighbor
            for (int neighbor : t._2) {
                degreeList.add(new Tuple2<>(neighbor, vd));
            }

            return degreeList.iterator();
        }).groupByKey(partitions).mapToPair(v -> {
            int degree = 0;
            // Iterate over higherIds to calculate degree of the current vertex
            if (v._2 == null)
                return new Tuple2<>(v._1, new int[]{0});

            for (VertexDeg vd : v._2) {
                degree++;
            }

            List<VertexDeg> list = new ArrayList<>();
            for (VertexDeg vd : v._2)
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
        });
    }

    public JavaPairRDD<Integer, int[]>  createCandidates(JavaPairRDD<Integer, int[]> fonl) {
        return fonl.filter(t -> t._2.length > 2) // Select vertices having more than 2 items in their values
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

    public void createVertexTC() {

        JavaPairRDD<Integer, int[]> fonl = getFonl();
        JavaPairRDD<Integer, int[]> candidates = getCandidates();

        vertexTC = candidates.cogroup(fonl, fonl.getNumPartitions())
                .flatMapToPair(t -> {
            Iterator<int[]> iterator = t._2._2.iterator();
            List<Tuple2<Integer, Integer>> output = new ArrayList<>();
            if (!iterator.hasNext())
                return output.iterator();

            int[] hDegs = iterator.next();

            iterator = t._2._1.iterator();
            if (!iterator.hasNext())
                return output.iterator();

            Arrays.sort(hDegs, 1, hDegs.length);

            int sum = 0;
            do {
                int[] forward = iterator.next();
                int count = GraphUtils.sortedIntersectionCountInt(hDegs, forward, output, 1, 1);
                if (count > 0) {
                    sum += count;
                    output.add(new Tuple2<>(forward[0], count));
                }
            } while (iterator.hasNext());

            if (sum > 0) {
                output.add(new Tuple2<>(t._1, sum));
            }

            return output.iterator();
        }).reduceByKey((a, b) -> a + b)
                .persist(StorageLevel.MEMORY_AND_DISK());
    }

    public long triangleCount() {
        Long tc3 = getVertexTC().map(kv -> Long.valueOf(kv._2)).reduce((a, b) -> a + b);
        return tc3 / 3;
    }
}
