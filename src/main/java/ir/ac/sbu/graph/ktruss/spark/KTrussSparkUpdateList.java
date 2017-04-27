package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

import static ir.ac.sbu.graph.utils.Log.log;

public class KTrussSparkUpdateList extends KTruss{

    public KTrussSparkUpdateList(KTrussConf conf) {
        super(conf);
    }

    public long start() {

        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> tVertices = triangleVertices();
        final int minSup = conf.k - 2;

        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> invalids = tVertices.filter(t -> t._2.size() < minSup)
            .repartition(partitionNum2).cache();

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> updates =
            sc.parallelize(new ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>>()).mapToPair(t -> new Tuple2<>(t._1, t._2));

        int iteration = 0;
        while (true) {
            long invCount = invalids.count();
            if (invCount == 0)
                break;
            System.out.println("iteration: " + ++ iteration + ", invalids: " + invCount);

            JavaPairRDD<Tuple2<Integer, Integer>, Integer> invUpdate = invalids.flatMapToPair(t -> {
                List<Tuple2<Tuple2<Integer, Integer>, Integer>> list = new ArrayList<>();
                int u = t._1._1;
                int v = t._1._2;
                Iterator<Integer> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    int w = iterator.next();
                    if (u < w)
                        list.add(new Tuple2<>(new Tuple2<>(u, w), v));
                    else
                        list.add(new Tuple2<>(new Tuple2<>(w, u), v));

                    if (v < w)
                        list.add(new Tuple2<>(new Tuple2<>(v, w), u));
                    else
                        list.add(new Tuple2<>(new Tuple2<>(w, v), u));
                }
                return list.iterator();
            }).cache();

            updates = updates.union(invUpdate);

            JavaPairRDD<Tuple2<Integer, Integer>, int[]> allInvUpdates = updates.cogroup(invUpdate).flatMapToPair(t -> {
                if (t._2._2.iterator().hasNext()) {
                    IntSet set = new IntOpenHashSet();
                    for (Integer i : t._2._1) {
                        set.add(i.intValue());
                    }
                    return Arrays.asList(new Tuple2<>(t._1, set.toIntArray())).iterator();
                }

                return Collections.emptyIterator();
            }).partitionBy(partitioner2);

            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> prevInvalids = invalids;

            invalids = tVertices.join(allInvUpdates).flatMapToPair(t -> {
                for (int i : t._2._2) {
                    t._2._1.remove(i);
                }

                if (t._2._1.size() == 0)
                    return Collections.emptyIterator();

                if (t._2._1.size() < minSup) {
                    return Arrays.asList(new Tuple2<>(t._1, t._2._1)).iterator();
                }

                return Collections.emptyIterator();
            }).repartition(partitionNum2).cache();
            prevInvalids.unpersist(false);
        }

        return tVertices.cogroup(updates).mapValues(t -> {
            IntSet set = t._1.iterator().next();
            for (Integer i : t._2) {
                set.remove(i);
            }
            if (set.size() < minSup)
                return 0;
            return 1;
        }).filter(t -> t._2 != 0).count();


    }

    public static void main(String[] args) {
        KTrussConf conf = new KTrussConf(args, KTrussSparkEdgeVertices.class.getSimpleName(),
            GraphUtils.VertexDegree.class, long[].class, List.class);
        KTrussSparkUpdateList kTruss = new KTrussSparkUpdateList(conf);

        long start = System.currentTimeMillis();
        long edgeCount = kTruss.start();
        long duration = System.currentTimeMillis() - start;
        kTruss.close();
        log("KTruss Edge Count: " + edgeCount, duration);
    }
}
