package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.TriangleSubgraph;
import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

/**
 *
 */
public class KTrussSparkTriangleSet {

    public static final int CO_PARTITION_FACTOR = 5;

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 10;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);
        final int minSup = k - 2;

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "KTruss-EdgeVertexList-" + k + "-MultiSteps", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, long[].class, List.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Partitioner partitionerSmall = new HashPartitioner(partition);
        Partitioner partitionerBig = new HashPartitioner(partition * 10);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partitionerSmall);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl).partitionBy(partitionerBig);

        JavaPairRDD<Integer, TriangleSubgraph> triangleSets = candidates.cogroup(fonl).flatMapToPair(t -> {
            Iterator<int[]> cvalues = t._2._2.iterator();
            List<Tuple2<Integer, int[]>> list = new ArrayList<>();

            if (!cvalues.hasNext())
                return Collections.emptyIterator();

            int[] fv = cvalues.next();
            cvalues = t._2._1.iterator();
            IntList wList = new IntArrayList();
            while (cvalues.hasNext()) {
                int v = t._1;
                int[] cvalue = cvalues.next();
                int lastIndex = 1;
                int index = lastIndex;
                int u = cvalue[0];
                wList.clear();
                for (int i = 1; i < cvalue.length && lastIndex < fv.length; i++) {
                    int w = cvalue[i];
                    boolean common = false;
                    for (int j = index; j < fv.length; j++, index++) {
                        if (w == fv[j]) {
                            wList.add(w);
                            common = true;
                            lastIndex = index + 1;
                            break;
                        }
                    }
                    if (!common)
                        index = lastIndex;
                }

                if (wList.isEmpty())
                    continue;

                // TODO use min1 and min2 as u and v and others should be sorted
                int[] wArray = new int[wList.size() + 1];
                wArray[0] = v;
                for (int i = 0; i < wList.size(); i++) {
                    wArray[i + 1] = wList.get(i);
                    list.add(new Tuple2<>(u, wArray));
                }
            }

            return list.iterator();
        }).groupByKey().mapValues(vIterable -> {
            Int2ObjectMap<int[]> map = new Int2ObjectOpenHashMap<>();
            for (int[] vList : vIterable) {
                int v = vList[0];
                int[] values = new int[vList.length - 1];
                System.arraycopy(vList, 1, values, 0, values.length);
                map.put(v, values);
            }

            int keySize = map.size();
            int[] keys = new int[keySize];
            IntIterator iterator = map.keySet().iterator();
            int i = 0;
            while (iterator.hasNext()) {
                keys[i++] = iterator.nextInt();
            }
            Arrays.sort(keys);
            int[][] values = new int[keySize][];
            for (i = 0; i < keys.length; i++) {
                values[i] = map.get(keys[i]);
                Arrays.sort(values[i]);
            }

            TriangleSubgraph ts = new TriangleSubgraph();
            ts.reset(keys, values);

            return ts;
        }).repartition(partition * CO_PARTITION_FACTOR).cache();

        JavaPairRDD<Tuple2<Integer, Integer>, int[]> edgeSupTriangle = triangleSets.flatMapToPair(p -> {
            int u = p._1;
            TriangleSubgraph ts = p._2;
            List<Tuple2<Tuple2<Integer, Integer>, int[]>> out = new ArrayList<>(p._2.keys.length * p._2.values.length);
            Long2IntOpenHashMap map = new Long2IntOpenHashMap(p._2.keys.length * p._2.values.length / 2);

            for (int i = 0; i < ts.keys.length; i++) {
                int v = ts.keys[i];
                long uv = (long) u << 32 | v & 0xFFFFFFFFL;
                int[] wArray = ts.values[i];
                map.compute(uv, (f1, f2) -> f2 == null ? wArray.length : f2.intValue() + wArray.length);

                for (int w : wArray) {
                    long uw = (long) u << 32 | w & 0xFFFFFFFFL;
                    map.compute(uw, (f1, f2) -> f2 == null ? 1 : f2.intValue() + 1);

                    long vw = (long) v << 32 | w & 0xFFFFFFFFL;
                    map.compute(vw, (f1, f2) -> f2 == null ? 1 : f2.intValue() + 1);
                }
            }

            for (Long2IntMap.Entry entry : map.long2IntEntrySet()) {
                long e = entry.getLongKey();
                int v1 = (int) (e >> 32);
                int v2 = (int) e;
                out.add(new Tuple2<>(new Tuple2<>(v1, v2), new int[]{entry.getIntValue(), u}));
            }

            return out.iterator();
        }).groupByKey().mapValues(value -> {
            int sup = 0;
            IntSet set = new IntOpenHashSet();
            for (int[] val : value) {
                sup += val[0];
                set.add(val[1]);
            }
            int[] result = new int[set.size() + 1];
            result[0] = sup;
            System.arraycopy(set.toIntArray(), 0, result, 1, set.size());
            return result;
        }).repartition(partition * CO_PARTITION_FACTOR).cache();

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> edgeSup = edgeSupTriangle.mapValues(value -> value[0]).cache();
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> prev = edgeSup;
        int iteration = 0;

        while (iteration < 4) {
            System.out.println("iteration: " + ++iteration);
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> invalids = edgeSup.filter(e -> e._2 < minSup).cache();
            long count = invalids.count();
            System.out.println("invalid count: " + count);
            if (count == 0)
                break;
            JavaPairRDD<Integer, Tuple2<List<Tuple2<Tuple2<Integer, Integer>, Integer>>, TriangleSubgraph>> update = invalids.join(edgeSupTriangle).flatMapToPair(f -> {
                int[] tKey = f._2._2;
                long e = (long) f._1._1 << 32 | f._1._2 & 0xFFFFFFFFL;
                List<Tuple2<Integer, Long>> list = new ArrayList<>(tKey.length);
                // skip sup which is in index 0
                for (int i = 1; i < tKey.length; i++) {
                    list.add(new Tuple2<>(tKey[i], e));
                }
                return list.iterator();
            }).groupByKey().join(triangleSets).mapToPair(f -> {
                List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>();
                Set<Tuple3<Integer, Integer, Integer>> invalidTriangles = new HashSet<>();
                TriangleSubgraph ts = f._2._2;
                for (long e : f._2._1) {
                    int v1 = (int) (e >> 32);
                    int v2 = (int) e;
                    int u = f._1;

                    if (v1 == u) {
                        int index = ts.getKeyIndex(v2);
                        if (index >= 0) {
                            for (int w : ts.values[index]) {
                                Tuple3<Integer, Integer, Integer> triangle = new Tuple3<>(v1, v2, w);
                                if (invalidTriangles.contains(triangle))
                                    continue;
                                output.add(new Tuple2<>(new Tuple2<>(v1, w), 1));
                                output.add(new Tuple2<>(new Tuple2<>(v2, w), 1));
                                invalidTriangles.add(triangle);
                            }
                            ts.removeKeyInIndex(index);
                        }
                        for (int i = 0; i < ts.keys.length; i++) {
                            if (ts.keys[i] == -1)
                                continue;

                            int valueIndex = ts.getValueIndex(i, v2);
                            if (valueIndex >= 0) {
                                Tuple3<Integer, Integer, Integer> triangle = new Tuple3<>(v1, ts.keys[i], v2);
                                if (invalidTriangles.contains(triangle))
                                    continue;
                                output.add(new Tuple2<>(new Tuple2<>(v1, ts.keys[i]), 1));
                                output.add(new Tuple2<>(new Tuple2<>(ts.keys[i], v2), 1));
                                invalidTriangles.add(triangle);
                                ts.removeValueIndexInKeyIndex(i, valueIndex);
                            }
                        }
                        continue;
                    }

                    int index = ts.getKeyIndex(v1);
                    if (index < 0)
                        continue;

                    int valueIndex = ts.getValueIndex(index, v2);
                    if (valueIndex < 0)
                        continue;

                    Tuple3<Integer, Integer, Integer> triangle = new Tuple3<>(u, v1, v2);
                    if (invalidTriangles.contains(triangle))
                        continue;
                    output.add(new Tuple2<>(new Tuple2<>(u, v1), 1));
                    output.add(new Tuple2<>(new Tuple2<>(u, v2), 1));
                    invalidTriangles.add(triangle);
                    ts.removeValueIndexInKeyIndex(index, valueIndex);
                }

                return new Tuple2<>(f._1, new Tuple2<>(output, ts));

            }).cache();

            triangleSets = update.mapValues(f -> f._2);
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> invalidSups = update.flatMapToPair(f -> f._2._1.iterator())
                .groupByKey().mapValues(t -> {
                    int sum = 0;
                    for (Integer x : t) {
                        sum++;
                    }
                    return sum;
                });

            edgeSup = edgeSup.filter(e -> e._2 >= minSup).cogroup(invalidSups)
                .flatMapToPair(t -> {
                    Iterator<Integer> prevSupIterator = t._2._1.iterator();
                    Iterator<Integer> invalidSupIterator = t._2._2.iterator();

                    if (!invalidSupIterator.hasNext())
                        return Arrays.asList(new Tuple2<>(t._1, prevSupIterator.next())).iterator();

                    if (!prevSupIterator.hasNext())
                        return Collections.emptyIterator();

                    int sumSup = invalidSupIterator.next();
                    while (invalidSupIterator.hasNext())
                        sumSup += invalidSupIterator.next();

                    int newSup = prevSupIterator.next() - sumSup;
                    if (newSup < 0)
                        newSup = 0;
//                        throw new RuntimeException("new sup < 0");
                    return Arrays.asList(new Tuple2<>(t._1, newSup)).iterator();
                }).cache();

//            System.out.println("iteration: " + iteration + " edgeSupCount: " + edgeSup.count());
            prev.unpersist();
            prev = edgeSup;
        }

        System.out.println("count, " + edgeSup.count());
        sc.close();
    }
}
