package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.Triangle;
import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class KTrussSparkTriangleCompress {

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

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceNoSortInt(edges, partition);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl, true, false);
        JavaPairRDD<Integer, Triangle> triangles = candidates.cogroup(fonl).flatMapToPair(t -> {
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
            IntSet set = new IntOpenHashSet();
            for (int[] vList : vIterable) {
                int v = vList[0];
                for (int i = 1; i < vList.length; i++) {
                    set.add(vList[i]);
                }

                map.put(v, set.toArray(new int[0]));
                set.clear();
            }

            int size = map.size();
            int[] keys = new int[size];
            IntIterator iterator = map.keySet().iterator();
            int i = 0;
            while (iterator.hasNext()) {
                keys[i++] = iterator.nextInt();
            }
            Arrays.sort(keys);
            int[][] values = new int[size][];
            for (i = 0; i < keys.length; i++) {
                values[i] = map.get(keys[i]);
            }

            Triangle sg = new Triangle();
            sg.reset(keys, values);

            return sg;
        }).repartition(partition * CO_PARTITION_FACTOR).cache();

        JavaPairRDD<Tuple2<Integer, Integer>, int[]> edgeSupTriangle = triangles.flatMapToPair(p -> {
            int u = p._1;
            Triangle triangle = p._2;
            List<Tuple2<Tuple2<Integer, Integer>, int[]>> list = new ArrayList<>(p._2.keys.length * p._2.values.length);
            Long2IntOpenHashMap map = new Long2IntOpenHashMap(p._2.keys.length * p._2.values.length / 2);

            for (int i = 0; i < triangle.keys.length; i++) {
                int v = triangle.keys[i];
//                Tuple2<Integer, Integer> uv = new Tuple2<>(u, v);
                long uv = (long) u << 32 | v & 0xFFFFFFFFL;
                map.put(uv, triangle.values.length);

                //list.add(new Tuple2<>(uv, triangle.values.length));

                int[] wArray = triangle.values[i];
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
                list.add(new Tuple2<>(new Tuple2<>(v1, v2), new int[] {entry.getIntValue(), u}));
            }

            return list.iterator();
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

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> edgeSup = edgeSupTriangle.mapValues(value -> value[0]);

        System.out.println("count, " + edgeSup.count());
        sc.close();
    }
}
