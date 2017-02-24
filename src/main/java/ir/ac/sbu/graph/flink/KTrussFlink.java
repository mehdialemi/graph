package ir.ac.sbu.graph.flink;


import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.*;

public class KTrussFlink {

    public static final TypeHint<Tuple2<Integer, Integer>> TUPLE_2_TYPE_HINT = new TypeHint<Tuple2<Integer, Integer>>() {
    };

    public static final TypeHint<Tuple3<Integer, Integer, Integer>> TUPLE_3_TYPE_HINT = new TypeHint<Tuple3<Integer, Integer, Integer>>() {
    };

    public static final TypeHint<Tuple2<Integer, int[]>> TUPLE_2_INT_ARRAY_TYPE_HINT = new TypeHint<Tuple2<Integer, int[]>>() {
    };
    public static final TypeHint<Tuple2<Tuple2<Integer, Integer>, int[]>> TYPE_TUPLE2_INT_ARRAY = new TypeHint<Tuple2<Tuple2<Integer, Integer>, int[]>>() {
    };
    public static final TypeHint<Tuple2<Tuple2<Integer, Integer>, Integer>> TYPE_TUPLE2_INT = new TypeHint<Tuple2<Tuple2<Integer, Integer>, Integer>>() {
    };
    public static final TypeHint<Tuple2<Tuple2<Integer, Integer>, Tuple2<int[], int[]>>> TYPE_TUPLE2_2INT_ARRAY = new TypeHint<Tuple2<Tuple2<Integer, Integer>, Tuple2<int[], int[]>>>() {
    };

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";

        if (args.length > 0)
            inputPath = args[0];

        int k = 4; // k-truss
        if (args.length > 1)
            k = Integer.parseInt(args[1]);
        final int minSup = k - 2;

        env.getConfig().enableForceKryo();
//        env.getConfig().enableObjectReuse();
        env.getConfig().registerPojoType(int[].class);

        long startTime = System.currentTimeMillis();
        FlatMapOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> edges = env.readCsvFile(inputPath)
            .fieldDelimiter("\t").ignoreComments("#")
            .types(Integer.class, Integer.class)
            .flatMap((Tuple2<Integer, Integer> t, Collector<Tuple2<Integer, Integer>> collector) -> {
                collector.collect(t);
                collector.collect(t.swap());
            }).returns(TUPLE_2_TYPE_HINT);

        GroupReduceOperator<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, int[]>> fonls =
            edges.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                final IntSet set = new IntOpenHashSet();

                @Override
                public void reduce(Iterable<Tuple2<Integer, Integer>> values, Collector<Tuple3<Integer, Integer, Integer>> collector)
                    throws Exception {
                    set.clear();
                    int v = -1;
                    for (Tuple2<Integer, Integer> value : values) {
                        v = value.f0;
                        set.add(value.f1);
                    }

                    if (set.size() == 0)
                        return;

                    for (Integer i : set) {
                        collector.collect(new Tuple3<>(i, v, set.size()));
                    }
                }
            }).returns(TUPLE_3_TYPE_HINT)
                .groupBy(0).reduceGroup(new GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, int[]>>() {
                final List<Tuple3<Integer, Integer, Integer>> list = new ArrayList<>();
                final List<Tuple3<Integer, Integer, Integer>> holds = new ArrayList<>();

                @Override
                public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> values, Collector<Tuple2<Integer, int[]>> collector)
                    throws Exception {

                    list.clear();
                    for (Tuple3<Integer, Integer, Integer> value : values) {
                        list.add(value);
                    }

                    int v = list.get(0).f0;
                    int deg = list.size();
                    holds.clear();

                    for (int i = 0; i < deg; i++) {
                        Tuple3<Integer, Integer, Integer> tuple = list.get(i);
                        if (tuple.f2 > deg || (tuple.f2 == deg && tuple.f1 > tuple.f0))
                            holds.add(tuple);
                    }

                    Collections.sort(holds, (a, b) -> a.f2 != b.f2 ? a.f2 - b.f2 : a.f1 - b.f1);

                    int[] higherDegs = new int[holds.size() + 1];
                    higherDegs[0] = deg;
                    for (int i = 1; i < higherDegs.length; i++)
                        higherDegs[i] = holds.get(i - 1).f1;

                    collector.collect(new Tuple2<>(v, higherDegs));
                }
            }).returns(TUPLE_2_INT_ARRAY_TYPE_HINT);

        FlatMapOperator<Tuple2<Integer, int[]>, Tuple2<Integer, int[]>> candidates = fonls.filter(t -> t.f1.length > 2)
            .flatMap(new FlatMapFunction<Tuple2<Integer, int[]>, Tuple2<Integer, int[]>>() {
                @Override
                public void flatMap(Tuple2<Integer, int[]> t, Collector<Tuple2<Integer, int[]>> collector) throws Exception {
                    int size = t.f1.length - 1;
                    if (size == 1)
                        return;
                    for (int index = 1; index < size; index++) {
                        int len = size - index;
                        int[] cvalue = new int[len + 1];
                        cvalue[0] = t.f0; // First vertex in the triangle
                        System.arraycopy(t.f1, index + 1, cvalue, 1, len);
                        Arrays.sort(cvalue, 1, cvalue.length); // quickSort to comfort with fonl
                        collector.collect(new Tuple2<>(t.f1[index], cvalue));
                    }
                }
            });

        GroupReduceOperator<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, int[]>> edgeVertices = fonls.map(t -> {
            Arrays.sort(t.f1, 1, t.f1.length);
            return t;
        }).returns(TUPLE_2_INT_ARRAY_TYPE_HINT).join(candidates)
            .where(0)
            .equalTo(0)
            .with((FlatJoinFunction<Tuple2<Integer, int[]>, Tuple2<Integer, int[]>, Tuple3<Integer, Integer, Integer>>)
                (first, second, collector) -> {
                    int[] fonl = first.f1;
                    int[] can = second.f1;
                    int v = first.f0;
                    int u = can[0];
                    Tuple2<Integer, Integer> uv;
                    if (u < v)
                        uv = new Tuple2<>(u, v);
                    else
                        uv = new Tuple2<>(v, u);

                    // The intersection determines triangles which u and v are two of their vertices.
                    // Always generate and edge (u, v) such that u < v.
                    int fi = 1;
                    int ci = 1;
                    while (fi < fonl.length && ci < can.length) {
                        if (fonl[fi] < can[ci])
                            fi++;
                        else if (fonl[fi] > can[ci])
                            ci++;
                        else {
                            int w = fonl[fi];
                            collector.collect(new Tuple3<>(uv.f0, uv.f1, w));
                            if (u < w)
                                collector.collect(new Tuple3<>(u, w, v));
                            else
                                collector.collect(new Tuple3<>(w, u, v));

                            if (v < w)
                                collector.collect(new Tuple3<>(v, w, u));
                            else
                                collector.collect(new Tuple3<>(w, v, u));
                            fi++;
                            ci++;
                        }
                    }
                }).returns(TUPLE_3_TYPE_HINT).groupBy(0, 1).reduceGroup((Iterable<Tuple3<Integer, Integer, Integer>> values,
                                                                         Collector<Tuple2<Tuple2<Integer, Integer>, int[]>> collector) -> {
                IntList list = new IntArrayList();

                Tuple2<Integer, Integer> key = null;
                for (Tuple3<Integer, Integer, Integer> i : values) {
                    if (key == null) {
                        key = new Tuple2<>(i.f0, i.f1);
                    }
                    list.add(i.f2.intValue());
                }
                collector.collect(new Tuple2<>(key, list.toIntArray()));
            }).returns(TYPE_TUPLE2_INT_ARRAY);


        IterativeDataSet<Tuple2<Tuple2<Integer, Integer>, int[]>> iteration = edgeVertices.iterate(100);

        FlatMapOperator<Tuple2<Tuple2<Integer, Integer>, int[]>, Tuple2<Tuple2<Integer, Integer>, Integer>> invUpdates =
            iteration
                .filter(e -> e.f1.length < minSup)
                .flatMap(new FlatMapFunction<Tuple2<Tuple2<Integer, Integer>, int[]>, Tuple2<Tuple2<Integer, Integer>, Integer>>() {

                    @Override
                    public void flatMap(Tuple2<Tuple2<Integer, Integer>, int[]> kv,
                                        Collector<Tuple2<Tuple2<Integer, Integer>, Integer>> collector)
                        throws Exception {
                        int u = kv.f0.f0;
                        int v = kv.f0.f1;
                        for (int i = 0; i < kv.f1.length; i++) {
                            int w = kv.f1[i];
                            if (w < u)
                                collector.collect(new Tuple2<>(new Tuple2<>(w, u), v));
                            else
                                collector.collect(new Tuple2<>(new Tuple2<>(u, w), v));

                            if (w < v)
                                collector.collect(new Tuple2<>(new Tuple2<>(w, v), u));
                            else
                                collector.collect(new Tuple2<>(new Tuple2<>(v, w), u));
                        }
                    }
                });

        CoGroupOperator<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Tuple2<Integer, Integer>, int[]>,
            Tuple2<Tuple2<Integer, Integer>, int[]>> newEdgeVertices = invUpdates.coGroup(iteration)
            .where(0)
            .equalTo(0)
            .with(new CoGroupFunction<Tuple2<Tuple2<Integer, Integer>, Integer>,
                Tuple2<Tuple2<Integer, Integer>, int[]>, Tuple2<Tuple2<Integer, Integer>, int[]>>() {

                @Override
                public void coGroup(Iterable<Tuple2<Tuple2<Integer, Integer>, Integer>> inv,
                                    Iterable<Tuple2<Tuple2<Integer, Integer>, int[]>> prev,
                                    Collector<Tuple2<Tuple2<Integer, Integer>, int[]>> collector) throws Exception {
                    Iterator<Tuple2<Tuple2<Integer, Integer>, int[]>> fi = prev.iterator();
                    if (!fi.hasNext())
                        return;

                    Tuple2<Tuple2<Integer, Integer>, int[]> fkv = fi.next();
                    if (fkv.f1.length < minSup)
                        return;

                    IntSet set = new IntOpenHashSet();
                    for (Tuple2<Tuple2<Integer, Integer>, Integer> s : inv) {
                        set.add(s.f1);
                    }

                    if (set.size() == 0) {
                        collector.collect(fkv);
                        return;
                    }

                    int validLen = fkv.f1.length - set.size();
                    if (validLen == 0)
                        return;

                    int[] out = new int[validLen];
                    int i = 0;
                    for (int valid : fkv.f1) {
                        if (set.contains(valid))
                            continue;
                        out[i ++] = valid;
                    }

                    collector.collect(new Tuple2<>(fkv.f0, out));
                }
            });

        DataSet<Tuple2<Tuple2<Integer, Integer>, int[]>> result = iteration.closeWith(newEdgeVertices, invUpdates);


        System.out.println("Result: " + result.distinct().count());
        long endTime = System.currentTimeMillis();
        System.out.println("duration: " + (endTime - startTime));
    }
}
