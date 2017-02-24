package ir.ac.sbu.graph.flink;


import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.*;

public class KTrussFlink {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        //        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";

        if (args.length > 0)
            inputPath = args[0];

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[1]);
        final int minSup = k - 2;

        long startTime = System.currentTimeMillis();
        DataSource<Tuple2<Integer, Integer>> edges = env.readCsvFile(inputPath)
            .fieldDelimiter("\t").ignoreComments("#")
            .types(Integer.class, Integer.class);

        GroupReduceOperator<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, int[]>> fonls = edges
            .flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                @Override
                public void flatMap(Tuple2<Integer, Integer> e, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                    collector.collect(e);
                    collector.collect(e.swap());
                }
            })
            .groupBy(0)
            .reduceGroup(
                new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple3<Integer, Integer, Integer>> collector)
                        throws Exception {
                        IntSet set = new IntOpenHashSet();
                        int v = -1;
                        for (Tuple2<Integer, Integer> i : iterable) {
                            v = i.f0;
                            set.add(i.f1);
                        }

                        if (v == -1)
                            throw new RuntimeException();

                        for (Integer i : set) {
                            collector.collect(new Tuple3<>(i, v, set.size()));
                        }
                    }
                }).groupBy(0)
            .reduceGroup(
                new GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, int[]>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> iterable, Collector<Tuple2<Integer, int[]>> collector)
                        throws Exception {
                        int v = -1;
                        List<Tuple3<Integer, Integer, Integer>> list = new ArrayList<>();
                        for (Tuple3<Integer, Integer, Integer> i : iterable) {
                            list.add(i);
                            v = i.f0;
                        }
                        if (v == -1)
                            throw new RuntimeException();

                        int deg = list.size();

                        for (int i = 0; i < list.size(); i++) {
                            Tuple3<Integer, Integer, Integer> tuple = list.get(i);
                            if (tuple.f2 > deg || (tuple.f2 == deg && tuple.f1 > tuple.f0))
                                continue;

                            list.remove(i);
                            i--;
                        }

                        Collections.sort(list, (a, b) -> {
                            int x, y;
                            if (a.f2 != b.f2) {
                                x = a.f2;
                                y = b.f2;
                            } else {
                                x = a.f1;
                                y = b.f1;
                            }
                            return x - y;
                        });
                        int[] higherDegs = new int[list.size() + 1];
                        higherDegs[0] = deg;
                        for (int i = 1; i < higherDegs.length; i++)
                            higherDegs[i] = list.get(i - 1).f1;

                        collector.collect(new Tuple2<>(v, higherDegs));
                    }
                }
            );

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

        GroupReduceOperator<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, int[]>> edgeVertices =
            candidates.coGroup(fonls)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<Integer, int[]>, Tuple2<Integer, int[]>, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, int[]>> can, Iterable<Tuple2<Integer, int[]>> fonl,
                                        Collector<Tuple3<Integer, Integer, Integer>> collector) throws Exception {
                        Iterator<Tuple2<Integer, int[]>> fonli = fonl.iterator();

                        Tuple2<Integer, int[]> fItem = fonli.next();
                        int v = fItem.f0;
                        int[] fVal = fItem.f1;
                        Arrays.sort(fVal, 1, fVal.length);
                        Iterator<Tuple2<Integer, int[]>> cani = can.iterator();
                        while (cani.hasNext()) {
                            int[] cVal = cani.next().f1;
                            int u = cVal[0];
                            scala.Tuple2<Integer, Integer> uv;
                            if (u < v)
                                uv = new scala.Tuple2<>(u, v);
                            else
                                uv = new scala.Tuple2<>(v, u);

                            // The intersection determines triangles which u and v are two of their vertices.
                            // Always generate and edge (u, v) such that u < v.
                            int fi = 1;
                            int ci = 1;
                            while (fi < fVal.length && ci < cVal.length) {
                                if (fVal[fi] < cVal[ci])
                                    fi++;
                                else if (fVal[fi] > cVal[ci])
                                    ci++;
                                else {
                                    int w = fVal[fi];
                                    collector.collect(new Tuple3<>(uv._1, uv._2, w));
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
                        }
                    }
                }).groupBy(0, 1).reduceGroup(
                new GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, int[]>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> iterable,
                                       Collector<Tuple2<Tuple2<Integer, Integer>, int[]>> collector) throws Exception {
                        IntList list = new IntArrayList();

                        Tuple2<Integer, Integer> key = null;
                        for (Tuple3<Integer, Integer, Integer> i : iterable) {
                            if (key == null) {
                                key = new Tuple2<>(i.f0, i.f1);
                            }
                            list.add(i.f2.intValue());
                        }
                        collector.collect(new Tuple2<>(key, list.toIntArray()));
                    }
                });

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
            Tuple2<Tuple2<Integer, Integer>, int[]>> update = invUpdates.coGroup(iteration)
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

        DataSet<Tuple2<Tuple2<Integer, Integer>, int[]>> result = iteration.closeWith(update, invUpdates);

        System.out.println("Result: " + result.distinct().count());
        long endTime = System.currentTimeMillis();
        System.out.println("duration: " + (endTime - startTime));
    }
}
