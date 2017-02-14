package ir.ac.sbu.graph.flink;


import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.*;

public class WordCount {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";

        DataSource<Tuple2<Integer, Integer>> edges = env.readCsvFile(inputPath)
            .fieldDelimiter("\t")
            .types(Integer.class, Integer.class);

        System.out.println("edge size: " + edges.count());

        GroupReduceOperator<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, int[]>> fonls = edges
            .groupBy(0)
            .reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                @Override
                public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple3<Integer, Integer, Integer>> collector)
                    throws Exception {
                    List<Integer> list = new ArrayList<>();
                    int v = -1;
                    for (Tuple2<Integer, Integer> i : iterable) {
                        v = i.f0;
                        list.add(i.f1);
                    }

                    if (v == -1)
                        throw new RuntimeException();

                    for (Integer i : list) {
                        collector.collect(new Tuple3<>(i, v, list.size()));
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

        System.out.println("fonl size: " + fonls.count());

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
                     }
            );

        fonls.print();

        CoGroupOperator<Tuple2<Integer, int[]>, Tuple2<Integer, int[]>, Tuple3<Integer, Integer, Integer>> edgeVertices =
            candidates.coGroup(fonls)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<Integer, int[]>, Tuple2<Integer, int[]>, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, int[]>> can, Iterable<Tuple2<Integer, int[]>> fonl,
                                        Collector<Tuple3<Integer, Integer, Integer>> collector) throws Exception {
                        Iterator<Tuple2<Integer, int[]>> fonli = fonl.iterator();
                        if (!fonli.hasNext())
                            throw new RuntimeException();

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
                                    collector.collect(new Tuple3<>(u, v, w));
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
                });
        System.out.println("EdgeCount: " + edgeVertices.count());

    }

}
