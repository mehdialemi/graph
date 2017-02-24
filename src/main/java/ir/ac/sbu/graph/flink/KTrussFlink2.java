package ir.ac.sbu.graph.flink;


import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KTrussFlink2 {

    public static final TypeHint<Tuple2<Integer, Integer>> TUPLE_2_TYPE_HINT = new TypeHint<Tuple2<Integer, Integer>>() {
    };

    public static final TypeHint<Tuple3<Integer, Integer, Integer>> TUPLE_3_TYPE_HINT = new TypeHint<Tuple3<Integer, Integer, Integer>>() {
    };

    public static final TypeHint<Tuple2<Integer, int[]>> TUPLE_2_INT_ARRAY_TYPE_HINT = new TypeHint<Tuple2<Integer, int[]>>() {
    };

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";

        if (args.length > 0)
            inputPath = args[0];

        ExecutionConfig config = env.getConfig();
        config.enableObjectReuse();


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

                    for (Integer i : set) {
                        collector.collect(new Tuple3<>(i, v, set.size()));
                    }
                }
            }).returns(TUPLE_3_TYPE_HINT)
                .groupBy(0).reduceGroup(new GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, int[]>>() {
                final List<Tuple3<Integer, Integer, Integer>> list = new ArrayList<>();

                @Override
                public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> values, Collector<Tuple2<Integer, int[]>> collector)
                    throws Exception {
                    list.clear();

                    for (Tuple3<Integer, Integer, Integer> value : values) {
                        list.add(value);
                    }

                    if (list.size() == 0)
                        return;

                    int v = list.get(0).f0;
                    int deg = list.size();

                    for (int i = 0; i < list.size(); i++) {
                        Tuple3<Integer, Integer, Integer> tuple = list.get(i);
                        if (tuple.f2 > deg || (tuple.f2 == deg && tuple.f1 > tuple.f0))
                            continue;

                        list.remove(i);
                        i--;
                    }

                    Collections.sort(list, (a, b) -> a.f2 != b.f2 ? a.f2 - b.f2 : a.f1 - b.f1);

                    int[] higherDegs = new int[list.size() + 1];
                    higherDegs[0] = deg;
                    for (int i = 1; i < higherDegs.length; i++)
                        higherDegs[i] = list.get(i - 1).f1;

                    collector.collect(new Tuple2<>(v, higherDegs));
                }
            }).returns(TUPLE_2_INT_ARRAY_TYPE_HINT);

        long count = fonls.count();
        long endTime = System.currentTimeMillis();
        System.out.println("fonl count: " + count + ", duration: " + (endTime - startTime));
    }
}
