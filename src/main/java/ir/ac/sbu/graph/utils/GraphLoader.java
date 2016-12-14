package ir.ac.sbu.graph.utils;

import ir.ac.sbu.graph.Edge;
import it.unimi.dsi.fastutil.BigArrays;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Tries to load ir.ac.sbu.graph into a key value based structure.
 */
public class GraphLoader {

    public static Edge[] loadFromLocalFile(String inputPath) throws IOException {
        System.out.println("Loading " + inputPath);

        List<List<Edge>> edgeLists = new ArrayList<>();
        final FileChannel channel = new FileInputStream(inputPath).getChannel();
        long tr1 = System.currentTimeMillis();

        long offset = 0;
        int batchSize = Integer.MAX_VALUE / 2;
        while (offset < channel.size()) {
            long end = Math.min(offset + batchSize, offset + channel.size());
            MappedByteBuffer mapBB = channel.map(FileChannel.MapMode.READ_ONLY, offset, end);
            byte[] buf = new byte[(int) (end - offset)];
            mapBB.get(buf);
            ByteArrayInputStream isr = new ByteArrayInputStream(buf);
            InputStreamReader ip = new InputStreamReader(isr);
            BufferedReader reader = new BufferedReader(ip);
            List<Edge> edgeList = new ArrayList<>(buf.length / 20);
            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;
                if (line.startsWith("#"))
                    continue;
                String[] split = line.split("\\s+");
                if (split == null || split.length != 2)
                    continue;
                
                edgeList.add(new Edge(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
            }
            edgeLists.add(edgeList);
            offset += end;
        }

        int size = 0;
        for (List<Edge> edgeList : edgeLists) {
            size += edgeList.size();
        }

        Edge[] edges = new Edge[size];
        int start = 0;
        for (List<Edge> edgeList : edgeLists) {
            Edge[] edgeArray = edgeList.toArray(new Edge[0]);
            System.arraycopy(edgeArray, 0, edges, start, edgeArray.length);
            start += edgeArray.length;
        }

        long tr2 = System.currentTimeMillis();
        System.out.println("Graph loaded, edges: " + edges.length + ", load time: " + (tr2 - tr1) + " ms");

        return edges;
    }

    public static JavaPairRDD<Long, Long> loadEdges(JavaRDD<String> input) {
        JavaPairRDD<Long, Long> edges = input.flatMapToPair((PairFlatMapFunction<String, Long, Long>) line -> {
            if (line.startsWith("#"))
                return Collections.emptyIterator();

            String[] s = line.split("\\s+");
            if (s == null || s.length != 2)
                return Collections.emptyIterator();


            long e1 = Long.parseLong(s[0]);
            long e2 = Long.parseLong(s[1]);

            if (e1 == e2)
                return Collections.emptyIterator();

            List<Tuple2<Long, Long>> list = new ArrayList<>();
            list.add(new Tuple2<>(e1, e2));
            list.add(new Tuple2<>(e2, e1));
            return list.iterator();
        });
        return edges;
    }

    public static JavaPairRDD<Integer, Integer> loadEdgesInt(JavaRDD<String> input) {
        JavaPairRDD<Integer, Integer> edges = input.flatMapToPair(new PairFlatMapFunction<String, Integer, Integer>() {

            @Override
            public Iterator<Tuple2<Integer, Integer>> call(String line) throws Exception {
                if (line.startsWith("#"))
                    return Collections.emptyIterator();
                String[] s = line.split("\\s+");

                if (s == null || s.length != 2)
                    return Collections.emptyIterator();

                int e1 = Integer.parseInt(s[0]);
                int e2 = Integer.parseInt(s[1]);

                if (e1 == e2)
                    return Collections.emptyIterator();

                List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                list.add(new Tuple2<>(e1, e2));
                list.add(new Tuple2<>(e2, e1));
                return list.iterator();
            }
        });
        return edges;
    }

    public static JavaRDD<Tuple2<Long, Long>> loadEdgeListSorted(JavaRDD<String> input) {
        return input.flatMap(line -> {
            if (line.startsWith("#"))
                return Collections.emptyIterator();

            List<Tuple2<Long, Long>> list = new ArrayList<>(1);
            String[] e = line.split("\\s+");
            if (e == null || e.length != 2)
                return Collections.emptyIterator();

            long v1 = Long.parseLong(e[0]);
            long v2 = Long.parseLong(e[1]);

            if (v1 == v2)
                return Collections.emptyIterator(); // self loop

            if (v1 < v2)
                list.add(new Tuple2<>(v1, v2));
            else if (v1 > v2)
                list.add(new Tuple2<>(v2, v1));
            return list.iterator(); // no self loop is accepted
        });
    }
}
