package ir.ac.sbu.graph.utils;

import ir.ac.sbu.graph.spark.*;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class EdgeListToVertexEdgeList extends SparkApp  {

    public EdgeListToVertexEdgeList(SparkAppConf conf) {
        super(conf);
    }

    public void generate() throws FileNotFoundException {
        JavaRDD<String> input = conf.getSc().textFile(conf.getInputPath());
        int numPartitions = input.getNumPartitions();
        long count = input.count();
        final int partitionLines = (int) Math.ceil(count / (float)numPartitions);

        JavaRDD<Tuple2<Integer, String>> eId = input.mapPartitionsWithIndex((pId, f) -> {
            int start = pId * partitionLines;
            int idx = 0;
            List<Tuple2<Integer, String>> out = new ArrayList<>();
            while (f.hasNext()) {
                String edge = f.next();
                int edgeId = start + idx++;
                out.add(new Tuple2<>(edgeId, edge));
            }
            return out.iterator();
        }, true);

        JavaPairRDD<Integer, Integer> result = eId.flatMapToPair(kv -> {
            int e = kv._1;
            if (kv._2.startsWith("#"))
                return Collections.emptyIterator();
            String[] s = kv._2.split("\\s+");

            if (s == null || s.length != 2)
                return Collections.emptyIterator();

            int v1 = Integer.parseInt(s[0]);
            int v2 = Integer.parseInt(s[1]);

            if (v1 == v2)
                return Collections.emptyIterator();

            List<Tuple2<Integer, Integer>> list = new ArrayList<>();
            list.add(new Tuple2<>(v1, e));
            list.add(new Tuple2<>(v2, e));
            return list.iterator();
        });

        String outPath = conf.getInputPath() + ".edgelist";
        System.out.println("Deleting " + outPath);
        FileUtils.deleteQuietly(new File(outPath));
        System.out.println("New vertex edge list file in " + outPath);
        PrintWriter pw = new PrintWriter(new File(outPath));
        for (Tuple2<Integer, Iterable<Integer>> vEdgeList : result.groupByKey().collect()) {
            pw.print(vEdgeList._1 + "\t ");
            for (Integer edge : vEdgeList._2) {
                pw.print(edge + "\t ");
            }
            pw.println();
            pw.flush();
        }
        pw.flush();
    }

    public static void main(String[] args) throws FileNotFoundException {
        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args));
        conf.init();
        EdgeListToVertexEdgeList eToV = new EdgeListToVertexEdgeList(conf);
        eToV.generate();
    }
}
