package ir.ac.sbu.graph.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Load edge list from input
 */
public class EdgeLoader extends SparkApp {

    public EdgeLoader(SparkAppConf conf) {
        super(conf);
    }

    public JavaPairRDD<Integer, Integer> create() {
        JavaRDD<String> input = conf.getSc().textFile(conf.getInputPath());

        JavaPairRDD<Integer, Integer> result = input.flatMapToPair(line -> {
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
        });

        conf.setPartitionNum(input.getNumPartitions() * 2);

        return result;
    }
}
