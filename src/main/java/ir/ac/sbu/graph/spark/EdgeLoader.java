package ir.ac.sbu.graph.spark;

import org.apache.spark.api.java.JavaPairRDD;
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

    public JavaPairRDD <Integer, Integer> create() {
        return conf.getSc()
                .textFile(conf.getInputPath(), conf.getPartitionNum())
                .flatMapToPair(line -> {
                    if (line.startsWith("#"))
                        return Collections.emptyIterator();
                    String[] s = line.split("\\s+");

                    if (s.length != 2)
                        return Collections.emptyIterator();

                    int e1 = Integer.parseInt(s[0]);
                    int e2 = Integer.parseInt(s[1]);

                    if (e1 == e2)
                        return Collections.emptyIterator();

                    List <Tuple2 <Integer, Integer>> list = new ArrayList <>();
                    list.add(new Tuple2 <>(e1, e2));
                    list.add(new Tuple2 <>(e2, e1));
                    return list.iterator();
                });
    }
}
