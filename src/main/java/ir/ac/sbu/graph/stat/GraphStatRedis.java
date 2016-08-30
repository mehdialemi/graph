package ir.ac.sbu.graph.stat;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import ir.ac.sbu.graph.GraphLoader;
import ir.ac.sbu.graph.GraphUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

/**
 *
 */
public class GraphStatRedis {

//    static RedisAsyncCommands<String, String> connection;

    public static void main(String[] args) throws IOException {

//        RedisClient redisClient = RedisClient.create(RedisURI.create("redis://localhost:6379/0"));
//        connection = redisClient.connect().async();
//        connection.set("1", "0");

        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        String dataset = new File(inputPath).getName();
        int index = inputPath.indexOf(dataset);
        String parentOutput = inputPath.substring(0, index);

        String outputPath = parentOutput + "/dist/" + dataset;
        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");

        GraphUtils.setAppName(conf, "Dist", partition, inputPath);

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);

        edges.filter(new Function<Tuple2<Long, Long>, Boolean>() {
            RedisAsyncCommands<String, String> connection;

            {
                RedisClient redisClient = RedisClient.create(RedisURI.create("redis://localhost:6379/0"));
                connection = redisClient.connect().async();
            }

            @Override
            public Boolean call(Tuple2<Long, Long> t) throws Exception {
                connection.incr(t._1().toString());
                return false;
            }
        }).count();

        sc.close();
    }
}
