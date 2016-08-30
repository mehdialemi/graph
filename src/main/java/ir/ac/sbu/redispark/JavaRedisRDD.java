package ir.ac.sbu.redispark;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.reflect.ClassTag$class;
import scala.runtime.AbstractFunction1;

import java.util.ArrayList;
import java.util.Collections;

/**
 *
 */
public class JavaRedisRDD extends RDD<Tuple2<Long, Long>>{

    private final Pipeline pipeline;

    public JavaRedisRDD(SparkContext _sc, String host, int port, Seq<Dependency<?>> deps) {
        super(_sc, deps, ClassTag$.MODULE$.apply(Tuple2.class));
        pipeline = new Jedis(host, port, 60000).pipelined();
    }

    @Override
    public Iterator<Tuple2<Long, Long>> compute(Partition split, TaskContext context) {
        ClassTag<Tuple2> classTag = ClassTag$.MODULE$.apply(Tuple2.class);
        Iterator<Tuple2> it = firstParent(classTag).iterator(split, context);
        while(it.hasNext()) {
            Tuple2 tuple = it.next();
            pipeline.incr(((Long) tuple._1).toString());
            pipeline.incr(((Long) tuple._2).toString());
        }
        return null;
    }

    @Override
    public Partition[] getPartitions() {
        return new Partition[0];
    }
}
