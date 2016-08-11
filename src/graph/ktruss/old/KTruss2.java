package graph.ktruss.old;

import graph.GraphOps;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KTruss2 implements Serializable {

	private JavaRDD<Triangle> allTriangles;
	private final int support;

	public KTruss2(JavaRDD<Triangle> triangles, int k) {
		this.allTriangles = triangles;
		support = k - 2;
	}

	public JavaRDD<SimpleEdge> findTriangles() {
		JavaRDD<Triangle> triangles = allTriangles;
		while(true) {
			triangles.persist(StorageLevel.MEMORY_AND_DISK());
			JavaPairRDD<Edge, Integer> edgeCount = triangles
				.flatMapToPair(new PairFlatMapFunction<Triangle, Edge, Integer>() {
				@Override
				public Iterator<Tuple2<Edge, Integer>> call(Triangle t) throws Exception {
					List<Tuple2<Edge, Integer>> edge = new ArrayList<>();
					edge.add(new Tuple2<>(new Edge(t.first, t.second, t.third), 1));
					edge.add(new Tuple2<>(new Edge(t.first, t.third, t.second), 1));
					edge.add(new Tuple2<>(new Edge(t.second, t.third, t.first), 1));
					return edge.iterator();
				}
			});

			JavaRDD<Triangle> removeTriangles = edgeCount.reduceByKey((a , b) -> a + b)
			.filter(f -> f._2 < support)
			.map(t -> t._1.createTriangle());

			if (removeTriangles.count() == 0)
				break;

            JavaRDD<Triangle> oldTriangles = triangles;
			triangles = triangles.subtract(removeTriangles);
            oldTriangles.unpersist();
		}
		
		return triangles.flatMap(t -> {
			List<SimpleEdge> edgeList = new ArrayList<>();
			edgeList.add(new SimpleEdge(t.first, t.second));
			edgeList.add(new SimpleEdge(t.first, t.third));
			edgeList.add(new SimpleEdge(t.second, t.third));
			return edgeList.iterator();
		}).distinct();
	}

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("KTruss FDONL");
        conf.setMaster("local");
        conf.registerKryoClasses(new Class[] { KTruss.class, SimpleEdge.class, Edge.class, Triangle.class, FdValue.class, long[].class });
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        int k = 4; // k-truss
        String inputFile = "input.txt";

        if (args.length > 0)
            k = Integer.parseInt(args[0]);

        if (args.length > 1)
            inputFile = args[1];

        JavaPairRDD<Long, Iterable<Long>> neighborList = GraphOps.getNeighbors(sc.textFile(inputFile));
        System.out.println("NeighborList:");

        Fdonl fdonl = new Fdonl(neighborList);
        JavaPairRDD<Long, FdValue> fdonlResult = fdonl.createFdonl();

        KCore kCore = new KCore(fdonlResult, sc, k - 1);
        JavaPairRDD<Long, FdValue> kCoreResult = kCore.findKCore();

        TriangleListGenerator triangleGenerator = new TriangleListGenerator(kCoreResult);
        JavaRDD<Triangle> triangles = triangleGenerator.findTriangles();

        KTruss2 ktruss2 = new KTruss2(triangles, k);
        JavaRDD<SimpleEdge> result = ktruss2.findTriangles();


        System.out.println("Remaining graph edge count: " + result.count());
        System.out.println("Edges: ");
        result.collect().forEach(r -> System.out.println(r));
    }
}
