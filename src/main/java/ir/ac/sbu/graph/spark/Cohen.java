package ir.ac.sbu.graph.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * KTruss algorithm proposed by Cohen implemented in Spark.
 * Ref: Cohen, Jonathan. "Graph twiddling in a mapreduce world." Computing in Science & Engineering 11.4 (2009): 29-41.
 */
public class Cohen extends SparkApp {

    public static final int INV_DEG = -1;
    public static final int P_MULTIPLIER = 5;
    private KTrussConf ktConf;
    private int partitions;

    public Cohen(KTrussConf ktConf) {
        super(ktConf);
        this.ktConf = ktConf;
    }

    public JavaRDD<Edge> generate(EdgeLoader edgeLoader) {

        JavaRDD<Edge> edgeList = edgeLoader.create()
                .map(kv -> kv._1 < kv._2 ? new Edge(kv._1, kv._2) : new Edge(kv._2, kv._1));
        partitions = edgeList.getNumPartitions() * P_MULTIPLIER;
        edgeList = edgeList.distinct(partitions)
                .distinct()
                .cache();


        final int minSup = ktConf.getKt() - 2;

        for (int iter = 0; iter < ktConf.getKtMaxIter(); iter++) {
            long t1 = System.currentTimeMillis();

            JavaRDD<Edge[]> triangles = createTriangles(edgeList);

            JavaPairRDD<Edge, Integer> eSup = triangles
                    .flatMapToPair(kv -> Arrays.asList(
                            new Tuple2<>(kv[0], 1), new Tuple2<>(kv[1], 1), new Tuple2<>(kv[2], 1)).iterator())
                    .reduceByKey((a, b) -> a + b);

            JavaPairRDD<Edge, Integer> invalids = eSup.filter(kv -> kv._2 < minSup);

            long invCount = invalids.count();
            long t2 = System.currentTimeMillis();
            log("iteration: " + (iter + 1) + ", invalid edge count: " + invCount, t1, t2);

            if (invCount == 0)
                break;

            edgeList = eSup.filter(kv -> kv._2 >= minSup).map(kv -> kv._1).cache();
        }

        return edgeList;
    }

    public JavaRDD<Edge[]> createTriangles(JavaRDD<Edge> edgeList) {

        // Find vertices and their corresponding edges. A key-value item contains a vertex as key and an edge as value.
        JavaPairRDD<Integer, Edge> vertexEdge = edgeList
                .flatMapToPair(e ->
                        Arrays.asList(new Tuple2<>(e.v1, e), new Tuple2<>(e.v2, e)).iterator());

        // Find an edge and degree related to one of its vertices.
        // First, collect all edges connected to a vertex by groupByKey() operator.
        // Second, since all neighbors of the current vertex are aggregated here, calculate degree of the vertex.
        // Third, construct key-value <Edge, VertexDegree> which each edge of the current vertex is as a key (Edge)
        // and the value (VertexDegree) is the degree of the current vertex. Here, values for all the keys are the
        // same.
        JavaPairRDD<Edge, int[]> edgeOneDeg = vertexEdge.groupByKey(partitions)
                .flatMapToPair(kv -> {
                    List<Tuple2<Edge, int[]>> list = new ArrayList<>();
                    Iterator<Edge> iterator = kv._2.iterator();

                    // Calculate degree and construct output key-value.
                    List<Edge> edges = new ArrayList<>();
                    while (iterator.hasNext())
                        edges.add(iterator.next());

                    int deg = edges.size();
                    int[] degIn1 = new int[]{deg, INV_DEG};
                    int[] degIn2 = new int[]{INV_DEG, deg};

                    for (Edge edge : edges)
                        list.add(kv._1 == edge.v1 ? new Tuple2<>(edge, degIn1) : new Tuple2<>(edge, degIn2));

                    return list.iterator();
                });

        // Find each edge and its corresponding degree of vertices. Here, key is an edge and value includes two
        // integers that first one is the degree of first vertex of the edge and second one is the degree of the
        // second vertex of the edge.
        JavaPairRDD<Edge, int[]> edgeTwoDegs = edgeOneDeg.groupByKey(partitions).mapValues(degs -> {
            Iterator<int[]> iterator = degs.iterator();
            int[] deg = iterator.next();
            int[] deg2 = iterator.next();

            if (deg[0] == INV_DEG)
                deg[0] = deg2[0];
            else
                deg[1] = deg2[1];
            return deg;
        }).persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<Integer, Edge> lowDegVertex = edgeTwoDegs
                .mapToPair(kv -> {
                    if (kv._2[0] < kv._2[1])
                        return new Tuple2<>(kv._1.v1, kv._1);
                    else if (kv._2[0] > kv._2[1])
                        return new Tuple2<>(kv._1.v2, kv._1);
                    else
                        return new Tuple2<>(kv._1.v1, kv._1);
                });

        JavaPairRDD<Edge, Edge[]> triad = lowDegVertex.groupByKey(partitions)
                .flatMapToPair(kv -> {
                    List<Edge> edges = new ArrayList<>();
                    for (Edge edge : kv._2)
                        edges.add(edge);

                    List<Tuple2<Edge, Edge[]>> out = new ArrayList<>();
                    for (int i = 0; i < edges.size(); i++) {
                        Edge e1 = edges.get(i);
                        for (int j = i + 1; j < edges.size(); j++) {
                            Edge e2 = edges.get(j);

                            int v1 = kv._1 == e1.v1 ? e1.v2 : e1.v1;
                            int v2 = kv._1 == e2.v1 ? e2.v2 : e2.v1;

                            Edge e = v1 < v2 ? new Edge(v1, v2) : new Edge(v2, v1);

                            out.add(new Tuple2<>(e, new Edge[]{e1, e2}));
                        }
                    }

                    return out.iterator();
                });

        JavaRDD<Edge[]> triangles = triad.join(edgeTwoDegs)
                .map(kv -> new Edge[]{kv._1, kv._2._1[0], kv._2._1[1]});

        return triangles;
    }

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spar").setLevel(Level.INFO);
        long t1 = System.currentTimeMillis();
        KTrussConf ktConf = new KTrussConf(new ArgumentReader(args)) {
            @Override
            protected String createAppName() {
                return "KTruss-Cohen-" + super.getKt() + "-(" + super.getFileName() +")";
            }
        };

        ktConf.init();

        Cohen cohen = new Cohen(ktConf);
        JavaRDD<Edge> subgraph = cohen.generate(new EdgeLoader(ktConf));
        log("KTruss edge count: " + subgraph.count(), t1, System.currentTimeMillis());

    }
}
