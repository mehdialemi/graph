package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.utils.Log;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;
import java.util.Map;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Generate graph statistics
 */
public class GraphStats extends SparkApp {

    private NeighborList neighborList;

    public GraphStats(NeighborList neighborList) {
        super(neighborList);
        this.neighborList = neighborList;
    }

    public Map<String, String> generateStats() {
        JavaPairRDD<Integer, int[]> neighbors = neighborList.create();

        Map<String, String> statMap = new HashMap<>();
        long vCount = neighbors.count();
        statMap.put("|V|", "" + vCount);

        JavaRDD<Long> degs = neighbors.mapValues(v -> v.length).map(kv -> Long.valueOf(kv._2)).cache();

        Long sumDeg = degs.reduce((a, b) -> a + b);
        long eCount = sumDeg / 2;
        statMap.put("|E|", "" + eCount);
        statMap.put("|E|/|V|", "" + eCount / (float) vCount);

        float avgDeg = sumDeg / vCount;
        statMap.put("AvgDeg", "" + avgDeg);

        Long maxDeg = degs.reduce((a, b) -> Math.max(a, b));
        statMap.put("MaxDeg", "" + maxDeg);

        Triangle triangle = new Triangle(neighborList);
        long tc = triangle.triangleCount();
        statMap.put("TC", "" + tc);

        LocalCC lcc = new LocalCC(triangle);
        statMap.put("AvgLcc", "" + lcc.avgLCC());

        return statMap;
    }

    public static void main(String[] args) {
        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args));
        conf.init();

        GraphStats gs = new GraphStats(new NeighborList(new EdgeLoader(conf)));
        Map<String, String> statMap = gs.generateStats();

        Log.setName(conf.getFileName());
        for (Map.Entry<String, String> entry : statMap.entrySet()) {
            log(entry.getKey() + ": " + entry.getValue());
        }

        gs.close();
    }
}
