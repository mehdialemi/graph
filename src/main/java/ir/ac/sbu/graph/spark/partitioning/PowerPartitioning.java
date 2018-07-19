package ir.ac.sbu.graph.spark.partitioning;

import ir.ac.sbu.graph.spark.*;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.spark_project.guava.collect.Multiset;
import org.spark_project.guava.collect.TreeMultiset;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class PowerPartitioning extends SparkApp {
    public static final int INIT = 0;
    public static final int UN_ASSIGNDED = -1;
    private NeighborList neighborList;

    public PowerPartitioning(NeighborList neighborList) {
        super(neighborList);
        this.neighborList = neighborList;
    }

    private int excludedSumDeg = 0;

    public int getExcludedSumDeg() {
        return excludedSumDeg * 2;
    }

    private int[] createSortedHighDegs(JavaPairRDD<Integer, int[]> neighborList, int minHighDegCount,
                                       int highDegSkipStep, int pLawDegThr) {

        final List<Tuple2<Integer, Integer>> collect = neighborList
                .mapToPair(kv -> new Tuple2<>(kv._2.length, 1))
                .reduceByKey((a, b) -> a + b).collect();

        List<Tuple2<Integer, Integer>> arrayList = new ArrayList<>(collect);
        Collections.sort(arrayList, (a, b) -> (b._1 - a._1));

        int highDegNum = 0;
        final List<Tuple2<Integer, Integer>> highDegs = new ArrayList<>();
        int min = Integer.MAX_VALUE, max = 0;

        boolean exclude = excludedSumDeg == 0 ? true : false;
        for (Tuple2<Integer, Integer> entry : arrayList) {
            highDegNum += entry._2;
            int deg = entry._1;
            min = Math.min(deg, min);
            max = Math.max(deg, max);
            if (highDegNum < pLawDegThr) {
                if (exclude)
                    excludedSumDeg += deg * entry._2;
                continue;
            }
            highDegs.add(entry);
            if (max > min && highDegNum > (minHighDegCount + pLawDegThr)) {
                break;
            }
        }

        highDegNum = 0;
        for (int i = 0; i < highDegs.size(); i++) {
            highDegNum += highDegs.get(i)._2;
            if (highDegNum > highDegSkipStep) {
                highDegNum = 0;
            } else {
                highDegs.remove(i);
                i--;
            }
        }

        final int[] sortedHighDegs = new int[highDegs.size()];
        for (int i = 0; i < highDegs.size(); i++) {
            sortedHighDegs[i] = highDegs.get(i)._1;
        }
        Arrays.sort(sortedHighDegs);
        return sortedHighDegs;
    }

    private Broadcast<Map<Integer, Integer>> createPartitionInfoBroadcast(
            JavaPairRDD<Integer, Integer> partitions, JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>> agents,
            JavaSparkContext sc) {
        Map<Integer, Integer> partitionInfoMap = partitions.union(agents.mapValues(v -> v._2()))
                .mapToPair(kv -> new Tuple2<>(kv._2, 1))
                .reduceByKey((a, b) -> a + b).collectAsMap();
        return sc.broadcast(partitionInfoMap);
    }

    public JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>> findAgents(
            JavaPairRDD<Integer, int[]> neighborList, int partitionNum, final int[] highDegArray) {


        final int lowAgentDeg = highDegArray.length == 0 ? 0 : highDegArray[0];
        final int highAgentDeg = highDegArray.length == 0 ? 1 : highDegArray[highDegArray.length - 1] + 1;

        return neighborList
                .filter(kv -> kv._2.length >= lowAgentDeg && kv._2.length < highAgentDeg)
                .mapPartitionsToPair(partition -> {
                    Random random = new Random();
                    List<Tuple2<Integer, Tuple3<Integer, Integer, int[]>>> out = new ArrayList<>();
                    while (partition.hasNext()) {
                        int pNum = random.nextInt(partitionNum);
                        final Tuple2<Integer, int[]> nl = partition.next();
                        out.add(new Tuple2<>(nl._1, new Tuple3<>(INIT, pNum, nl._2)));
                    }
                    return out.iterator();
                }).cache();
    }

    public JavaPairRDD<Integer, Integer> getPartitions(int partitionNum) {
        JavaPairRDD<Integer, int[]> neighborList = this.neighborList
                .getOrCreate();

        int minHighDegCount = partitionNum * 5; // higher value more balanced partition sizes, but more cost

        // higher value more balanced partition sizes. lower value causes the program to be run faster
        // lowering this along with lowering partitionSizeAlert may result a better partitioning with fast run
        int pSizeController = 10;

        // lower value more balanced partition sizes
        int partitionSizeAlert = (int) (neighborList.count() / partitionNum) / 2;

        final Broadcast<Integer> pSizeBroadcast = conf.getSc().broadcast(partitionSizeAlert);
        int pLaw = 100;

        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
        JavaPairRDD<Integer, Integer> partitions = conf.getSc()
                .parallelizePairs(list)
                .partitionBy(new HashPartitioner(neighborList.getNumPartitions()));;

        List<Tuple2<Integer, Tuple3<Integer, Integer, int[]>>> alist = new ArrayList<>();
        JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>> agents = conf.getSc()
                .parallelizePairs(alist)
                .partitionBy(new HashPartitioner(neighborList.getNumPartitions()));

        int iteration = 0;
        while (true) {
            iteration ++;
            long vCount = neighborList.count();
            long aCount = agents.count();
            System.out.println("Iteration: " + iteration + ", agentCount: " + aCount + ", vertexCount: " + vCount);
            if (vCount == 0 && aCount == 0) {
                break;
            }

            int[] sortedHighDegs = createSortedHighDegs(neighborList, minHighDegCount, pSizeController, pLaw);

            if (vCount != 0 && aCount == 0)
                agents = findAgents(neighborList, partitionNum, sortedHighDegs);

            final Broadcast<Map<Integer, Integer>> partitioningInfo =
                    createPartitionInfoBroadcast(partitions, agents, conf.getSc());

            neighborList = neighborList.subtractByKey(agents).cache();
            Tuple2<JavaPairRDD<Integer, Integer>, JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>>> result =
                    partitioning(partitioningInfo, pSizeBroadcast, agents, neighborList, sortedHighDegs);

            partitions = partitions.union(result._1.cache());
            agents = result._2.cache();
//            minHighDegCount = Math.max(10, minHighDegCount - 10);
//            pSizeController += 5;
        }

        return partitions.cache();
    }

    public Tuple2<JavaPairRDD<Integer, Integer>, JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>>> partitioning(
            Broadcast<Map<Integer, Integer>> partitioningInfo, Broadcast<Integer>  pSizeBroadcast,
            JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>> agents, JavaPairRDD<Integer, int[]> neighborList,
            int[] highDegArray) {

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> pSuggestions = agents.flatMapToPair(kv -> {
            List<Tuple2<Integer, Tuple2<Integer, Integer>>> out = new ArrayList<>();
            int power = kv._2._1();
            final int partition = kv._2._2();
            final int[] neighbors = kv._2._3();

            final Integer pCount = partitioningInfo.getValue().get(partition);

            if (power == INIT) {
                final int deg = neighbors.length;
                int index = Arrays.binarySearch(highDegArray, deg);
                index = index >= 0 ? index : -(index + 1);
                power = index;
            }

            Integer partitionSize = pSizeBroadcast.getValue();
            if (pCount != null && pCount > partitionSize) {
                power = (int) (power * (partitionSize / (float) pCount));
            }

            power ++;

            out.add(new Tuple2<>(kv._1, new Tuple2<>(partition, power)));
            for (int neighbor : neighbors) {
                out.add(new Tuple2<>(neighbor, new Tuple2<>(partition, power)));
            }
            return out.iterator();
        });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> vpPartitions = pSuggestions
                .groupByKey()
                .mapValues(suggestions -> {
                    TreeMultiset<Integer> powerSet = TreeMultiset.create();
                    int maxPower = 0;
                    int partition = UN_ASSIGNDED;
                    for (Tuple2<Integer, Integer> suggestion : suggestions) {
                        final int occurrence = suggestion._2;
                        powerSet.add(suggestion._1, occurrence);
                    }

                    for (Multiset.Entry<Integer> entry : powerSet.entrySet()) {
                        if (entry.getCount() > maxPower) {
                            partition = entry.getElement();
                            maxPower = entry.getCount();
                        }
                    }
                    return new Tuple2<>(maxPower, partition);
                });

        JavaPairRDD<Integer, Integer> agentPartitions =
                agents.join(vpPartitions).mapValues(v -> v._2._2);

        JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>> nextAgents = neighborList.leftOuterJoin(vpPartitions)
                .mapValues(v -> {
                    if (v._2.isPresent())
                        return new Tuple3<>(v._2.get()._1, v._2.get()._2, v._1);
                    return null;
                }).filter(kv -> kv._2 != null);

        return new Tuple2<>(agentPartitions, nextAgents);
    }

    public static void main(String[] args) {

        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args));
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        PowerPartitioning partitioning = new PowerPartitioning(neighborList);
        final JavaPairRDD<Integer, Integer> partitions = partitioning.getPartitions(100).cache();

        List<Tuple2<Integer, Integer>> partitionList = partitions
                .mapToPair(kv -> new Tuple2<>(kv._2, 1))
                .reduceByKey((a, b) -> a + b).collect();

        List<Tuple2<Integer, Integer>> pList = new ArrayList<>(partitionList);
        Collections.sort(pList, (a, b) -> a._1 - b._1);

        for (Tuple2<Integer, Integer> pc : pList) {
            System.out.println(pc);
        }
        PartitioningCost partitioningCost = new PartitioningCost(neighborList, partitions);
        final long totalCost = partitioningCost.getCost();
        final long cost = totalCost - partitioning.getExcludedSumDeg();
        System.out.println("TotalCost: " + totalCost + ", excludes: " + partitioning.getExcludedSumDeg() +
                ", remainingCost of partitioning: " + cost);
    }
}
