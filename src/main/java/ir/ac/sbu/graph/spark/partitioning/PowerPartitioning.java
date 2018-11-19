package ir.ac.sbu.graph.spark.partitioning;

import ir.ac.sbu.graph.spark.*;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.stream.Collectors;

public class PowerPartitioning extends SparkApp {
    private static final int INIT_POWER = 0;
    private static final int UN_ASSIGNDED = -1;
    private int powerLowVertexCount = 0;
    private int ignoreHighestCount;
    protected NeighborList neighborList;
    protected final Map<Integer, Long> pSizes;
    protected long vCount;

    public PowerPartitioning(NeighborList neighborList, int ignoreHighestCount) {
        super(neighborList);
        this.neighborList = neighborList;
        this.ignoreHighestCount = ignoreHighestCount;
        pSizes = new HashMap<>();
    }

    public int getPowerLowVertexCount() {
        return powerLowVertexCount;
    }

    /**
     *
     * @param neighborList
     * @param partitionSize
     * @return sorted PInfo and leaf vertex count
     */
    private Tuple2<PInfo, Long> createPInfo(JavaPairRDD<Integer, int[]> neighborList, int partitionSize,
                                                     int ignoreHighestCount) {

        Map<Integer, Long> degCount = neighborList.map(kv -> kv._2.length).countByValue();
        long leafCount = degCount.getOrDefault(1, 0L);

        List<Map.Entry<Integer, Long>> collect = degCount.entrySet()
                .parallelStream()
                .sorted(Comparator.comparingInt(Map.Entry::getKey))
                .collect(Collectors.toList());

        PInfo pInfo = new PInfo();
        int[] sorted = new int[collect.size()];
        for (int i = 0; i < sorted.length; i++) {
            Map.Entry<Integer, Long> entry = collect.get(i);
            sorted[i] = entry.getKey();
        }

        int sum = 0;
        int hIndex = Math.max(0, sorted.length - ignoreHighestCount);
        pInfo.high = sorted[hIndex];

        int lIndex = hIndex;
        while (true) {
            if (sum > partitionSize || lIndex == 0)
                break;

            sum += sorted[lIndex --];
        }
        pInfo.low = sorted[lIndex];
        pInfo.sortedDegCounts = sorted;
        return new Tuple2<>(pInfo, leafCount);
    }


    private JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>> findAgents(
            JavaPairRDD<Integer, int[]> neighborList, Broadcast<PInfo> broadcast ) {
        System.out.println("Finding new agents");

        return neighborList.mapPartitionsToPair(partition -> {
            PInfo pInfo = broadcast.getValue();
            int[] sorted = pInfo.sortedDegCounts;
            int lowDeg = pInfo.low;
            int highDeg = pInfo.high;

            List<Tuple2<Integer, Tuple3<Integer, Integer, int[]>>> out = new ArrayList<>();
            Random random = new Random();
            while (partition.hasNext()) {
                Tuple2<Integer, int[]> kv = partition.next();
                if (kv._2.length < lowDeg || kv._2.length > highDeg)
                    continue;

                int pNum = random.nextInt(pInfo.partitionNum);
                out.add(new Tuple2<>(kv._1, new Tuple3<>(INIT_POWER, pNum, kv._2)));
            }
            return out.iterator();
        }).cache();
    }

    public JavaPairRDD<Integer, Integer> getPartitions(int partitionNum) {
        JavaPairRDD<Integer, int[]> neighborList = this.neighborList
                .getOrCreate();

        // Key-values to have vertex Id in the key and its assigned partition in the value
        int initNeighborPartitions = neighborList.getNumPartitions();


        // Nodes are able to send their partition numbers to their neighbors
        JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>> agents = conf.getSc()
                .parallelizePairs(new ArrayList<Tuple2<Integer, Tuple3<Integer, Integer, int[]>>>())
                .repartition(initNeighborPartitions); // Set num of partitions

        vCount = neighborList.count();
        // lower value more balanced partition sizes
        int avgPSize = (int) (vCount / partitionNum);
        final Broadcast<Integer> pSizeBroadcast = conf.getSc().broadcast(avgPSize);

        Tuple2<PInfo, Long> pInfoResult = createPInfo(neighborList, avgPSize, ignoreHighestCount);
        long leafVertexCount = pInfoResult._2;
        System.out.println("leaf vertex num: " + leafVertexCount);
        final PInfo pInfo = pInfoResult._1;
        final long maxExcludeCount = pInfo.sortedDegCounts.length - pInfo.high;
        pInfo.partitionNum = partitionNum;

        ArrayList<Tuple2<Integer, Integer>> highestVPartitions = new ArrayList<>();

        JavaPairRDD<Integer, Integer> partitions = conf.getSc()
                .parallelizePairs(highestVPartitions)
                .repartition(initNeighborPartitions);

        for (int i = 0; i < pInfo.partitionNum; i++) {
            pSizes.put(i, maxExcludeCount);
        }
        Broadcast<PInfo> pInfoBroadcast = conf.getSc().broadcast(pInfo);

        int iteration = 0;
        while (true) {
            iteration++;
            long vCount = neighborList.count();
            long aCount = agents.count();
            System.out.println("Iteration: " + iteration + ", agentCount: " + aCount + ", vertexCount: " + vCount);
            if (vCount == 0 && aCount == 0) {
                break;
            }

            if (vCount != 0 && aCount == 0) {
                agents = findAgents(neighborList, pInfoBroadcast);
            }

            if (vCount < leafVertexCount) {
                System.out.println("vCount: " + vCount + " < leaf: " + leafVertexCount);
                long sum = pSizes
                        .values()
                        .stream()
                        .reduce((a, b) -> a + b)
                        .get();
                System.out.println("sum:" + sum);

                TreeMap<Integer, Long> sortedMap = new TreeMap<>(pSizes);
                final int[] distArray = new int[sortedMap.size()];
                final int[] pNumArray = new int[distArray.length];
                int i = 0;
                System.out.println("creating partition pInfoBroadcast pNumArray");
                for (Map.Entry<Integer, Long> entry : sortedMap.entrySet()) {
                    if (i == 0) {
                        distArray[i] = 0;
                    } else {
                        distArray[i] = distArray[i - 1];
                    }
                    distArray[i] += sum - entry.getValue();
                    pNumArray[i] = entry.getKey();
                    i++;
                    if (i == distArray.length)
                        break;
                }

                final Broadcast<int[]> dist = conf.getSc().broadcast(distArray);
                final Broadcast<int[]> partitionNumbers = conf.getSc().broadcast(pNumArray);

                JavaPairRDD<Integer, Integer> finalPartitions = neighborList
                        .mapPartitionsToPair(p -> {

                            final int[] distribution = dist.getValue();
                            final int[] pNumbers = partitionNumbers.getValue();
                            final int max = distribution[distribution.length - 1];

                            List<Tuple2<Integer, Integer>> out = new ArrayList<>();
                            final Random random = new Random();

                            int high = pInfoBroadcast.getValue().high;
                            while (p.hasNext()) {
                                Tuple2<Integer, int[]> kv = p.next();
                                final Integer vertex = kv._1;
                                if (kv._2.length > high) {
//                                    for (int pNumber : pNumbers) {
//                                        out.add(new Tuple2<>(vertex, pNumber));
//                                    }
                                } else {
                                    int idxSearch = random.nextInt() % max;
                                    int index = Arrays.binarySearch(distribution, idxSearch);
                                    index = index >= 0 ? index : -(index + 1);
                                    index = Math.min(distribution.length - 1, index);
                                    out.add(new Tuple2<>(vertex, pNumbers[index]));
                                }
                            }
                            System.out.println("out support: " + out.size());
                            return out.iterator();
                        });

                return partitions.union(finalPartitions).coalesce(initNeighborPartitions).cache();
            }


            Broadcast<Map<Integer, Long>> partitionSizes = conf.getSc().broadcast(pSizes);
            neighborList = neighborList.subtractByKey(agents).persist(StorageLevel.MEMORY_AND_DISK());

            Tuple2<JavaPairRDD<Integer, Integer>, JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>>> result =
                    nextPartitions(partitionSizes, pSizeBroadcast, agents, neighborList, pInfoBroadcast);

            JavaPairRDD<Integer, Integer> newAssignedPartitions = result._1.persist(StorageLevel.MEMORY_AND_DISK());
            final Map<Integer, Long> map = newAssignedPartitions.values().countByValue();

            System.out.println("update partition sizes, entries: " + map.size());
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                pSizes.compute(entry.getKey(), (k, v) -> v == null ? entry.getValue() : v + entry.getValue());
            }

            partitions = partitions.union(newAssignedPartitions);
            agents = result._2.cache();
        }

        return partitions.cache();
    }

    /**
     * Determine the partition nums of some vertices determined as agents and specifies the vertices to
     * be as the next agents which are allowed to send their partition num to their neighbors
     *
     * @param pSizeBroadcast Desired support of
     * @param agents         Vertices having assigned a partition num and are allowed to send their
     *                       partition num to their neighbors
     */
    private Tuple2<JavaPairRDD<Integer, Integer>, JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>>> nextPartitions(
            final Broadcast<Map<Integer, Long>> partitionSizes, final Broadcast<Integer> pSizeBroadcast,
            JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>> agents,
            JavaPairRDD<Integer, int[]> neighborList, Broadcast<PInfo> pInfoBroadcast) {

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> pSuggestions = agents.mapPartitionsToPair(partition -> {
            List<Tuple2<Integer, Tuple2<Integer, Integer>>> out = new ArrayList<>();
            final PInfo pInfo = pInfoBroadcast.getValue();
            final int[] sorted = pInfo.sortedDegCounts;
            final int highDeg = pInfo.high;

            final Integer partitionSize = pSizeBroadcast.getValue();
            final Map<Integer, Long> partitionSizeInfo = partitionSizes.value();

            while (partition.hasNext()) {
               Tuple2<Integer, Tuple3<Integer, Integer, int[]>> kv = partition.next();

               int currentPartition = kv._2._2();
                final long pSize = partitionSizeInfo.get(currentPartition);

                if (pSize > partitionSize) {
                    continue;
                }

                int power = kv._2._1();
                int[] neighbors = kv._2._3();
                int deg = neighbors.length;
                if (deg > highDeg)
                    continue;

                int index = Arrays.binarySearch(sorted ,deg);
                index = index >= 0 ? index : -(index + 1);

                if (power == INIT_POWER || index / 2 > power)
                    power = index;
                else
                    power = power + index / 2;
                power ++;

                out.add(new Tuple2<>(kv._1, new Tuple2<>(currentPartition, power)));
                for (int neighbor : neighbors) {
                    out.add(new Tuple2<>(neighbor, new Tuple2<>(currentPartition, power)));
                }
            }
            return out.iterator();
        });
//        JavaPairRDD<Integer, Tuple2<Integer, Integer>> pSuggestions = agents.flatMapToPair(kv -> {
//            List<Tuple2<Integer, Tuple2<Integer, Integer>>> out = new ArrayList<>();
//
//            final int partition = kv._2._2();
//            PInfo pInfo = pInfoBroadcast.getValue();
//            final int[] sorted = pInfo.sortedDegCounts;
//
//            // If number of nodes in the partition is larger than the specified partition support then decrease
//            // it by the ratio of (current partition support) / (number of nodes in the current partition)
//            Integer partitionSize = pSizeBroadcast.getValue();
//
//            // Number of assigned nodes to the current partition
//            final Long pCount = partitionSizes.value().get(partition);
//            if (pCount != null && pCount > partitionSize) {
//                return out.iterator();
//            }
//
//            int power = kv._2._1();
//            final int[] neighbors = kv._2._3();
//            final int deg = neighbors.length;
//
//
//            if (power == INIT_POWER)
//                power = index;
//            else if (index > power)
//                power = (power + index) / 2;
//            else
//                power = power + index / 2;
//
//            power++;
//
//            // This node sends its nextPartitions power to its neighbors as a partition number suggestion
//            out.add(new Tuple2<>(kv._1, new Tuple2<>(partition, power)));
//            for (int neighbor : neighbors) {
//                out.add(new Tuple2<>(neighbor, new Tuple2<>(partition, power)));
//            }
//            return out.iterator();
//        });

        // Per vertex, received partition num suggestions are aggregated and a the suggestion
        // with the highest power is selected as the specified partition num
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> vpPartitions = pSuggestions
                .groupByKey()
                .mapValues(suggestions -> {
                    Int2IntMap map = new Int2IntOpenHashMap();
                    int maxPower = 0;
                    int partition = UN_ASSIGNDED;
                    for (Tuple2<Integer, Integer> suggestion : suggestions) {
                        final int currentPartition = suggestion._1;
                        final int power = suggestion._2;
                        map.compute(currentPartition, (k, v) -> v == null ? power : v + power);
                    }

                    for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
                        if (entry.getValue() > maxPower) {
                            partition = entry.getKey();
                            maxPower = entry.getValue();
                        }
                    }

                    return new Tuple2<>(partition, maxPower);
                }).cache();

        // The partition nums of agents are updated by the new assigned partition nums
        JavaPairRDD<Integer, Integer> vPartitions = vpPartitions.mapValues(v -> v._1);
        JavaPairRDD<Integer, Integer> agentPartitions = agents
                .join(vPartitions)
                .mapValues(v -> v._2);

        // Next agents are determined to send their partitions to the others
        JavaPairRDD<Integer, Tuple3<Integer, Integer, int[]>> nextAgents = neighborList.leftOuterJoin(vpPartitions)
                .mapValues(v -> {
                    if (v._2.isPresent()) {
                        return new Tuple3<>(v._2.get()._2, v._2.get()._1, v._1);
                    }
                    return null;
                }).filter(kv -> kv._2 != null);

        return new Tuple2<>(agentPartitions, nextAgents);
    }

    public static void main(String[] args) {

        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args));
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        PowerPartitioning partitioning = new PowerPartitioning(neighborList, 10);
        final JavaPairRDD<Integer, Integer> partitions = partitioning.getPartitions(2);
        System.out.println("partition support: " + partitions.count());

        List<Tuple2<Integer, Integer>> partitionList = partitions
                .mapToPair(kv -> new Tuple2<>(kv._2, 1))
                .reduceByKey((a, b) -> a + b).collect();

        System.out.println("partition list: " + partitionList.size());
        List<Tuple2<Integer, Integer>> pList = new ArrayList<>(partitionList);
        pList.sort((a, b) -> a._1 - b._1);

        final long count = neighborList.getOrCreate().count();
        float avg = count / (float) pList.size();
        float variance = 0;
        for (Tuple2<Integer, Integer> pc : pList) {
            variance += Math.abs(pc._2 - avg) / pList.size();
            System.out.println(pc);
        }

        System.out.println("calculating num cuts");
        PartitioningCost partitioningCost = new PartitioningCost(neighborList, partitions);
        final long numCut = partitioningCost.getNumCut();
        System.out.println("Num Cuts: " + numCut + ", Variance: " + variance
                + ", PowerLow Vertex Count: " + partitioning.getPowerLowVertexCount());
    }

}
