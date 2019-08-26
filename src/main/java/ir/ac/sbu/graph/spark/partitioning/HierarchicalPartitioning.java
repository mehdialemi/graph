package ir.ac.sbu.graph.spark.partitioning;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkAppConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.spark_project.guava.collect.Multiset;
import org.spark_project.guava.collect.SortedMultiset;
import org.spark_project.guava.collect.TreeMultiset;
import scala.Tuple2;

import java.util.*;

public class HierarchicalPartitioning extends PowerPartitioning {

    private final int k;

    public HierarchicalPartitioning(NeighborList neighborList, int ignoreHighestCount, int k) {
        super(neighborList, ignoreHighestCount);
        this.k = k;
    }

    public JavaPairRDD<Integer, Integer> find(int partitions) {

        final JavaPairRDD<Integer, Integer> vpRDD = getPartitions(partitions * k);

        final JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> neighborPartitions = neighborList.getOrCreate()
                .join(vpRDD.repartition(partitions))
                .flatMapToPair(v -> {

                    List<Tuple2<Integer, Tuple2<Integer, Integer>>> out = new ArrayList<>();
                    for (int vertex : v._2._1) {
                        out.add(new Tuple2<>(vertex, new Tuple2<>(v._1, v._2._2)));
                    }

                    return out.iterator();

                }).groupByKey();

        final List<Tuple2<Tuple2<Integer, Integer>, Integer>> pRelations = neighborPartitions
                .flatMapToPair(kv -> {

                    List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>();
                    SortedMultiset<Integer> set = TreeMultiset.create((a, b) -> b - a);
                    for (Tuple2<Integer, Integer> vp : kv._2) {
                        set.add(vp._2);
                    }

                    int[] pNum = new int[set.size()];
                    int[] pCount = new int[set.size()];
                    int i = 0;
                    for (Multiset.Entry<Integer> entry : set.entrySet()) {
                        pNum[i] = entry.getElement();
                        pCount[i] = entry.getCount();
                        i++;
                    }

                    for (i = 0; i < pNum.length; i++) {
                        for (int j = 0; j < pNum.length; j++) {
                            if (i == j)
                                continue;
                            out.add(new Tuple2<>(new Tuple2<>(pNum[i], pNum[j]), pCount[j]));
                        }
                    }

                    return out.iterator();
                })
                .reduceByKey((a, b) -> a + b)
                .collect();

        int avgPSize = (int) (vCount / partitions);
        final int maxPartitionSize = (int) (avgPSize * 1.5);
        final Map<Integer, SortedMultiset<Integer>> pConnections = new HashMap<>();
        for (Tuple2<Tuple2<Integer, Integer>, Integer> pRelation : pRelations) {
            final int pDestCount = pRelation._2;
            if (pDestCount == 0)
                continue;

            final int pSrc = pRelation._1._1;
            final int pDst = pRelation._1._2;

            if (pSizes.get(pDst).intValue() > maxPartitionSize)
                continue;

            SortedMultiset<Integer> r = pConnections.get(pSrc);
            if (r == null) {
                r = TreeMultiset.create();
                pConnections.put(pSrc, r);
            }
            r.add(pDst);
        }

        TreeMap<Tuple2<Integer, Integer>, Integer> sortedMap = new TreeMap<>((a, b) -> {
            int diff = a._2 - b._2;
            return diff == 0 ? a._1 - b._1 : diff;
        });

        for (Map.Entry<Integer, Long> entry : pSizes.entrySet()) {
            sortedMap.put(new Tuple2<>(entry.getKey(), entry.getValue().intValue()), 1);
        }

        final Map<Integer, Integer> newPartitionNums = new HashMap<>();
        int optimizeCount = 0;

        boolean stop = false;
        while (!stop) {
            optimizeCount ++;
            final Tuple2<Integer, Integer> first = sortedMap.firstKey();
            Integer pNum = first._1;
            int pCount = first._2;

            System.out.println("Optimize count: " + optimizeCount + ", pNum: " + pNum
                    + ", pCount: " + pCount + ", avgPSize: " + avgPSize);

            if (pCount > avgPSize || pSizes.size() <= partitions) {
                break;
            }

            final SortedMultiset<Integer> pList = pConnections.get(pNum);

            Integer pNum2 = 0;
            int pCount2 = -1;
            for (Integer p : pList) {
                Long count = pSizes.get(p);

                if (count == null || count == 0)
                    continue;

                if (count < avgPSize) {
                    pNum2 = p;
                    pCount2 = count.intValue();
                    break;
                }
            }

            if (pCount2 == -1) {
                final Tuple2<Integer, Integer> second = sortedMap.higherKey(first);
                pNum2 = second._1;
                pCount2 = second._2;
            }

            Integer pSrc, pDest;
            int newPCount = pCount + pCount2;
            if (pNum < pNum2) {
                pSrc = pNum2;
                pDest = pNum;
            } else {
                pSrc = pNum;
                pDest = pNum2;
            }

            sortedMap.remove(new Tuple2<>(pNum, pCount));
            sortedMap.remove(new Tuple2<>(pNum2, pCount2));

            sortedMap.put(new Tuple2<>(pDest, newPCount), 1);
            newPartitionNums.put(pSrc, pDest);
            pSizes.put(pDest, Long.valueOf(newPCount));
            pSizes.remove(pSrc);
        }

        TreeMap<Integer, Integer> rPNames = new TreeMap<>();
        for (Map.Entry<Integer, Integer> entry : newPartitionNums.entrySet()) {
            rPNames.put(entry.getValue(), entry.getKey());
        }



        final Broadcast<Map<Integer, Integer>> pNumMapping = conf.getJavaSparkContext().broadcast(newPartitionNums);

        JavaPairRDD<Integer, Integer> vPartition = vpRDD.mapPartitionsToPair(p -> {
            final Map<Integer, Integer> map = pNumMapping.getValue();
            List<Tuple2<Integer, Integer>> out = new ArrayList<>();
            while (p.hasNext()) {
                Tuple2<Integer, Integer> kv = p.next();
                Integer newPNum = map.get(kv._2);
                if (newPNum == null)
                    newPNum = kv._2;
                out.add(new Tuple2<>(kv._1, newPNum));
            }
            return out.iterator();
        }).cache();

        return vPartition;
    }

    public static void main(String[] args) {
        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args));
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        HierarchicalPartitioning partitioning = new HierarchicalPartitioning(neighborList, 20, 10);
        final JavaPairRDD<Integer, Integer> partitions = partitioning.find(50);

        List<Tuple2<Integer, Integer>> partitionList = partitions
                .mapToPair(kv -> new Tuple2<>(kv._2, 1))
                .reduceByKey((a, b) -> a + b).collect();

        List<Tuple2<Integer, Integer>> pList = new ArrayList<>(partitionList);
        pList.sort((a, b) -> a._1 - b._1);

        final long count = neighborList.getOrCreate().count();
        float avg = count / (float) pList.size();
        float variance = 0;
        int aCount = 0;
        for (Tuple2<Integer, Integer> pc : pList) {
            variance += Math.abs(pc._2 - avg) / pList.size();
            aCount += pc._2;
            System.out.println(pc);
        }

        PartitioningCost partitioningCost = new PartitioningCost(neighborList, partitions);
        final long numCut = partitioningCost.getNumCut();
        System.out.println("Num Cuts: " + numCut + ", Variance: " + variance
                + ", PowerLow Vertex Count: " + partitioning.getPowerLowVertexCount());    }
}
