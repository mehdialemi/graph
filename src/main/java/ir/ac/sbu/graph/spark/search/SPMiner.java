package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.*;
import ir.ac.sbu.graph.fonl.matcher.LabelMeta;
import ir.ac.sbu.graph.fonl.matcher.LocalFonlCreator;
import ir.ac.sbu.graph.fonl.matcher.Sonl;
import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class SPMiner extends SparkApp {

    private SGMatcherConf conf;
    private EdgeLoader edgeLoader;
    private JavaPairRDD <Integer, String> labels;

    public SPMiner(SGMatcherConf conf, EdgeLoader edgeLoader, JavaPairRDD <Integer, String> labels) {
        super(edgeLoader);
        this.conf = conf;
        this.edgeLoader = edgeLoader;
        this.labels = labels;
    }

    public void search(Sonl query) {
        // load label
        NeighborList neighborList = new NeighborList(edgeLoader);
        JavaPairRDD <Integer, Fvalue <LabelMeta>> lFonl = SparkFonlCreator.createLabelFonl(neighborList, labels);
        printFonl(lFonl);

        JavaPairRDD <Integer, Integer> edges = lFonl.flatMapToPair(kv -> {
            List <Tuple2 <Integer, Integer>> out = new ArrayList <>();
            for (int n : kv._2.fonl) {
                out.add(new Tuple2 <>(kv._1, n));
            }
            return out.iterator();
        }).cache();

        query.finalize();
        int diameter = query.diameter();

        final Broadcast <Sonl> queryBroadCast = conf.getSc().broadcast(query);
        JavaPairRDD <Integer, Iterable <Candidate[]>> candidateList = initCandidates(lFonl, queryBroadCast);

        JavaRDD <Candidate> fullMatches = conf.getSc().parallelize(Collections.emptyList());

        for (int i = 0; i < diameter; i++) {

            long count = candidateList.count();

            System.out.println("Candidates: ");
            printCandidates(candidateList);
            System.out.println("candidate count: " + count);

            if (count == 0)
                break;

            JavaRDD <Candidate> complete = getComplete(candidateList);
            fullMatches = fullMatches.union(complete).cache();

            JavaPairRDD <Integer, Tuple2 <Integer, Candidate>> complement = getComplement(lFonl, queryBroadCast, candidateList);

            System.out.println("Complements");
            for (Tuple2 <Integer, Tuple2 <Integer, Candidate>> c : complement.collect()) {
                System.out.println("Complement: " + c);
            }

            for (Tuple2 <Integer, Integer> e : edges.collect()) {
                System.out.println("e " + e);
            }

            JavaPairRDD <String, Iterable <Candidate>> cResponse = complement.join(edges)
                    .filter(kv -> kv._2._1._1 == -1 || (kv._2._1._1.equals(kv._2._2)))
                    .mapValues(kv -> kv._1._2)
                    .map(kv -> kv._2)
                    .groupBy(c -> Arrays.toString(c.vArray));

            for (Tuple2 <String, Iterable <Candidate>> c : cResponse.collect()) {
                System.out.println("cResponse: " + c);
            }

            JavaPairRDD <Integer, Iterable <Candidate>> exCandidates = cResponse.mapValues(v -> {
                int cc = 0;
                Candidate c = null;
                for (Candidate candidate : v) {
                    if (c == null)
                        c = candidate;

                    cc++;
                }

                if (cc == c.checkCount)
                    return c;
                return null;
            }).filter(kv -> kv._2 != null)
                    .mapToPair(kv -> new Tuple2 <>(kv._2.getSrc(), kv._2))
                    .groupByKey(lFonl.getNumPartitions()).cache();

//            System.out.println("Complements");
//            printCandidates(complement);

//            JavaPairRDD <Integer, Iterable <Candidate[]>> exCandidates = getExCandidates(lFonl, queryBroadCast, complement);

//            System.out.println("Extended Candidates");
//            printCandidates(exCandidates);

            for (Tuple2 <Integer, Iterable <Candidate>> c : exCandidates.collect()) {
                System.out.println("exCandidates: " + c);
            }

            JavaPairRDD <Integer, Candidate[]> distincts  = candidateList.fullOuterJoin(exCandidates)
                    .mapValues(values -> {
                        Iterable <Candidate> i2 = values._2.orElse(CollatingIterator::new);
                        Iterable <Candidate[]> i1 = values._1.orElse(CollatingIterator::new);
                        Set <Candidate> set = new HashSet <>();
                        for (Candidate[] cArray : i1) {
                            for (Candidate c : cArray) {
                                set.add(c);
                            }
                        }
                        for (Candidate c : i2) {
                            set.add(c);
                        }

                        return set.toArray(new Candidate[0]);
            });

//            JavaPairRDD <Integer, Candidate[]> distincts = candidateList.union(exCandidates)
//                    .groupByKey(lFonl.getNumPartitions())
//                    .mapValues(values -> {
//                        Set <Candidate> set = new HashSet <>();
//                        for (Iterable <Candidate[]> iterable : values) {
//                            for (Candidate[] candidates : iterable) {
//                                for (Candidate candidate : candidates) {
//                                    candidate.resetMeta();
//                                    set.add(candidate);
//                                }
//                            }
//                        }
//                        return set.toArray(new Candidate[0]);
//                    });

            System.out.println("Distinct Candidates");
            print(distincts);

            candidateList = distincts.join(lFonl).flatMapToPair(kv -> {
                int vertex = kv._1;

                Sonl SONL = queryBroadCast.getValue();
                Fvalue <LabelMeta> fvalue = kv._2._2;
                CandidGen candidGen = new CandidGen(vertex, fvalue, SONL);

                Map <Integer, Set <Candidate>> v2Candids = new HashMap <>();
                // Extend candidateList through their neighbors
                for (Candidate candidate : kv._2._1) {

                    CandidGen.addToMap(vertex, candidate, v2Candids);

                    if (!candidate.isFull()) {
                        Map <Integer, Set <Candidate>> map = candidGen.newCandidates(candidate);
                        if (map == null)
                            continue;

                        for (Map.Entry <Integer, Set <Candidate>> entry : map.entrySet()) {
                            v2Candids.compute(entry.getKey(), (k, v) -> {
                                if (v == null)
                                    v = new HashSet <>();
                                v.addAll(entry.getValue());
                                return v;
                            });
                        }
                    }
                }

                return v2Candids
                        .entrySet()
                        .stream()
                        .map(e -> new Tuple2 <>(e.getKey(), e.getValue().toArray(new Candidate[0])))
                        .iterator();

            }).groupByKey(lFonl.getNumPartitions()).persist(StorageLevel.MEMORY_AND_DISK());
        }

        printCandidates(candidateList);
        printCandidates(fullMatches.distinct());
    }

    private JavaPairRDD <Integer, Tuple2 <Integer, Candidate>> getComplement(JavaPairRDD <Integer, Fvalue <LabelMeta>> lFonl,
                                                                        Broadcast <Sonl> queryBroadCast,
                                                                        JavaPairRDD <Integer, Iterable <Candidate[]>> candidateList) {
        return candidateList.flatMapToPair(kv -> {
            Iterable <Candidate[]> candidates = kv._2;

            // remove repetitive candidateList and add them to a list
            List <Candidate[]> list = new ArrayList <>();
            for (Candidate[] cArray : candidates) {
                list.add(cArray);
            }

            List <Tuple2 <Integer, Tuple2 <Integer, Candidate>>> out = new ArrayList <>();
            Set <Candidate> set = new HashSet <>();
            Sonl sonl = queryBroadCast.getValue();
            for (int j = 0; j < list.size(); j++) {
                for (int k = 0; k < list.size(); k++) {
                    if (j == k)
                        continue;
                    for (Candidate current : list.get(j)) {
                        if (current.isSubSet() || current.isDuplicate() || current.isFull())
                            continue;
                        for (Candidate other : list.get(k)) {

                            if (other.isSubSet() || other.isDuplicate() || other.isFull())
                                continue;

                            Map <Candidate, List <Tuple2 <Integer, Integer>>> cMap = current.complement(other, sonl);
                            if (cMap == null)
                                continue;

                            for (Map.Entry <Candidate, List <Tuple2 <Integer, Integer>>> entry : cMap.entrySet()) {
                                List <Tuple2 <Integer, Integer>> value = entry.getValue();
                                entry.getKey().checkCount = value.size();
                                Candidate c = entry.getKey();
                                if (value.size() == 0) {
                                    out.add(new Tuple2 <>(c.getSrc(), new Tuple2 <>(-1, c)));
                                } else {
                                    for (Tuple2 <Integer, Integer> tuple2 : value) {
                                        int i1 = tuple2._1;
                                        int i2 = tuple2._2;

                                        out.add(new Tuple2 <>(i1, new Tuple2 <>(i2, entry.getKey())));
                                    }
                                }
                            }

                        }
                    }
                }
            }

            return out.iterator();
        });
    }

//    private JavaPairRDD <Integer, Iterable <Candidate[]>> getExCandidates(JavaPairRDD <Integer, Fvalue <LabelMeta>> lFonl, Broadcast <Sonl> queryBroadCast, JavaPairRDD <Integer, Iterable <Candidate[]>> complement) {
//        return complement.join(lFonl).flatMapToPair(kv -> {
//            Sonl sonl = queryBroadCast.getValue();
//            Fvalue<LabelMeta> fvalue = kv._2._2;
//            Map<Integer, Set<Candidate>> map = new HashMap<>();
//            for (Candidate[] candidates : kv._2._1) {
//                for (Candidate candidate : candidates) {
//                    if (candidate.neighbor(fvalue))
//                        CandidGen.addToMap(candidate.getSrc(), candidate, map);
//                }
//            }
//            return map
//                    .entrySet()
//                    .stream()
//                    .map(e -> new Tuple2<>(e.getKey(), e.getValue().toArray(new Candidate[0])))
//                    .iterator();
//        }).groupByKey(lFonl.getNumPartitions());
//    }

    private JavaRDD <Candidate> getComplete(JavaPairRDD <Integer, Iterable <Candidate[]>> candidates) {
        return candidates.flatMap(kv -> {
            List <Candidate> list = new ArrayList <>();
            for (Candidate[] cArray : kv._2) {
                for (Candidate candidate : cArray) {
                    if (candidate.isFull())
                        list.add(candidate);
                }
            }
            return list.iterator();
        }).cache();
    }

    private JavaPairRDD <Integer, Iterable <Candidate[]>> initCandidates(JavaPairRDD <Integer, Fvalue <LabelMeta>> lFonl,
                                                                         Broadcast <Sonl> queryBroadCast) {
        return lFonl.flatMapToPair(kv -> {
            int vertex = kv._1;
            Fvalue <LabelMeta> fvalue = kv._2;
            Sonl SONL = queryBroadCast.getValue();

            CandidGen candidGen = new CandidGen(vertex, fvalue, SONL);

            Map <Integer, Set <Candidate>> vToCandids = candidGen.newCandidates();
            if (vToCandids == null)
                return Collections.emptyIterator();

            return vToCandids
                    .entrySet()
                    .stream()
                    .map(e -> new Tuple2 <>(e.getKey(), e.getValue().toArray(new Candidate[0])))
                    .iterator();

        }).groupByKey();
    }

    private void printCandidates(JavaRDD <Candidate> candidates) {
        List <Candidate> collect = candidates.collect();
        System.out.println("Candidate Size: " + collect.size());

        for (Candidate candidate : collect) {
            System.out.println("Subgraph: " + candidate);
        }
    }

    private void print(JavaPairRDD <Integer, Candidate[]> candidates) {

        List <Tuple2 <Integer, Candidate[]>> collect = candidates.collect();

        for (Tuple2 <Integer, Candidate[]> t : collect) {
            String start = new StringBuilder("<").append(t._1).append(" , Candidates: {").toString();
            StringBuilder sb = new StringBuilder(start);
            for (Candidate candidate : t._2) {
                sb.append(candidate).append(" , ");
            }

            String log = sb.append("}>").toString();
            System.out.println(log);
        }
    }

    private void printCandidates(JavaPairRDD <Integer, Iterable <Candidate[]>> candidates) {

        List <Tuple2 <Integer, Iterable <Candidate[]>>> collect = candidates.collect();
        for (Tuple2 <Integer, Iterable <Candidate[]>> t : collect) {
            String start = new StringBuilder("<").append(t._1).append(" , Candidates: {").toString();
            for (Candidate[] cArray : t._2) {
                StringBuilder sb = new StringBuilder(start);
                for (Candidate candidate : cArray) {
                    sb.append(candidate).append(" , ");
                }
                String log = sb.append("}>").toString();
                System.out.println(log);
            }
        }
    }

    private void printFonl(JavaPairRDD <Integer, Fvalue <LabelMeta>> labelFonl) {
        List <Tuple2 <Integer, Fvalue <LabelMeta>>> collect = labelFonl.collect();
        for (Tuple2 <Integer, Fvalue <LabelMeta>> t : collect) {
            System.out.println(t);
        }
    }

    private JavaPairRDD <Integer, Fvalue <LabelMeta>> startNodes(JavaPairRDD <Integer, Fvalue <LabelMeta>> labelFonl) {
        JavaPairRDD <Integer, Iterable <Integer>> neighborMsg = labelFonl.flatMapToPair(kv -> {
            List <Tuple2 <Integer, Integer>> list = new ArrayList <>();
            for (int v : kv._2.fonl) {
                list.add(new Tuple2 <>(v, kv._1));
            }
            return list.iterator();
        }).groupByKey(labelFonl.getNumPartitions());

        return labelFonl.leftOuterJoin(neighborMsg)
                .filter(kv -> !kv._2._2.isPresent())
                .mapValues(value -> value._1);
    }

    private static JavaPairRDD <Integer, String> getLables(JavaSparkContext sc, String path, int pNum) {
        return sc
                .textFile(path, pNum)
                .map(line -> line.split("\\s+"))
                .mapToPair(split -> new Tuple2 <>(Integer.parseInt(split[0]), split[1]));
    }

    public static void main(String[] args) {
        long t1 = System.currentTimeMillis();
        SGMatcherConf conf = new SGMatcherConf(new ArgumentReader(args));
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        JavaPairRDD <Integer, String> labels = getLables(conf.getSc(), conf.getLablePath(), conf.getPartitionNum());
        SPMiner SPMiner = new SPMiner(conf, edgeLoader, labels);

        Map <Integer, List <Integer>> neighbors = new HashMap <>();
        neighbors.put(1, Arrays.asList(4, 2, 3));
        neighbors.put(2, Arrays.asList(3, 1));
        neighbors.put(3, Arrays.asList(2, 1));
        neighbors.put(4, Arrays.asList(1));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "B");
        labelMap.put(3, "C");
        labelMap.put(4, "A");

        Sonl sonl = LocalFonlCreator.createSonl(neighbors, labelMap);

        SPMiner.search(sonl);

        SPMiner.close();
    }
}
