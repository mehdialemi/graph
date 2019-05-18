package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.*;
import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class SPMiner extends SparkApp {

    private FSearchConf conf;
    private EdgeLoader edgeLoader;

    public SPMiner(FSearchConf conf, EdgeLoader edgeLoader) {
        super(edgeLoader);
        this.conf = conf;
        this.edgeLoader = edgeLoader;
    }

    public void search(LabelFonl query) {
        // load label
        JavaPairRDD <Integer, String> lables = conf.getSc()
                .textFile(conf.getLablePath(), conf.getPartitionNum())
                .map(line -> line.split("\\s+"))
                .mapToPair(split -> new Tuple2 <>(Integer.parseInt(split[0]), split[1]));

        JavaPairRDD <Integer, Fvalue <LabelMeta>> lFonl = SparkFonlCreator.createLabelFonl(
                new NeighborList(edgeLoader), lables);

        printFonl(lFonl);

        final Broadcast <LabelFonl> queryBroadCast = conf.getSc().broadcast(query);

        JavaPairRDD <Integer, Iterable <Candidate>> candidates = lFonl.flatMapToPair(kv -> {
            int vertex = kv._1;
            Fvalue <LabelMeta> fvalue = kv._2;
            LabelFonl labelFonl = queryBroadCast.getValue();

            CandidGen candidGen = new CandidGen(vertex, fvalue, labelFonl);
            List <Tuple2 <Integer, Candidate>> newCandidates = candidGen.newCandidates();

            return newCandidates.iterator();
        }).groupByKey();

        JavaRDD<Candidate> patterns = conf.getSc().parallelize(Collections.emptyList());

        while (true) {

            long count = candidates.count();

            printCandidates(candidates);
            System.out.println("candidate count: " + count);

            if (count == 0)
                break;

            JavaRDD <Candidate> found = candidates.flatMap(kv -> {
                List<Candidate> list = new ArrayList <>();
                for (Candidate candidate : kv._2) {
                    if (candidate.isFull())
                        list.add(candidate);
                }
                return list.iterator();
            });

            System.out.println("FOUND ******");
            printCandidates(found);

            patterns = patterns.union(found);

            printCandidates(candidates);
            printFonl(lFonl);

            candidates = candidates
                    .join(lFonl)
                    .flatMapToPair(kv -> {
                        int vertex = kv._1;
                        LabelFonl labelFonl = queryBroadCast.getValue();
                        Fvalue <LabelMeta> fvalue = kv._2._2;
                        CandidGen candidGen = new CandidGen(vertex, fvalue, labelFonl);
                        List <Tuple2 <Integer, Candidate>> newCandidates = new ArrayList <>();
                        for (Candidate candidate : kv._2._1) {
                            if (candidate.isFull())
                                continue;
                            newCandidates.addAll(candidGen.newCandidates(candidate));
                        }

                        return newCandidates.iterator();
                    }).groupByKey(lFonl.getNumPartitions())
                    .persist(StorageLevel.MEMORY_AND_DISK());
        }

        printCandidates(patterns);

    }

    private void printCandidates( JavaRDD <Candidate> candidates) {
        List <Candidate> collect = candidates.collect();
        System.out.println("Candidate Size: " + collect.size());

        for (Candidate  candidate : collect) {
            StringBuilder sb = new StringBuilder();
            sb.append(candidate).append(" , ");
        }
    }

    private void printCandidates(  JavaPairRDD <Integer, Iterable <Candidate>>  candidates) {

        List <Tuple2 <Integer,  Iterable <Candidate>>> collect = candidates.collect();
        System.out.println("Candidate Size: " + collect.size());

        for (Tuple2 <Integer,  Iterable <Candidate>> candidateKV : collect) {
            for (Candidate candidate : candidateKV._2) {
                StringBuilder sb = new StringBuilder("<Vertex: ");
                sb.append(candidateKV._1).append(" , ");
                sb.append(candidate).append(" >");

                System.out.println("KV: " + sb.toString());
            }

        }
    }

    private void printFonl(JavaPairRDD <Integer, Fvalue <LabelMeta>> labelFonl ) {
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

    public static void main(String[] args) {
        long t1 = System.currentTimeMillis();
        FSearchConf conf = new FSearchConf(new ArgumentReader(args));
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        SPMiner SPMiner = new SPMiner(conf, edgeLoader);

        Map <Integer, List <Integer>> neighbors = new HashMap <>();
        neighbors.put(1, Arrays.asList(2, 3));
        neighbors.put(2, Arrays.asList(1, 3));
        neighbors.put(3, Arrays.asList(1, 2));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "B");
        labelMap.put(3, "C");

        LabelFonl labelFonl = LocalFonlCreator.create(neighbors, labelMap);

        SPMiner.search(labelFonl);

        SPMiner.close();
    }
}
