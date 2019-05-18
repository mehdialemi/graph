package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.*;
import ir.ac.sbu.graph.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
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

        JavaPairRDD <Integer, List <SCandidate>> candidates = lFonl.flatMapToPair(kv -> {
            int vertex = kv._1;
            Fvalue <LabelMeta> fvalue = kv._2;
            int vDeg = fvalue.meta.deg;

            LabelFonl labelFonl = queryBroadCast.getValue();
            int index = labelFonl.lowerDegIndex(vDeg);
            if (index < 0)
                return Collections.emptyIterator();

            String label = fvalue.meta.label;
            int[] indexes = labelFonl.vIndexes(index, label);
            if (indexes == null)
                return Collections.emptyIterator();

            int[] nDegs = fvalue.meta.degs;
            String[] nLabels = fvalue.meta.labels;

            List <Tuple2 <Integer, List <SCandidate>>> newCandidates = new ArrayList <>();
            for (int i = 0; i < nDegs.length; i++) {
                List <SCandidate> cands = null;
                for (int vIndex : indexes) {
                    int[] vNeighborIndexes = labelFonl.nIndexes(vIndex, nDegs[i], nLabels[i]);

                    if (vNeighborIndexes == null)
                        continue;

                    SCandidate cand = new SCandidate(labelFonl.size());
                    cand.set(vIndex);
                    for (int neighborIndex : vNeighborIndexes) {
                        cand.set(neighborIndex);
                    }

                    if (cands == null)
                        cands = new ArrayList <>();

                    cands.add(cand);
                }

                if (cands != null) {
                    int nId = fvalue.fonl[i];
                    newCandidates.add(new Tuple2 <>(nId, cands));
                }
            }
            return newCandidates.iterator();
        });

        printCandidates(candidates);
//        while (true) {
//            lFonl.flatMapToPair(kv -> {
//
//            })
//        }

    }

    private void printCandidates( JavaPairRDD <Integer, List <SCandidate>> candidates) {
        for (Tuple2 <Integer, List <SCandidate>> candidateKV : candidates.collect()) {
            StringBuilder sb = new StringBuilder("<Vertex: ");
            sb.append(candidateKV._1).append(" , Candidates: ");
            for (SCandidate sCandidate : candidateKV._2) {
                sb.append(sCandidate).append("  ";
            }
            sb.append(">");
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
        Map <Integer, String> labelMap = new HashMap <>();

        LabelFonl labelFonl = LocalFonlCreator.create(neighbors, labelMap);

        SPMiner.search(labelFonl);

        SPMiner.close();
    }
}
