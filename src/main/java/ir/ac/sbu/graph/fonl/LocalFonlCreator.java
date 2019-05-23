package ir.ac.sbu.graph.fonl;

import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.VertexDeg;
import it.unimi.dsi.fastutil.ints.*;
import scala.Tuple2;

import java.util.*;

public class LocalFonlCreator {

    public static LocalTriangle createTriangle(Map<Integer, List<Integer>> neighbors, Map<Integer, String> labelMap) {
        LocalFonl localFonl = createFonl(neighbors, labelMap);
        Set<Edge> edges = new HashSet <>();
        for (int i = 0; i < localFonl.vIndex.length; i++) {
            for (int nIdx : localFonl.fonl[i]) {
                edges.add(new Edge(i, nIdx));
            }
        }

        LocalTriangle localTriangle = new LocalTriangle();
        localTriangle.dIndex = localFonl.dIndex;
        localTriangle.vIndex = localFonl.vIndex;
        localTriangle.labels = localFonl.labels;
        localTriangle.fonl = localFonl.fonl;


        Map<Integer, IntOpenHashSet> eTrack = new HashMap <>();
        int i = 0;
        Int2IntAVLTreeMap tc = new Int2IntAVLTreeMap();
        for (int j = 0; j < localTriangle.vIndex.length; j++) {
            tc.put(j, 0);
            eTrack.put(j, new IntOpenHashSet(localTriangle.fonl[j]));
        }

        for (int[] fonl : localTriangle.fonl) {
            for (int c1 = 0; c1 < fonl.length; c1++) {
                for (int c2 = c1 + 1; c2 < fonl.length; c2++) {
                    Edge e = new Edge(fonl[c1], fonl[c2]);
                    if (edges.contains(e)) {
                        localTriangle.edges.computeIfAbsent(i, integer -> new HashSet <>()).add(e);
                        Set <Integer> iSet = eTrack.computeIfAbsent(i, set -> new IntOpenHashSet());
                        iSet.add(fonl[c1]);
                        iSet.add(fonl[c2]);
                        tc.addTo(fonl[c1], 1);
                        tc.addTo(fonl[c2], 1);
                        tc.addTo(i, 1);
                    }
                }
            }
            i ++;
        }

        int[] keys = tc.keySet().toIntArray();
//        Int2ObjectMap<Object2ObjectMap<Tuple2<Integer, Integer>, IntSet>>[] extendMap = new Int2ObjectOpenHashMap[keys.length];

        Int2IntMap[] n2vIdxarray = new Int2IntOpenHashMap[keys.length];
        for (int k = keys.length - 1; k >= 0; k--) {
            IntSet tSet = new IntOpenHashSet();
            Int2IntMap nIdx2vIdx = new Int2IntOpenHashMap();

            int vIdx = tc.get(keys[k]);
            int[] fonl = localTriangle.fonl[vIdx];
            // sort neighbors based on triangle count in reverse
            TreeSet<Tuple2<Integer, Tuple2<Integer, Integer>>> treeSet = new TreeSet <>((o1 , o2) -> o2._1 - o1._1);
            int idx = 0;
            for (int nIdx : fonl) {
                int counter = tc.get(nIdx);
                if (counter > 0)
                    treeSet.add(new Tuple2 <>(counter, new Tuple2 <>(nIdx, idx)));
                idx ++;
            }

            for (Tuple2 <Integer, Tuple2 <Integer, Integer>> t : treeSet) {
                int nIdx = t._2._1;
                idx = t._2._2;
                IntOpenHashSet s = eTrack.getOrDefault(nIdx, new IntOpenHashSet());
                s.removeAll(tSet);
                if (s.size() == 0 && !tSet.contains(nIdx))
                    s.add(nIdx);

                if (s.size() > 0) {
                    nIdx2vIdx.put(nIdx, idx);
                }

                tSet.addAll(s);
            }

            n2vIdxarray[vIdx] = nIdx2vIdx;
        }

        int size = localTriangle.vIndex.length;
        localTriangle.nArray = new int[size][];
        localTriangle.vArray = new int[size][];
        for (int j = 0; j < n2vIdxarray.length; j++) {
            if (n2vIdxarray[j] == null || n2vIdxarray[j].size() == 0)
                continue;

            localTriangle.nArray[j] = new int[n2vIdxarray[j].size()];
            localTriangle.vArray[j] = new int[n2vIdxarray[j].size()];
            int k = 0;
            for (Map.Entry <Integer, Integer> entry : n2vIdxarray[j].entrySet()) {
                localTriangle.nArray[j][k] = entry.getKey();
                localTriangle.vArray[j][k] = entry.getValue();
                k ++;
            }
        }

        return localTriangle;
    }

    public static LocalFonl createFonl(Map<Integer, List<Integer>> neighbors, Map<Integer, String> labelMap) {
        Sonl sonl = createSonl(neighbors, labelMap);
        LocalFonl localFonl = new LocalFonl();
        localFonl.vIndex = sonl.vArray;
        localFonl.dIndex = sonl.dArray;
        localFonl.labels = sonl.labels;
        localFonl.fonl = new int[sonl.vArray.length][];
        int i = 0;
        for (int[] snIdx : sonl.dsnArray) {
            int d = localFonl.dIndex[i];
            int v = localFonl.vIndex[i];

            IntSet set = new IntOpenHashSet();
            for (int idx : snIdx) {
                int dn = localFonl.dIndex[idx];
                int vn = localFonl.vIndex[idx];
                if (dn > d || (dn == d && vn > v))
                    set.add(idx);
            }
            localFonl.fonl[i] = set.toIntArray();
            i ++;
        }
        return localFonl;
    }

    public static Sonl createSonl(Map<Integer, List<Integer>> neighbors, Map<Integer, String> labelMap) {
        // sort vertex by their degree
        SortedMap<VertexDeg, SortedSet<VertexDeg>> sortedMap = new TreeMap <>((o1, o2) -> {
            int result = o1.degree - o2.degree;
            return result == 0 ? o1.vertex - o2.vertex : result;
        });

        for (Map.Entry <Integer, List <Integer>> entry : neighbors.entrySet()) {
            List<Integer> vNeighbors = entry.getValue();
            int degree = vNeighbors.size();
            int vId = entry.getKey();

            Iterator <Integer> nIter = vNeighbors.iterator();
            SortedSet<VertexDeg> sortedSet = new TreeSet <>((o1, o2) -> {
                int result = o1.degree - o2.degree;
                return result == 0 ? o1.vertex - o2.vertex : result;
            });

            while (nIter.hasNext()) {
                int neighborId = nIter.next();
                int nDeg = neighbors.get(neighborId).size();

                sortedSet.add(new VertexDeg(neighborId, nDeg));
            }

            sortedMap.put(new VertexDeg(vId, degree), sortedSet);
        }

        Sonl SONL = new Sonl(sortedMap.size());

        Map<Integer, Integer> vIndex = new HashMap <>();
        int index = 0;
        for (VertexDeg key : sortedMap.keySet()) {
            vIndex.put(key.vertex, index ++);
        }

        for (Map.Entry <VertexDeg, SortedSet <VertexDeg>> entry : sortedMap.entrySet()) {
            int vertex = entry.getKey().vertex;
            int degree = entry.getKey().degree;

            VertexDeg[] vertexDegs = entry.getValue().toArray(new VertexDeg[0]);

            int[] nIndex= new int[vertexDegs.length];
            for (int i = 0; i < vertexDegs.length; i++) {
                nIndex[i] = vIndex.get(vertexDegs[i].vertex);
            }

            String label = labelMap.get(vertex);
            SONL.add(vertex, degree, label, nIndex);
        }

        return SONL;
    }
}
