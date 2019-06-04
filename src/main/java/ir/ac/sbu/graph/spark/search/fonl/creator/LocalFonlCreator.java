package ir.ac.sbu.graph.spark.search.fonl.creator;

import ir.ac.sbu.graph.spark.search.fonl.local.LocalFonl;
import ir.ac.sbu.graph.spark.search.fonl.local.OrderedNeighbors;
import ir.ac.sbu.graph.spark.search.fonl.local.QFonl;
import ir.ac.sbu.graph.spark.search.fonl.local.QSplit;
import ir.ac.sbu.graph.spark.search.fonl.local.Subquery;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.VertexDeg;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;

import java.util.*;

public class LocalFonlCreator {

    public static List<Subquery> getSubqueries(QFonl qFonl) {
        List<Subquery> list = new ArrayList <>();

        for (QSplit split : qFonl.splits) {
            Subquery subquery = new Subquery();
            subquery.vertex = split.vIndex;
            subquery.degree = qFonl.degIndices[split.vIndex];
            subquery.label = qFonl.labels[split.vIndex];

            int[] fonl = qFonl.fonl[split.vIndex];
            subquery.fonl = fonl;
            subquery.degrees = new int[fonl.length];
            subquery.labels = new String[fonl.length];
            subquery.v2i = new Int2IntOpenHashMap();

            for (int i = 0; i < fonl.length; i++) {
                int index = fonl[i];
                subquery.v2i.put(fonl[i], i);
                subquery.degrees[i] = qFonl.degIndices[index];
                subquery.labels[i] = qFonl.labels[index];

                if (qFonl.edgeArray[i] != null)
                    subquery.vi2List = qFonl.edgeArray[i];
            }

            list.add(subquery);
        }

        return list;
    }

    public static QFonl createQFonl(Map <Integer, List <Integer>> neighbors, Map <Integer, String> labelMap) {

        LocalFonl localFonl = createFonl(neighbors, labelMap);

        // Each Edge(v1, v2) determines v1 = index of left vertex, v2 = index of right vertex
        Set <Edge> edges = new HashSet <>();
        for (int vIdx1 = 0; vIdx1 < localFonl.vIndices.length; vIdx1++) {
            for (int vIdx2 : localFonl.fonl[vIdx1]) {
                edges.add(new Edge(vIdx1, vIdx2));
            }
        }

        // Fill qFonl using localFonl and then determine split points per vertex (idx)
        // Note that if for any vertex (idx) splits is null then no split is determined for that vertex
        QFonl qFonl = new QFonl(localFonl);

        Map <Integer, IntOpenHashSet> triangleTrack = new HashMap <>();
        // Track of connected vertices to each vertex (idx)
        Map <Integer, IntOpenHashSet> fTrack = new HashMap <>();
        Int2IntOpenHashMap tc = new Int2IntOpenHashMap();
        for (int vIndex = 0; vIndex < qFonl.vIndices.length; vIndex++) {
            tc.put(vIndex, 0);
            fTrack.put(vIndex, new IntOpenHashSet(qFonl.fonl[vIndex]));
        }

        // find triangles per fonl item (fonl value for one vertex)
        int vIdx = 0;
        for (int[] fonl : qFonl.fonl) {
            for (int idx1 = 0; idx1 < fonl.length; idx1++) {
                for (int idx2 = idx1 + 1; idx2 < fonl.length; idx2++) {

                    // check existence of edge between two vertex
                    int vIdx1 = fonl[idx1];
                    int vIdx2 = fonl[idx2];
                    Edge candidate = new Edge(vIdx1, vIdx2);
                    if (edges.contains(candidate)) {

                        qFonl.edgeArray[vIdx].computeIfAbsent(vIdx1, set -> new IntOpenHashSet()).add(vIdx2);

                        IntOpenHashSet tSet = triangleTrack.computeIfAbsent(vIdx, set -> new IntOpenHashSet());
                        tSet.add(vIdx1);
                        tSet.add(vIdx2);
                        triangleTrack.computeIfAbsent(vIdx1, set -> new IntOpenHashSet()).add(vIdx2);

                        tc.addTo(vIdx1, 1);
                        tc.addTo(vIdx2, 1);
                        tc.addTo(vIdx, 1);
                    }
                }
            }
            vIdx++;
        }

        // Sort first consider number of triangle of vertices. If it is equal, consider fonl value size
        Object2IntAVLTreeMap <Map.Entry <Integer, Integer>> sortedMap =
                new Object2IntAVLTreeMap <>((o1, o2) -> {
                    if (!o1.getValue().equals(o2.getValue()))
                        return o2.getValue() - o1.getValue();

                    Integer vIdx1 = o1.getKey();
                    Integer vIdx2 = o2.getKey();

                    int fLen1 = qFonl.fonl[vIdx1].length;
                    int fLen2 = qFonl.fonl[vIdx2].length;

                    if (fLen1 != fLen2)
                        return fLen2 - fLen1;

                    int deg1 = qFonl.degIndices[vIdx1];
                    int deg2 = qFonl.degIndices[vIdx2];

                    if (deg1 != deg2)
                        return deg2 - deg1;

                    return vIdx1 - vIdx2;
                });


        for (Map.Entry <Integer, Integer> entry : tc.entrySet()) {
            sortedMap.put(entry, entry.getValue());
        }

        // remove triangle related vertices from other fTrackers
        for (Map.Entry <Integer, Integer> vTcEntry : sortedMap.keySet()) {
            vIdx = vTcEntry.getKey();
            for (Integer vIdx1 : fTrack.get(vIdx)) {
                fTrack.getOrDefault(vIdx1, new IntOpenHashSet())
                        .removeAll(triangleTrack.getOrDefault(vIdx, new IntOpenHashSet()));
            }
        }

        // remove indexes that don't need any joining
        Int2IntOpenHashMap fTrackCount = new Int2IntOpenHashMap();
        for (Map.Entry <Integer, IntOpenHashSet> entry : fTrack.entrySet()) {
            for (Integer vIndex : entry.getValue()) {
                fTrackCount.addTo(vIndex, 1);
            }
        }

        Int2ObjectAVLTreeMap<Map.Entry<Integer, IntOpenHashSet>> treeMap = new Int2ObjectAVLTreeMap<>();
        for (Map.Entry <Integer, IntOpenHashSet> entry : fTrack.entrySet()) {
            treeMap.put(entry.getValue().size(), entry);
        }

        List <QSplit> qSplits = new ArrayList <>();
        for (Map.Entry <Integer, IntOpenHashSet> fTrackItem : treeMap.values()) {
            vIdx = fTrackItem.getKey();
            IntOpenHashSet iSet = fTrackItem.getValue();

            IntSet intSet = new IntOpenHashSet();
            int[] fonl = qFonl.fonl[vIdx];
            for (int i = 0; i < fonl.length; i++) {
                int vertexIndex = fonl[i];
                if (iSet.contains(vertexIndex)) {
                    // if no other join would be required then continue
                    if (fTrack.getOrDefault(vertexIndex, new IntOpenHashSet()).size() == 0 && fTrackCount.get(vertexIndex) <= 1)
                        continue;
                    intSet.add(i);
                }
            }

            if (intSet.size() > 0) {
                QSplit qSplit = new QSplit(vIdx, intSet.toIntArray());
                qSplits.add(qSplit);
            }

        }

        qFonl.splits = qSplits.toArray(new QSplit[0]);

        return qFonl;
    }

    public static LocalFonl createFonl(Map <Integer, List <Integer>> neighbors, Map <Integer, String> labelMap) {
        OrderedNeighbors orderedNeighbors = createOrderedNeighbors(neighbors, labelMap);
        LocalFonl localFonl = new LocalFonl();
        localFonl.vIndices = orderedNeighbors.vArray;
        localFonl.degIndices = orderedNeighbors.dArray;
        localFonl.labels = orderedNeighbors.labels;
        localFonl.fonl = new int[orderedNeighbors.vArray.length][];
        int i = 0;
        for (int[] snIdx : orderedNeighbors.dsnArray) {
            int d = localFonl.degIndices[i];
            int v = localFonl.vIndices[i];

            IntSet set = new IntOpenHashSet();
            for (int idx : snIdx) {
                int dn = localFonl.degIndices[idx];
                int vn = localFonl.vIndices[idx];
                if (dn > d || (dn == d && vn > v))
                    set.add(idx);
            }
            localFonl.fonl[i] = set.toIntArray();
            i++;
        }
        return localFonl;
    }

    public static OrderedNeighbors createOrderedNeighbors(Map <Integer, List <Integer>> neighbors, Map <Integer, String> labelMap) {
        // sort vertex by their degree
        SortedMap <VertexDeg, SortedSet <VertexDeg>> sortedMap = new TreeMap <>((o1, o2) -> {
            int result = o1.degree - o2.degree;
            return result == 0 ? o1.vertex - o2.vertex : result;
        });

        for (Map.Entry <Integer, List <Integer>> entry : neighbors.entrySet()) {
            List <Integer> vNeighbors = entry.getValue();
            int degree = vNeighbors.size();
            int vId = entry.getKey();

            Iterator <Integer> nIter = vNeighbors.iterator();
            SortedSet <VertexDeg> sortedSet = new TreeSet <>((o1, o2) -> {
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

        OrderedNeighbors OrderedNeighbors = new OrderedNeighbors(sortedMap.size());

        Map <Integer, Integer> vIndex = new HashMap <>();
        int index = 0;
        for (VertexDeg key : sortedMap.keySet()) {
            vIndex.put(key.vertex, index++);
        }

        for (Map.Entry <VertexDeg, SortedSet <VertexDeg>> entry : sortedMap.entrySet()) {
            int vertex = entry.getKey().vertex;
            int degree = entry.getKey().degree;

            VertexDeg[] vertexDegs = entry.getValue().toArray(new VertexDeg[0]);

            int[] nIndex = new int[vertexDegs.length];
            for (int i = 0; i < vertexDegs.length; i++) {
                nIndex[i] = vIndex.get(vertexDegs[i].vertex);
            }

            String label = labelMap.get(vertex);
            OrderedNeighbors.add(vertex, degree, label, nIndex);
        }

        return OrderedNeighbors;
    }
}
