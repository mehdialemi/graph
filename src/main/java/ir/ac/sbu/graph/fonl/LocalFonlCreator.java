package ir.ac.sbu.graph.fonl;

import ir.ac.sbu.graph.types.VertexDeg;

import java.util.*;

public class LocalFonlCreator{



    public static LabelFonl create(Map<Integer, List<Integer>> neighbors, Map<Integer, String> labelMap) {
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

//                if (nDeg < degree || (nDeg == degree && neighborId < vId))
//                    continue;

                sortedSet.add(new VertexDeg(neighborId, nDeg));
            }

            sortedMap.put(new VertexDeg(vId, degree), sortedSet);
        }

        LabelFonl labelFonl = new LabelFonl(sortedMap.size());

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
            int[] nDegs = new int[vertexDegs.length];
            String[] labels = new String[vertexDegs.length];
            for (int i = 0; i < vertexDegs.length; i++) {
                nIndex[i] = vIndex.get(vertexDegs[i].vertex);
                nDegs[i] = vertexDegs[i].degree;
                labels[i] = labelMap.get(vertexDegs[i].vertex);
            }

            Fvalue<LabelMeta> fvalue = new Fvalue <>();
            fvalue.fonl = nIndex;
            fvalue.meta = new LabelMeta();
            fvalue.meta.deg = degree;
            fvalue.meta.label = labelMap.get(vertex);
            fvalue.meta.labels = labels;
            fvalue.meta.degs = nDegs;

            labelFonl.add(vertex, degree, fvalue);
        }

        return labelFonl;
    }
}
