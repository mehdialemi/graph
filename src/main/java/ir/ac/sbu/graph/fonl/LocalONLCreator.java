package ir.ac.sbu.graph.fonl;

import ir.ac.sbu.graph.types.VertexDeg;

import java.util.*;

public class LocalONLCreator {

    public static SONL create(Map<Integer, List<Integer>> neighbors, Map<Integer, String> labelMap) {
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

        SONL SONL = new SONL(sortedMap.size());

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
