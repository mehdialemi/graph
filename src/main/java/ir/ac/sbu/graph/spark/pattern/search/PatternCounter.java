package ir.ac.sbu.graph.spark.pattern.search;

import ir.ac.sbu.graph.spark.pattern.index.IndexRow;
import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import scala.Tuple2;

import java.util.*;

public class PatternCounter {

    private IndexRow indexRow;
    private Subquery subquery;
    private IntSet[] selects;
    private IndexState[] indexStates;
    private Int2ObjectMap<IntSet> v2Indices;
    private Int2IntOpenHashMap counts;

    public PatternCounter(IndexRow indexRow, Subquery subquery) {
        this.indexRow = indexRow;
        this.subquery = subquery;
        this.selects = new IntSet[subquery.size()];
        this.indexStates = new IndexState[subquery.size()];
        this.counts = new Int2IntOpenHashMap();
        if (subquery.links == null) {
            subquery.links = new IntOpenHashSet();
            subquery.links.add(0);
        }
    }

    private boolean isMatched(int qIndex, int rIndex) {
        if (!indexRow.labels[rIndex].equals(subquery.labels[qIndex]) ||
                indexRow.degrees[rIndex] < subquery.degrees[qIndex])
            return false;
        if (subquery.tc[qIndex] > 0) {
            if (indexRow.tc == null)
                return false;
            if (indexRow.tc[rIndex] < subquery.tc[qIndex])
                return false;
        }
        return true;
    }

    public Iterator<Tuple2<Integer, MatchCount>> counts() {

        if (indexRow.size() < subquery.size() || indexRow.maxDegree() < subquery.maxDegree())
            return Collections.emptyIterator();

        if(!isMatched(0, 0))
            return Collections.emptyIterator();

        selects[0] = new IntOpenHashSet();
        selects[0].add(indexRow.vertices[0]);

        for (int qIndex = 1; qIndex < subquery.size(); qIndex++) {
            IntSet srcIndices = subquery.srcEdgeIndices.get(qIndex);

            for (int rIndex = 1; rIndex < indexRow.size(); rIndex++) {

                if (!isMatched(qIndex, rIndex))
                    continue;

                add(qIndex, indexRow.vertices[rIndex], srcIndices);
            }

            if (!finalize(qIndex))
                return Collections.emptyIterator();
        }

        Int2IntOpenHashMap counts = linkCounts();
        List<Tuple2<Integer, MatchCount>> out = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
            MatchCount matchCount =
                    new MatchCount(indexRow.vertices[0], entry.getValue(), entry.getKey());
            out.add(new Tuple2<>(indexRow.vertices[entry.getKey()], matchCount));
        }

        return out.iterator();
    }

    private Int2IntOpenHashMap linkCounts() {
        if (subquery.links.isEmpty()) {
            subquery.links.add(0);
        }

        join(0, new IntOpenHashSet(), new IntArrayList());

        return counts;
    }

    private void join(int index, IntSet mSet, IntList mList) {
        if (index >= subquery.size()) {
            for (int link : subquery.links) {
                counts.addTo(link, 1);
            }
            return;
        }

        for (int v : selects[index]) {
            if (mSet.contains(v))
                continue;

            if (indexStates[index] != null && !indexStates[index].isIncluded(v, mList)) {
                continue;
            }

            mSet.add(v);
            mList.add(v);

            join(index + 1, mSet, mList);

            mSet.rem(v);
            mList.rem(v);
        }
    }

    public boolean finalize(int index) {
        if (selects[index] == null)
            return false;

        if (v2Indices == null)
            return true;

        // if there is only one vertex for the current index,
        // then no others should have the current vertex
        if (selects[index].size() == 1) {
            int vertex = selects[index].iterator().nextInt();
            IntSet v2IndexSet = v2Indices.get(vertex);
            if (v2IndexSet != null && v2IndexSet.size() > 0) {
                for (Integer vIndex : v2IndexSet) {
                    selects[vIndex].remove(vertex);
                    if (selects[vIndex].isEmpty())
                        return false;
                }
            }
        }
        return true;
    }

    public void add(int index, int vertex, IntSet srcIndices) {
        if (srcIndices != null) {
            for (int srcIndex : srcIndices) {

                if (selects[srcIndex] == null) {
                    throw new RuntimeException("index: " + index + ", srcIndex: " + srcIndex +
                            ", subquery: " + subquery);
                }

                // Check if the given vertex is connected with at least one item of the given vSet
                boolean hasConnection = false;
                for (int srcVertex : selects[srcIndex]) {
                    if (indexRow.hasEdge(vertex, srcVertex)) {
                        hasConnection = true;
                        if (indexStates[index] == null)
                            indexStates[index] = new IndexState();
                        indexStates[index].add(vertex, srcIndex, srcVertex);
                    }
                }

                if (!hasConnection)
                    return;
            }
        }

        if (selects[index] == null) {
            selects[index] = new IntOpenHashSet();
        }

        if (v2Indices == null)
            v2Indices = new Int2ObjectOpenHashMap<>();

        v2Indices.computeIfAbsent(vertex, i -> new IntOpenHashSet()).add(index);
        selects[index].add(vertex);
    }

    static class IndexState {
        // (destination vertex) -> list of (index, source) vertex
        private Int2ObjectMap<LongSet> v2IndexVertices;

        private Long2ObjectOpenHashMap<IntSet> indexVertex2Neighbors;

        // (destination vertex) -> list of source index
        private Int2ObjectMap<IntList> v2Indices;

        IndexState() {
            v2IndexVertices = new Int2ObjectOpenHashMap<>();
            v2Indices = new Int2ObjectOpenHashMap<>();
            indexVertex2Neighbors = new Long2ObjectOpenHashMap<>();
        }

        public void add(int dest, int srcIndex, int srcVertex) {
            long num = Edge.longEdge(srcIndex, srcVertex);
            v2IndexVertices.computeIfAbsent(dest, n -> new LongOpenHashSet()).add(num);
            v2Indices.computeIfAbsent(dest, n -> new IntArrayList()).add(srcIndex);

            long key = Edge.longEdge(srcIndex, dest);
            indexVertex2Neighbors.computeIfAbsent(key, n -> new IntOpenHashSet()).add(srcVertex);
        }

        /**
         * Check if for the given vertex, for its connections, the selected vertices are
         * its neighbors
         * @param dest vertex to check the neighborhood
         * @param srcVertices  selected vertices in the pattern matching
         * @return true if the relationship has been hold; otherwise, false.
         */
        boolean isIncluded(int dest, IntList srcVertices) {
            IntList indices = v2Indices.get(dest);
            if (indices == null)
                return true;

            for (int index = 0; index < srcVertices.size(); index++) {
                if (!indices.contains(index))
                    continue;

                long key = Edge.longEdge(index, dest);
                IntSet vertices = indexVertex2Neighbors.get(key);
                if (vertices == null || !vertices.contains(srcVertices.getInt(index)))
                    return false;
            }

            return true;
//            LongSet set = v2IndexVertices.get(dest);
//            for (int index : indices) {
//                int srcVertex = srcVertices.get(index);
//                long num = Edge.longEdge(index, srcVertex);
//                if (!set.contains(num))
//                    return false;
//            }
//
//            return true;
        }
    }
}
