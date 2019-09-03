package ir.ac.sbu.graph.spark.pattern.search;

import ir.ac.sbu.graph.spark.pattern.index.IndexRow;
import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import it.unimi.dsi.fastutil.ints.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class PatternCounter {

    private IndexRow row;
    private Subquery subquery;
    private IntSet[] selects;
    //    private IndexState[] indexStates;
//    private Int2ObjectMap<IntSet> v2Indices;
//    private int[] allCounts;

    public PatternCounter(IndexRow row, Subquery subquery) {
        this.row = row;
        this.subquery = subquery;
        this.selects = new IntSet[subquery.size()];
//        this.indexStates = new IndexState[subquery.size()];
//        this.allCounts = new int[subquery.size()];
        if (subquery.linkIndices == null) {
            subquery.linkIndices = new IntOpenHashSet();
            subquery.linkIndices.add(0);
        }
    }

    public Iterator<Tuple2<Integer, MatchCount>> candidates() {
        if (row.size() < subquery.size() || row.maxDegree() < subquery.maxDegree())
            return Collections.emptyIterator();

        if (!isMatched(0, 0))
            return Collections.emptyIterator();

        int patterns = 1;
        Int2IntOpenHashMap vCounter = new Int2IntOpenHashMap();
        for (int index = 1; index < subquery.size(); index++) {
            for (int i = 1; i < row.size(); i++) {
                if (!isMatched(index, i))
                    continue;

                if (selects[index] == null)
                    selects[index] = new IntOpenHashSet();
                selects[index].add(row.vertices[i]);

                if (subquery.tc(index) == 0) {
                    vCounter.addTo(row.vertices[i], 1);
                    patterns *= selects[index].size();
                }
            }

            if (selects[index] == null)
                return Collections.emptyIterator();

            if (selects[index].size() == 1) {
                int v = selects[index].iterator().nextInt();
                for (int i = 1; i < index - 1; i++) {
                    selects[i].remove(v);
                    if (selects[i].size() == 0)
                        return Collections.emptyIterator();
                }
            }
        }

        Int2ObjectMap[] nSelects = null;
        if (subquery.l2rIndex.size() > 0)
            nSelects = new Int2ObjectMap[subquery.size()];

        for (int index = 0; index < selects.length; index++) {
            if (subquery.tc(index) == 0)
                continue;

            IntSet rIndices = subquery.l2rIndex.get(index);
            if (rIndices != null) {
                IntSet lSet = selects[index];
                for (int rIndex : rIndices) {
                    IntSet rSet = selects[rIndex];

                    int sum = 0;
                    // TODO lazy
                    IntSet uSelect = new IntOpenHashSet();
                    IntSet removes = new IntOpenHashSet();
                    for (int v : lSet) {
                        IntSet nSet = new IntOpenHashSet();
                        for (int v2: rSet) {
                            if (row.hasEdge(v, v2)) {
                                nSet.add(v2);
                            }
                        }

                        if (nSet.isEmpty()) {
                            removes.add(v);
                            continue;
                        }

                        sum += nSet.size();
                        uSelect.addAll(nSet);

                        if (nSelects[index] == null)
                            nSelects[index] = new Int2ObjectArrayMap();
                        nSelects[index].put(v, nSet);
                    }

                    lSet.removeAll(removes);
                    selects[rIndex] = uSelect;

                    if (lSet.isEmpty() || selects[rIndex].isEmpty())
                        return Collections.emptyIterator();

                    patterns *= sum;
                }
            }
        }

        List<Tuple2<Integer, MatchCount>> out = new ArrayList<>();
        for (int linkIndex : subquery.linkIndices) {
            if (selects[linkIndex] != null) {
                int count = patterns / selects[linkIndex].size();
                for (Integer v : selects[linkIndex]) {
                    out.add(new Tuple2<>(v, new MatchCount(row.vertices[0], count, linkIndex)));
                }
            } else if (nSelects[linkIndex] != null) {
                int count = patterns / nSelects[linkIndex].size();
                for (Integer v : nSelects[linkIndex].keySet()) {
                    out.add(new Tuple2<>(v, new MatchCount(row.vertices[0], count, linkIndex)));
                }
            }
        }

        return out.iterator();
    }

    private int factor(int count) {
        int num = count - 1;
        return (num * (num - 1)) / 2;
    }

    private boolean isMatched(int qIndex, int rIndex) {
        if (!row.labels[rIndex].equals(subquery.labels[qIndex]) ||
                row.degrees[rIndex] < subquery.degrees[qIndex])
            return false;

        if (subquery.tc[qIndex] > 0)
            return row.tc != null && row.tc[rIndex] >= subquery.tc[qIndex];

        return true;
    }

//
//
//    public Iterator<Tuple2<Integer, MatchCount>> counts() {
//
//        if (row.size() < subquery.size() || row.maxDegree() < subquery.maxDegree())
//            return Collections.emptyIterator();
//
//        if (!isMatched(0, 0))
//            return Collections.emptyIterator();
//
//        selects[0] = new IntOpenHashSet();
//        selects[0].add(row.vertices[0]);
//
//        for (int qIndex = 1; qIndex < subquery.size(); qIndex++) {
//            IntSet srcIndices = subquery.right2Left.get(qIndex);
//
//            for (int rIndex = 1; rIndex < row.size(); rIndex++) {
//                int vertex = row.vertices[rIndex];
//
//                if (!isMatched(qIndex, rIndex)) {
//                    continue;
//                }
//
//                if (srcIndices != null) {
//                    boolean hasConnection = false;
//                    for (int srcIndex : srcIndices) {
//                        if (selects[srcIndex] == null) {
//                            throw new RuntimeException("qIndex: " + qIndex + ", srcIndex: " +
//                                    srcIndex + ", subquery: " + subquery);
//                        }
//
//                        // Check if the given vertex is connected with at least one item of the given vSet
//                        hasConnection = false;
//                        for (int srcVertex : selects[srcIndex]) {
//                            if (row.hasEdge(vertex, srcVertex)) {
//                                hasConnection = true;
//                                if (indexStates[qIndex] == null)
//                                    indexStates[qIndex] = new IndexState();
//                                indexStates[qIndex].add(vertex, srcIndex, srcVertex);
//                            }
//                        }
//
//                        if (!hasConnection)
//                            break;
//                    }
//
//                    if (!hasConnection)
//                        continue;
//                }
//
//                if (selects[qIndex] == null) {
//                    selects[qIndex] = new IntOpenHashSet();
//                }
//
//                if (v2Indices == null)
//                    v2Indices = new Int2ObjectOpenHashMap<>();
//
//                v2Indices.computeIfAbsent(vertex, i -> new IntOpenHashSet()).add(qIndex);
//                selects[qIndex].add(vertex);
//            }
//
//            if (!finalize(qIndex))
//                return Collections.emptyIterator();
//        }
//
//        Int2IntOpenHashMap counts = linkCounts();
//        List<Tuple2<Integer, MatchCount>> out = new ArrayList<>();
//        for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
//            MatchCount matchCount =
//                    new MatchCount(row.vertices[0], entry.getValue(), entry.getKey());
//            out.add(new Tuple2<>(row.vertices[entry.getKey()], matchCount));
//        }
//
//        return out.iterator();
//    }
//
//    private Int2IntOpenHashMap linkCounts() {
//        if (subquery.linkIndices.isEmpty()) {
//            subquery.linkIndices.add(0);
//        }
//
//        join(0, new IntOpenHashSet(), new IntArrayList());
//
//        return counts;
//    }
//
//    private void join(int index, IntSet mSet, IntList mList) {
//        if (index >= subquery.size()) {
//            for (int link : subquery.linkIndices) {
//                counts.addTo(link, 1);
//            }
//            return;
//        }
//
//        for (int v : selects[index]) {
//            if (mSet.contains(v))
//                continue;
//
//            if (indexStates[index] != null && !indexStates[index].isIncluded(v, mList)) {
//                continue;
//            }
//
//            mSet.add(v);
//            mList.add(v);
//
//            join(index + 1, mSet, mList);
//
//            mSet.rem(v);
//            mList.rem(v);
//        }
//    }

//    public boolean finalize(int index) {
//        if (selects[index] == null)
//            return false;
//
//        if (v2Indices == null)
//            return true;
//
//        // if there is only one vertex for the current index,
//        // then no others should have the current vertex
//        if (selects[index].size() == 1) {
//            int vertex = selects[index].iterator().nextInt();
//            IntSet v2IndexSet = v2Indices.get(vertex);
//            if (v2IndexSet != null && v2IndexSet.size() > 0) {
//                for (Integer vIndex : v2IndexSet) {
//                    selects[vIndex].remove(vertex);
//                    if (selects[vIndex].isEmpty())
//                        return false;
//                }
//            }
//        }
//        return true;
//    }
//
//    static class IndexState {
//        // (destination vertex) -> list of (index, source) vertex
//        private Int2ObjectMap<LongSet> v2IndexVertices;
//
//        private Long2ObjectOpenHashMap<IntSet> indexVertex2Neighbors;
//
//        // (destination vertex) -> list of source index
//        private Int2ObjectMap<IntList> v2Indices;
//
//        IndexState() {
//            v2IndexVertices = new Int2ObjectOpenHashMap<>();
//            v2Indices = new Int2ObjectOpenHashMap<>();
//            indexVertex2Neighbors = new Long2ObjectOpenHashMap<>();
//        }
//
//        public void add(int dest, int srcIndex, int srcVertex) {
//            long num = Edge.longEdge(srcIndex, srcVertex);
//            v2IndexVertices.computeIfAbsent(dest, n -> new LongOpenHashSet()).add(num);
//            v2Indices.computeIfAbsent(dest, n -> new IntArrayList()).add(srcIndex);
//
//            long key = Edge.longEdge(srcIndex, dest);
//            indexVertex2Neighbors.computeIfAbsent(key, n -> new IntOpenHashSet()).add(srcVertex);
//        }
//
//        /**
//         * Check if for the given vertex, for its connections, the selected vertices are
//         * its neighbors
//         *
//         * @param dest        vertex to check the neighborhood
//         * @param srcVertices selected vertices in the pattern matching
//         * @return true if the relationship has been hold; otherwise, false.
//         */
//        boolean isIncluded(int dest, IntList srcVertices) {
//            IntList indices = v2Indices.get(dest);
//            if (indices == null)
//                return true;
//
//            for (int index = 0; index < srcVertices.size(); index++) {
//                if (!indices.contains(index))
//                    continue;
//
//                long key = Edge.longEdge(index, dest);
//                IntSet vertices = indexVertex2Neighbors.get(key);
//                if (vertices == null || !vertices.contains(srcVertices.getInt(index)))
//                    return false;
//            }
//
//            return true;
////            LongSet set = v2IndexVertices.get(dest);
////            for (int index : indices) {
////                int srcVertex = srcVertices.get(index);
////                long num = Edge.longEdge(index, srcVertex);
////                if (!set.contains(num))
////                    return false;
////            }
////
////            return true;
//        }
//    }
}
