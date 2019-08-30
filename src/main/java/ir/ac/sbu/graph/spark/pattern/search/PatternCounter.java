package ir.ac.sbu.graph.spark.pattern.search;

import ir.ac.sbu.graph.spark.pattern.index.IndexRow;
import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

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

    public Int2IntOpenHashMap counts() {
        if (subquery.links == null) {
            subquery.links = new IntOpenHashSet();
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

            if (indexStates[index] == null || !indexStates[index].isIncluded(v, mList)) {
                continue;
            }

            mSet.add(v);
            mList.add(v);

            join(index + 1, mSet, mList);

            mSet.remove(v);
            mList.remove(v);
        }
    }

    public boolean finalize(int index) {
        if (selects[index] == null)
            return false;

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
                for (int srcVertex : selects[srcIndex]) {
                    if (indexRow.hasEdge(vertex, srcVertex)) {
                        if (indexStates[index] == null)
                            indexStates[index] = new IndexState();
                        indexStates[index].add(vertex, srcIndex, srcVertex);
                    }
                }
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

        // (destination vertex) -> list of source index
        private Int2ObjectMap<IntList> v2Indices;

        IndexState() {
            v2IndexVertices = new Int2ObjectOpenHashMap<>();
            v2Indices = new Int2ObjectOpenHashMap<>();
        }

        public void add(int dest, int srcIndex, int srcVertex) {
            long num = Edge.longEdge(srcIndex, srcVertex);
            v2IndexVertices.computeIfAbsent(dest, n -> new LongOpenHashSet()).add(num);
            v2Indices.computeIfAbsent(dest, n -> new IntArrayList()).add(srcIndex);
        }

        public boolean isIncluded(int dest, IntList srcVertices) {
            IntList indices = v2Indices.get(dest);
            if (indices == null)
                return true;

            LongSet set = v2IndexVertices.get(dest);
            for (int index : indices) {
                int srcVertex = srcVertices.get(index);
                long num = Edge.longEdge(index, srcVertex);
                if (!set.contains(num))
                    return false;
            }

            return true;
        }
    }
}
