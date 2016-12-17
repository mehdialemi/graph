package ir.ac.sbu.graph.ktruss.parallel;

import ir.ac.sbu.graph.Edge;
import ir.ac.sbu.graph.utils.PartitioningUtils;
import ir.ac.sbu.graph.utils.VertexCompare;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 *
 */
public class ParallelKTruss7 extends ParallelKTrussBase {

    private ForkJoinPool forkJoinPool;

    public ParallelKTruss7(Edge[] edges, int minSup, int threads) {
        super(edges, minSup, threads);
        forkJoinPool = new ForkJoinPool(threads);
    }

    @Override
    public void start() throws Exception {
        long tStart = System.currentTimeMillis();
        batchSelector = new AtomicInteger(0);
        int maxVertexNum = 0;
        for (Edge edge : edges) {
            if (edge.v1 > maxVertexNum)
                maxVertexNum = edge.v1;
            if (edge.v2 > maxVertexNum)
                maxVertexNum = edge.v2;
        }

        long tMax = System.currentTimeMillis();
        System.out.println("find maxVertexNum in " + (tMax - tStart) + " ms");

        // Construct degree arrayList such that vertexId is the index of the arrayList.
        final int vCount = maxVertexNum + 1;
        int[] d = new int[vCount];  // vertex degree
        for (Edge e : edges) {
            d[e.v1]++;
            d[e.v2]++;
        }


        long tDeg = System.currentTimeMillis();
        System.out.println("calculate deg in " + (tDeg - tMax) + " ms");

        final VertexCompare vertexCompare = new VertexCompare(d);
        int idx = 0;
        final Long2IntMap eIndexMap = new Long2IntOpenHashMap(edges.length);
        final AtomicInteger[] eSup = new AtomicInteger[edges.length];
        final long[] edgeArray = new long[edges.length];
        final Int2ObjectOpenHashMap<IntArrayList> fonl = new Int2ObjectOpenHashMap<>(vCount);
        for (Edge e : edges) {
            if (d[e.v1] < 2 || d[e.v2] < 2)
                continue;
            eSup[idx] = new AtomicInteger(0);
            int v1 = e.v1, v2 = e.v2;

            if (vertexCompare.compare(e.v1, e.v2) == 1) {
                v1 = e.v2;
                v2 = e.v1;
            }
            long v12 = (long) v1 << 32 | v2 & 0xFFFFFFFFL;
            edgeArray[idx] = v12;
            eIndexMap.put(v12, idx++);
            IntArrayList list = fonl.get(v1);
            if (list == null) {
                list = new IntArrayList();
                fonl.put(v1, list);
            }
            list.add(v2);
        }

        long tneighbors = System.currentTimeMillis();
        System.out.println("construct fonl in " + (tneighbors - tDeg) + " ms");

        final Int2IntMap partitions = PartitioningUtils.createPartition(fonl, threads);
        forkJoinPool.submit(() -> partitions.int2IntEntrySet().parallelStream().forEach(vp -> {
            IntArrayList fu = fonl.get(vp.getIntKey());
            vertexCompare.quickSort(fu, 0, fu.size() - 1);
        })).get();
        long tpartition = System.currentTimeMillis();
        System.out.println("find partitions in " + (tpartition - tneighbors) + " ms");

        Int2ObjectOpenHashMap<IntSet>[] evMap = new Int2ObjectOpenHashMap[threads];
        for (int i = 0; i < threads; i++) {
            evMap[i] = new Int2ObjectOpenHashMap(vCount);
        }

        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(p -> {
            ObjectIterator<Int2ObjectMap.Entry<IntArrayList>> iterator = fonl.int2ObjectEntrySet().fastIterator();
            while (iterator.hasNext()) {
                Int2ObjectMap.Entry<IntArrayList> f = iterator.next();
                if (partitions.get(f.getIntKey()) != p)
                    continue;

                int u = f.getIntKey();

                IntArrayList fu = f.getValue();
                for (int i = 0; i < fu.size(); i++) {
                    int v = fu.getInt(i);

                    IntArrayList fv = fonl.get(v);
                    if (fv == null) {
                        continue;
                    }

                    long uv = (long) u << 32 | v & 0xFFFFFFFFL;
                    int uvi = eIndexMap.get(uv);
                    int ui = i + 1, vi = 0;
                    int c = 0;
                    while (ui < fu.size() && vi < fv.size()) {
                        if (fu.getInt(ui) == fv.getInt(vi)) {
                            int w = fu.getInt(ui);
                            long uw = (long) u << 32 | w & 0xFFFFFFFFL;
                            long vw = (long) v << 32 | w & 0xFFFFFFFFL;
                            int uwi = eIndexMap.get(uw);
                            int vwi = eIndexMap.get(vw);
                            eSup[uwi].incrementAndGet();
                            eSup[vwi].incrementAndGet();

                            IntSet set = evMap[p].get(uvi);
                            if (set == null) {
                                set = new IntOpenHashSet(); // TODO set size
                                evMap[p].put(uvi, set);
                            }
                            set.add(w);

                            set = evMap[p].get(uwi);
                            if (set == null) {
                                set = new IntOpenHashSet(); // TODO set size
                                evMap[p].put(uwi, set);
                            }
                            set.add(v);

                            set = evMap[p].get(vwi);
                            if (set == null) {
                                set = new IntOpenHashSet(); // TODO set size
                                evMap[p].put(vwi, set);
                            }
                            set.add(u);

                            c++;
                            ui++;
                            vi++;
                        } else if (vertexCompare.compare(fu.getInt(ui), fv.getInt(vi)) == -1)
                            ui++;
                        else
                            vi++;
                    }
                    if (c == 0)
                        continue;
                    eSup[uvi].addAndGet(c);
                }
            }
        })).get();
        long ttc = System.currentTimeMillis();
        System.out.println("find triangles in " + (ttc - tpartition) + " ms");

        int c = 0;
        for (int i = 0; i < eSup.length; i++)
            if (eSup[i] != null && eSup[i].get() > 0)
                c++;

        long tcount = System.currentTimeMillis();
        System.out.println("tcount: " + c + " in " + (tcount - ttc) + " ms");

        int[] invalids = new int[c];
        final IntArrayList[] localInvalids = new IntArrayList[threads];
        for (int i = 0; i < threads; i++)
            localInvalids[i] = new IntArrayList(c / threads);

        batchSelector = new AtomicInteger(0);
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(t -> {
            while (true) {
                int start = batchSelector.getAndAdd(BATCH_SIZE);
                if (start >= eSup.length)
                    break;
                int end = Math.min(eSup.length, BATCH_SIZE + start);
                for(int i = start ; i < end; i ++) {
                    if (eSup[i] == null || eSup[i].get() >= minSup || eSup[i].get() == 0)
                        continue;
                    localInvalids[t].add(i);
                }
            }
        })).get();

        long tlocali = System.currentTimeMillis();
        System.out.println("fill local invalids in " + (tlocali - tcount) + " ms");

        int iteration = 0;
        while (true) {
            long t1 = System.currentTimeMillis();
            List<Tuple2<Integer, IntArrayList>> invalidOffset = new ArrayList<>();
            int loffset = 0;
            invalidOffset.add(new Tuple2<>(loffset, localInvalids[0]));
            for (int i = 1; i < localInvalids.length; i++) {
                loffset += localInvalids[i - 1].size();
                invalidOffset.add(new Tuple2<>(loffset, localInvalids[i]));
            }

            final int invalidSize = forkJoinPool.submit(() -> invalidOffset.parallelStream().map(packet -> {
                int offset = packet._1;
                for (int i = 0; i < packet._2.size(); i++) {
                    invalids[i + offset] = packet._2.getInt(i);
                }
                return packet._2.size();
            })).get().reduce((a, b) -> a + b).get();

            long tInvalid = System.currentTimeMillis();
            System.out.println("iteration " + ++iteration + ", invalid size: " + invalidSize + " time: " + (tInvalid - t1) + " ms");
            if (invalidSize == 0)
                break;

            for (int i = 0; i < localInvalids.length; i++)
                localInvalids[i].clear();

            forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(p -> {
                for (int i = 0; i < invalidSize; i++) {
                    int eIndex = invalids[i];
                    IntSet set = evMap[p].get(eIndex);
                    if (set == null)
                        continue;
                    IntIterator iterator = set.iterator();

                    while (iterator.hasNext()) {
                        int w = iterator.nextInt();
                        int u = (int) (edgeArray[eIndex] >> 32);
                        int v = (int) edgeArray[eIndex];
                        long e;
                        if (vertexCompare.compare(u, w) == -1) {
                            e = (long) u << 32 | w & 0xFFFFFFFFL;
                        } else {
                            e = (long) w << 32 | u & 0xFFFFFFFFL;
                        }

                        int index = eIndexMap.get(e);
                        int sup = eSup[index].decrementAndGet();
                        if (sup == minSup - 1) {
                            // add index to invalid indexes
                            localInvalids[p].add(index);
                        }
                        IntSet vList = evMap[p].get(index);
                        if (vList != null) {
                            vList.remove(v);
                            if (vList.size() == 0)
                                evMap[p].remove(index);
                        }

                        if (vertexCompare.compare(v, w) == -1) {
                            e = (long) v << 32 | w & 0xFFFFFFFFL;
                        } else {
                            e = (long) w << 32 | v & 0xFFFFFFFFL;
                        }
                        index = eIndexMap.get(e);
                        sup = eSup[index].decrementAndGet();
                        if (sup == minSup - 1) {
                            // add index to invalid indexes
                            localInvalids[p].add(index);
                        }
                        vList = evMap[p].get(index);
                        if (vList != null) {
                            vList.remove(u);
                            if (vList.size() == 0)
                                evMap[p].remove(index);
                        }
                    }
                }
            })).get();
            long tUpdate = System.currentTimeMillis();
            System.out.println("Update in " + (tUpdate - tInvalid) + " ms");
        }
    }
}
