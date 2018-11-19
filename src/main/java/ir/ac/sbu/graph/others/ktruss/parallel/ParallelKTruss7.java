package ir.ac.sbu.graph.others.ktruss.parallel;

import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.utils.PartitioningUtils;
import ir.ac.sbu.graph.utils.VertexCompare;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import scala.Tuple2;

import java.util.ArrayList;
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
        final Int2ObjectOpenHashMap<IntArrayList> fonl = new Int2ObjectOpenHashMap<>(vCount);
        for (Edge e : edges) {
            if (d[e.v1] < 2 || d[e.v2] < 2)
                continue;
            int v1 = e.v1, v2 = e.v2;
            if (vertexCompare.compare(e.v1, e.v2) == 1) {
                v1 = e.v2;
                v2 = e.v1;
            }
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

        Long2ObjectOpenHashMap<IntSet>[] evMap = new Long2ObjectOpenHashMap[threads];
        for (int i = 0; i < threads; i++) {
            evMap[i] = new Long2ObjectOpenHashMap(vCount);
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
                    int ui = i + 1, vi = 0;
                    int c = 0;
                    while (ui < fu.size() && vi < fv.size()) {
                        if (fu.getInt(ui) == fv.getInt(vi)) {
                            int w = fu.getInt(ui);
                            long uw = (long) u << 32 | w & 0xFFFFFFFFL;
                            long vw = (long) v << 32 | w & 0xFFFFFFFFL;
                            IntSet set = evMap[p].get(uv);
                            if (set == null) {
                                set = new IntOpenHashSet(); // TODO set support
                                evMap[p].put(uv, set);
                            }
                            set.add(w);

                            set = evMap[p].get(uw);
                            if (set == null) {
                                set = new IntOpenHashSet(); // TODO set support
                                evMap[p].put(uw, set);
                            }
                            set.add(v);

                            set = evMap[p].get(vw);
                            if (set == null) {
                                set = new IntOpenHashSet(); // TODO set support
                                evMap[p].put(vw, set);
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
                }
            }
        })).get();
        long ttc = System.currentTimeMillis();
        System.out.println("find triangles in " + (ttc - tpartition) + " ms");

        int sum = 0;
        for (Long2ObjectOpenHashMap<IntSet> map : evMap) {
            sum += map.size();
        }
        int avg = sum / threads;

        Long2IntMap ei = new Long2IntOpenHashMap(avg * 2);
        Int2LongMap ie = new Int2LongOpenHashMap(avg * 2);
        int count = 0;
        for (int i = 0 ; i < threads; i ++) {
            for (Long e : evMap[i].keySet()) {
                Integer prevVal = ei.putIfAbsent(e, count);
                if (prevVal == null) {
                    ie.put(count, e.longValue());
                    count++;
                }
            }
        }

        AtomicInteger[] eSup = new AtomicInteger[count];
        for(int i = 0 ; i < count ; i ++)
            eSup[i] = new AtomicInteger(0);

        forkJoinPool.submit(() -> IntStream.range(0, threads).forEach(t -> {
            ObjectIterator<Long2ObjectMap.Entry<IntSet>> iterator = evMap[t].long2ObjectEntrySet().fastIterator();
            while (iterator.hasNext()) {
                Long2ObjectMap.Entry<IntSet> entry = iterator.next();
                eSup[ei.get(entry.getLongKey())].addAndGet(entry.getValue().size());
            }
        })).get();

        long tcount = System.currentTimeMillis();
        System.out.println("tcount: " + count + " in " + (tcount - ttc) + " ms");

        int[] invalids = new int[count];
        final IntArrayList[] localInvalids = new IntArrayList[threads];
        for (int i = 0; i < threads; i++)
            localInvalids[i] = new IntArrayList(count / threads);

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
            System.out.println("iteration " + ++iteration + ", invalid support: " + invalidSize + " time: " + (tInvalid - t1) + " ms");
            if (invalidSize == 0)
                break;

            for (int i = 0; i < localInvalids.length; i++)
                localInvalids[i].clear();

            forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(p -> {
                for (int i = 0; i < invalidSize; i++) {
                    int eIndex = invalids[i];
                    long edge = ie.get(eIndex);
                    IntSet set = evMap[p].get(edge);
                    if (set == null)
                        continue;
                    IntIterator iterator = set.iterator();

                    int u = (int) (edge >> 32);
                    int v = (int) edge;
                    while (iterator.hasNext()) {
                        int w = iterator.nextInt();
                        long e;
                        if (vertexCompare.compare(u, w) == -1) {
                            e = (long) u << 32 | w & 0xFFFFFFFFL;
                        } else {
                            e = (long) w << 32 | u & 0xFFFFFFFFL;
                        }

                        int index = ei.get(e);
                        int sup = eSup[index].decrementAndGet();
                        if (sup == minSup - 1) {
                            // add index to invalid indexes
                            localInvalids[p].add(index);
                        }
                        IntSet vList = evMap[p].get(e);
                        if (vList != null) {
                            vList.remove(v);
                            if (vList.size() == 0)
                                evMap[p].remove(e);
                        }

                        if (vertexCompare.compare(v, w) == -1) {
                            e = (long) v << 32 | w & 0xFFFFFFFFL;
                        } else {
                            e = (long) w << 32 | v & 0xFFFFFFFFL;
                        }
                        index = ei.get(e);
                        sup = eSup[index].decrementAndGet();
                        if (sup == minSup - 1) {
                            // add index to invalid indexes
                            localInvalids[p].add(index);
                        }
                        vList = evMap[p].get(e);
                        if (vList != null) {
                            vList.remove(u);
                            if (vList.size() == 0)
                                evMap[p].remove(e);
                        }
                    }
                }
            })).get();
            long tUpdate = System.currentTimeMillis();
            System.out.println("Update in " + (tUpdate - tInvalid) + " ms");
        }
    }
}
