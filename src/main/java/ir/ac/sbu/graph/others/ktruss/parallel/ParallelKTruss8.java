package ir.ac.sbu.graph.others.ktruss.parallel;

import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.utils.PartitioningUtils;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
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
public class ParallelKTruss8 extends ParallelKTrussBase {

    private ForkJoinPool forkJoinPool;

    public ParallelKTruss8(Edge[] edges, int minSup, int threads) {
        super(edges, minSup, threads);
        forkJoinPool = new ForkJoinPool(threads);
    }

    @Override
    public void start() throws Exception {
        long tStart = System.currentTimeMillis();

        Int2ObjectOpenHashMap<IntSet> neighbors = new Int2ObjectOpenHashMap<>();
        for (Edge e : edges) {
            IntSet set = neighbors.get(e.v1);
            if (set == null) {
                set = new IntOpenHashSet();
                neighbors.put(e.v1, set);
            }
            set.add(e.v2);

            set = neighbors.get(e.v2);
            if (set == null) {
                set = new IntOpenHashSet();
                neighbors.put(e.v2, set);
            }
            set.add(e.v1);
        }
        long tneighbor = System.currentTimeMillis();
        System.out.println("create neighbor list in " + (tneighbor - tStart) + " ms");

        Long2IntMap partitions = PartitioningUtils.createPartition(edges, neighbors, threads);

        batchSelector = new AtomicInteger(0);
        Long2ObjectOpenHashMap<IntSet>[] ev = new Long2ObjectOpenHashMap[threads];
        for (int i = 0; i < threads; i++)
            ev[i] = new Long2ObjectOpenHashMap<>(edges.length / threads);

        long tpartition = System.currentTimeMillis();
        System.out.println("partition in " + (tpartition - tneighbor) + " ms");
        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(t -> {
            for (int i = 0; i < edges.length; i++) {
                int v1 = edges[i].v1;
                int v2 = edges[i].v2;
                if (v2 < v1) {
                    v1 = edges[i].v2;
                    v2 = edges[i].v1;
                }

                long e = (long) v1 << 32 | v2 & 0xFFFFFFFFL;
                if (partitions.get(e) != t)
                    continue;

                IntSet set1 = neighbors.get(edges[i].v1);
                IntSet set2 = neighbors.get(edges[i].v2);

                IntSet common = null;
                if (set1.size() < set2.size()) {
                    IntIterator iter = set1.iterator();
                    while (iter.hasNext()) {
                        int v = iter.nextInt();
                        if (!set2.contains(v))
                            continue;
                        if (common == null)
                            common = new IntOpenHashSet();
                        common.add(v);
                    }
                } else {
                    IntIterator iter = set2.iterator();
                    while (iter.hasNext()) {
                        int v = iter.nextInt();
                        if (!set1.contains(v))
                            continue;
                        if (common == null)
                            common = new IntOpenHashSet();
                        common.add(v);
                    }
                }

                if (common == null)
                    continue;

                ev[t].put(e, common);
            }
        })).get();
        long tesup = System.currentTimeMillis();
        System.out.println("calculate esup " + (tesup - tpartition) + " ms");

        int sum = 0;
        for (Long2ObjectOpenHashMap<IntSet> x : ev) {
            sum += x.size();
        }

        long[] invalids = new long[sum];
        final LongArrayList[] localInvalids = new LongArrayList[threads];
        for (int i = 0; i < threads; i++) {
            localInvalids[i] = new LongArrayList(ev[i].size());
        }

        forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(t -> {
            ObjectIterator<Long2ObjectMap.Entry<IntSet>> iterator = ev[t].long2ObjectEntrySet().fastIterator();
            while (iterator.hasNext()) {
                Long2ObjectMap.Entry<IntSet> entry = iterator.next();
                if (entry.getValue().size() < minSup)
                    localInvalids[t].add(entry.getLongKey());
            }
        })).get();

        Long2ObjectMap<IntSet>[][] others = new Long2ObjectMap[threads][];
        for (int i = 0; i < threads; i++) {
            others[i] = new Long2ObjectMap[threads];
            for (int j = 0 ; j < threads; j ++) {
                others[i][j] = new Long2ObjectOpenHashMap<>();
            }
        }

        int iteration = 0;
        while (true) {
            long t1 = System.currentTimeMillis();
            List<Tuple2<Integer, LongArrayList>> invalidOffset = new ArrayList<>();
            int loffset = 0;
            invalidOffset.add(new Tuple2<>(loffset, localInvalids[0]));
            for (int i = 1; i < localInvalids.length; i++) {
                loffset += localInvalids[i - 1].size();
                invalidOffset.add(new Tuple2<>(loffset, localInvalids[i]));
            }
            final int invalidSize = forkJoinPool.submit(() -> invalidOffset.parallelStream().map(packet -> {
                int offset = packet._1;
                for (int i = 0; i < packet._2.size(); i++) {
                    invalids[i + offset] = packet._2.getLong(i);
                }
                return packet._2.size();
            })).get().reduce((a, b) -> a + b).get();
            long tInvalid = System.currentTimeMillis();
            System.out.println("iteration " + ++iteration + ", invalid size: " + invalidSize + " time: " + (tInvalid - t1) + " ms");

            if (invalidSize == 0)
                break;

            for (int i = 0; i < threads; i++) {
                localInvalids[i].clear();
                for(int j = 0 ; j < threads; j ++)
                    others[i][j].clear();
            }

            forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(t -> {
                for (int i = 0; i < invalidSize; i++) {
                    long edge = invalids[i];
                    IntSet set = ev[t].remove(edge);
                    if (set == null)
                        continue;
                    IntIterator iterator = set.iterator();
                    int u = (int) (edge >> 32);
                    int v = (int) edge;

                    long e;
                    while (iterator.hasNext()) {
                        int w = iterator.nextInt();
                        if (u < w) {
                            e = (long) u << 32 | w & 0xFFFFFFFFL;
                        } else {
                            e = (long) w << 32 | u & 0xFFFFFFFFL;
                        }

                        int p = partitions.get(e);
                        if (p != t) {
                            IntSet iset = others[t][p].get(e);
                            if (iset == null) {
                                iset = new IntOpenHashSet();
                                others[t][p].put(e, iset);
                            }
                            iset.add(v);
                        } else {
                            IntSet iset = ev[t].get(e);
                            if (iset != null) {
                                iset.remove(v);
                                if (iset.size() < minSup) {
                                    localInvalids[t].add(e);
                                }
                            }
                        }

                        if (v < w) {
                            e = (long) v << 32 | w & 0xFFFFFFFFL;
                        } else {
                            e = (long) w << 32 | v & 0xFFFFFFFFL;
                        }

                        p = partitions.get(e);
                        if (p != t) {
                            IntSet iset = others[t][p].get(e);
                            if (iset == null) {
                                iset = new IntOpenHashSet();
                                others[t][p].put(e, iset);
                            }
                            iset.add(u);
                        } else {
                            IntSet iset = ev[t].get(e);
                            if (iset != null) {
                                iset.remove(u);
                                if (iset.size() < minSup) {
                                    localInvalids[t].add(e);
                                }
                            }
                        }
                    }
                }
            })).get();

            forkJoinPool.submit(() -> IntStream.range(0, threads).parallel().forEach(t -> {
                for(int i = 0 ; i < threads; i ++) {
                    if (i == t)
                        continue;
                    ObjectIterator<Long2ObjectMap.Entry<IntSet>> iterator = others[i][t].long2ObjectEntrySet().iterator();
                    while (iterator.hasNext()) {
                        Long2ObjectMap.Entry<IntSet> entry = iterator.next();
                        IntSet set = ev[t].get(entry.getLongKey());
                        if (set == null) continue;
                        IntIterator viter = entry.getValue().iterator();
                        while (viter.hasNext()) {
                            set.remove(viter.nextInt());
                        }
                        if (set.size() < minSup)
                            localInvalids[t].add(entry.getLongKey());
                    }
                }
            })).get();
        }

        int size = 0;
        for(int i = 0 ; i < threads; i ++) {
            ObjectIterator<Long2ObjectMap.Entry<IntSet>> iterator = ev[i].long2ObjectEntrySet().fastIterator();
            while (iterator.hasNext()) {
                size += iterator.next().getValue().size();
            }
        }
        System.out.println("size: " + size);
    }
}
