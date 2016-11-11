package ir.ac.sbu.graph.ktruss.sequential;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

/**
 * Created by mehdi on 11/11/16.
 */
public class test {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Map<Integer, Integer> map = new HashMap<>();
        for(int i = 0 ; i < 100 ; i ++)
            map.put(i, i);

        System.setProperty("java.util.concurrent.ForkJoinPool.common‌​.parallelism", "10");
        ForkJoinPool forkJoinPool = new ForkJoinPool(2);
//        Spliterator<Map.Entry<Integer, Integer>> split = map.entrySet().spliterator().trySplit();
//        System.out.println("characteristics: " + split.characteristics());
//        System.out.println("size: " + split.estimateSize());
        Map<Long, List<Integer>> threadKeys = new HashMap<>();
        forkJoinPool.submit( () ->
        map.entrySet().parallelStream().forEach(entry -> {
            long id = Thread.currentThread().getId();
            List<Integer> keys = threadKeys.get(id);
            if (keys == null) {
                keys = new ArrayList<>();
                threadKeys.put(id, keys);
            }
            keys.add(entry.getKey());
        })).get();

        threadKeys.entrySet().forEach(entry -> {
            System.out.println("thread: " + entry.getKey() + ", keys: " + entry.getValue());
        });


    }
}
