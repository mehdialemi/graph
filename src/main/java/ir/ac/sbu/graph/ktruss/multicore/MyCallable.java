package ir.ac.sbu.graph.ktruss.multicore;

import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 */
public abstract class MyCallable<T, V> implements Callable<V>, Iterable<T>{

    private final Tuple2<Integer, Integer> bucket;
    private int current;
    private T[] array;

    public MyCallable(int index, List<Tuple2<Integer, Integer>> buckets, T[] array) {
        this.array = array;
        bucket = buckets.get(index);
        current = bucket._1;
    }

    @Override
    public Iterator<T> iterator() {
        return null;
    }

    class MyIterator implements Iterator<T> {

        @Override
        public boolean hasNext() {
            if (current < bucket._2)
                return true;
            return false;
        }

        @Override
        public T next() {
            return array[current++];
        }
    }

}
