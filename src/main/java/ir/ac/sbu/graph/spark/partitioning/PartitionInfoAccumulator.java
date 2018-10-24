package ir.ac.sbu.graph.spark.partitioning;

import org.apache.spark.util.AccumulatorV2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PartitionInfoAccumulator extends AccumulatorV2<Integer, Map<Integer,Long>> {

    private final Map<Integer, Long> _map;

    public PartitionInfoAccumulator() {
        this(new HashMap<>());
    }

    public PartitionInfoAccumulator(Map<Integer, Long> map) {
        this._map = Collections.synchronizedMap(map);
    }

    @Override
    public boolean isZero() {
        return _map.isEmpty();
    }

    @Override
    public AccumulatorV2<Integer, Map<Integer, Long>> copy() {
        return new PartitionInfoAccumulator(_map);
    }

    @Override
    public void reset() {
        _map.clear();
    }

    @Override
    public void add(Integer k) {
        add(k, 1L);
    }

    private void add(Integer k, Long v) {
        if (k < 0) {
            _map.compute(k, (key, value) -> value == null ? -1 : value - v);
        } else {
            _map.compute(k, (key, value) -> value == null ? 1 : value + v);
        }
    }

    @Override
    public void merge(AccumulatorV2<Integer, Map<Integer, Long>> other) {
        PartitionInfoAccumulator acc = (PartitionInfoAccumulator) other;
        acc._map.forEach((k, v) -> add(k, v));
    }

    @Override
    public synchronized Map<Integer, Long> value() {
        return _map;
    }

    public synchronized void addAll(Map<Integer, Long> map) {
        this._map.putAll(map);
    }
}
