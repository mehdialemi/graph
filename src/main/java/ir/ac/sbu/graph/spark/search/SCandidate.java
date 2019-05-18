package ir.ac.sbu.graph.spark.search;

import java.util.BitSet;

public class SCandidate {
    private final BitSet bitSet;
    private final int size;

    public SCandidate(int size) {
        this.bitSet = new BitSet(size);
        this.size = size;
    }

    public void set(int index) {
        bitSet.set(index);
    }

    public boolean isFull() {
       return bitSet.cardinality() == size;
    }

    public int[] emptyIndex() {
        int[] indexes = new int[size - bitSet.cardinality()];
        int index = 0;
        for (int i = 0; i < size; i++) {
            if (!bitSet.get(i))
                indexes[index ++] = i;
        }
        return indexes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("C[");
        for (int i = 0; i < size; i++) {
            if (bitSet.get(i)) {
                sb.append(i).append(" ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public static void main(String[] args) {
        SCandidate sCandidate = new SCandidate(10);

        for (int i = 0; i < 10; i++) {
            sCandidate.set(i);
            System.out.println("emptyIndex Size: " + sCandidate.emptyIndex().length);

        }
    }
}
