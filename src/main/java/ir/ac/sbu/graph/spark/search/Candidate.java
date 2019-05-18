package ir.ac.sbu.graph.spark.search;

public class Candidate {
    private final int[] vArray;
    private int setCount = 0;

    public Candidate(int size) {
        vArray = new int[size];
    }

    public Candidate(Candidate candidate) {
        vArray = new int[candidate.vArray.length];
        System.arraycopy(candidate.vArray, 0, vArray, 0, vArray.length);
        setCount = candidate.setCount;
    }


    public void set(int index, int vertex) {
        if (vArray[index] == 0)
            setCount++;
        vArray[index] = vertex;
    }

    public int index(int vertex) {
        for (int i = 0; i < vArray.length; i++) {
            if (vArray[i] == vertex)
                return i;
        }
        return -1;
    }

    public boolean isNotEmpty(int index) {
        return vArray[index] != 0;
    }

    public boolean isFull() {
        return setCount == vArray.length;
    }

    public boolean oneEmptyRemain() {
        return setCount == vArray.length - 1;
    }

    public int[] emptyIndex() {
        int[] indexes = new int[vArray.length - setCount];
        int index = 0;
        for (int i = 0; i < vArray.length; i++) {
            if (vArray[i] != 0)
                indexes[index++] = i;
        }
        return indexes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("C[");
        for (int i = 0; i < vArray.length; i++) {
            sb.append(vArray[i]).append(" , ");

        }
        sb.append("]");
        return sb.toString();
    }
}
