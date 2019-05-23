package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.fonl.LabelMeta;
import ir.ac.sbu.graph.fonl.SONL;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Candidate implements Serializable {
    private int src;
    public final int[] vArray;
    private int setCount = 0;
    public int[] checkIndexes;
    boolean subSet = false;
    boolean duplicate = false;

    public Candidate(int src, int size) {
        this.src = src;
        vArray = new int[size];
    }

    public Candidate(Candidate candidate) {
        this.src = candidate.src;
        vArray = new int[candidate.vArray.length];
        System.arraycopy(candidate.vArray, 0, vArray, 0, vArray.length);
        setCount = candidate.setCount;
    }

    public void resetMeta() {
        subSet = false;
        duplicate = false;
        checkIndexes = null;
    }

    public int[] getCheckIndexes() {
        return checkIndexes;
    }

    public boolean isSubSet() {
        return subSet;
    }

    public boolean isDuplicate() {
        return duplicate;
    }

    public List <Candidate> complement(Candidate other, SONL sonl) {
        if (subSet || duplicate)
            return null;

        boolean complement = true;

        int cThis = 0;
        int cOther = 0;
        for (int i = 0; i < vArray.length; i++) {
            if (this.vArray[i] != other.vArray[i]) {
                if (this.vArray[i] == 0) {
                    cOther++;
                } else if (other.vArray[i] == 0) {
                    cThis++;
                } else {
                    complement = false;
                    break;
                }
            }
        }

        if (!complement)
            return null;

        if (cThis == cOther && cThis == 0) {
            other.duplicate = true;
            return null;
        }

        if (cThis > 0 && cOther == 0) {
            other.subSet = true;
            return null;

        } else if (cOther > 0 && cThis == 0) {
            this.subSet = true;
            return null;
        }

        Candidate c1 = null;
        Candidate c2 = null;
        IntList c1Check = null;
        IntList c2Check = null;
        for (int i = 0; i < vArray.length; i++) {
            if (vArray[i] != other.vArray[i]) {
                if (vArray[i] != 0) {

                    if (c2 == null)
                        c2 = new Candidate(other);

                    int checkIdx = sonl.neighborIndex(i, other.vArray);
                    if (checkIdx >= 0) {
                        if (c2Check == null)
                            c2Check = new IntArrayList();
                        c2Check.add(checkIdx);
                    }
                    c2.set(i, vArray[i]);

                } else {

                    if (c1 == null)
                        c1 = new Candidate(this);
                    int checkIdx = sonl.neighborIndex(i, vArray);
                    if (checkIdx >= 0) {
                        if (c1Check == null)
                            c1Check = new IntArrayList();
                        c1Check.add(checkIdx);
                    }
                    c1.set(i, other.vArray[i]);
                }
            }
        }

        if (c1Check != null)
            c1.checkIndexes = c1Check.toIntArray();
        if (c2Check != null)
            c2.checkIndexes = c2Check.toIntArray();

        List <Candidate> list = new ArrayList <>(1);
        if (c1 != null)
            list.add(c1);
        if (c2 != null)
            list.add(c2);
        return list;
    }

    public boolean neighbor(Fvalue<LabelMeta> fvalue) {
        if (checkIndexes == null)
            return true;

        for (int index : checkIndexes) {
            boolean check = false;
            for (int fonl : fvalue.fonl) {
                check = vArray[index] == fonl;
                if (check)
                    break;
            }
            if (!check)
                return false;
        }
        return true;
    }

    public boolean neighbor(SONL sonl, Fvalue<LabelMeta> fvalue) {
        if (checkIndexes == null)
            return true;

        for (int index : checkIndexes) {
            boolean check = false;
            for (int fonl : fvalue.fonl) {
                check = vArray[index] == fonl;
                if (check)
                    break;
            }
            if (!check)
                return false;
        }
        return true;
    }


    public void set(int index, int vertex) {
        if (vArray[index] == 0 && vertex != 0)
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
    public boolean equals(Object obj) {
        if (obj == null)
            return super.equals(obj);

        Candidate c = (Candidate) obj;

        if (setCount != c.setCount)
            return false;

        for (int i = 0; i < vArray.length; i++) {
            if (vArray[i] != c.vArray[i])
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (Integer i : vArray) {
            hash += i.hashCode();
        }
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("C[ " +
                "Src: " + src + ", " +
                "CheckIndexes: {" + Arrays.toString(checkIndexes) + "}, " +
                "Indexes: {" + Arrays.toString(vArray) +"}" +
                "]");
        return sb.toString();
    }

    public void setSrc(int src) {
        this.src = src;
    }

    public int getSrc() {
        return src;
    }

    public boolean canExtend(int[] nIdx, Fvalue <LabelMeta> fvalue) {

        for (int idx : nIdx) {
            if (vArray[idx] == 0)
                continue;
            boolean found = false;
            for (int v : fvalue.fonl) {
                if (vArray[idx] == v) {
                    found = true;
                    break;
                }
            }

            if (!found)
                return false;
        }

        return true;
    }
}
