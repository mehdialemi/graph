package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.fonl.LabelMeta;
import ir.ac.sbu.graph.fonl.Sonl;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class Candidate implements Serializable {
    private int src;
    public final int[] vArray;
    public final int[] orders;

    public int checkCount = 0;
    private int setCount = 0;
    public int checkVertex = 0;
    boolean subSet = false;
    boolean duplicate = false;

    public Candidate(int src, int size) {
        this.src = src;
        vArray = new int[size];
        orders = new int[size];
    }

    public Candidate(Candidate candidate) {
        this.src = candidate.src;
        vArray = new int[candidate.vArray.length];
        System.arraycopy(candidate.vArray, 0, vArray, 0, vArray.length);
        orders = new int[candidate.vArray.length];
        System.arraycopy(candidate.orders, 0, orders, 0, orders.length);
        setCount = candidate.setCount;
    }

    public void resetMeta() {
        subSet = false;
        duplicate = false;
//        checkIndexes = null;
    }

//    public int[] getCheckIndexes() {
//        return checkIndexes;
//    }

    public boolean isSubSet() {
        return subSet;
    }

    public boolean isDuplicate() {
        return duplicate;
    }

    public Map <Candidate, List <Tuple2 <Integer, Integer>>> complement(Candidate other, Sonl sonl) {
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
        IntSet o1 = null;
        IntSet o2 = null;
        Map<Candidate, List<Tuple2<Integer, Integer>>> map = new HashMap <>();
        TreeSet<Tuple2<Integer, Integer>> tree = new TreeSet <>(Comparator.comparingInt(ts -> ts._1));

        for (int i = 0; i < vArray.length; i++) {

            if (vArray[i] != other.vArray[i]) {
                if (vArray[i] != 0) {

                    if (c2 == null)
                        c2 = new Candidate(other);

                    tree.clear();
                    for (int idx : sonl.isnArray[i]) {
                        if (c2.vArray[idx] != 0) {
//                            if (o2 == null)
//                                o2 = new IntOpenHashSet();
//                            o2.add(c2.orders[idx]);
                            tree.add(new Tuple2 <>(c2.orders[idx], c2.vArray[idx]));
                        }
                    }

                    int v = -1;
                    int w = -1;
                    if (tree.size() == 1) {
                        v = tree.first()._2;
                        map.computeIfAbsent(c2, candidate -> new ArrayList <>()).add(new Tuple2 <>(v, w));
                    } else if (tree.size() > 1) {
                        for (Tuple2 <Integer, Integer> t : tree) {
                            w = t._2;
                            if (v != -1) {
                                map.computeIfAbsent(c2, candidate -> new ArrayList <>()).add(new Tuple2 <>(v, w));
                            }
                            v = w;
                        }
                    }

                    c2.set(i, vArray[i]);

                } else {

                    if (c1 == null)
                        c1 = new Candidate(this);

                    tree.clear();
                    for (int idx : sonl.isnArray[i]) {
                        if (c1.vArray[idx] != 0) {
//                            if (o1 == null)
//                                o1 = new IntOpenHashSet();
//                            o1.add(c1.orders[idx]);
                            tree.add(new Tuple2 <>(c1.orders[idx], c1.vArray[idx]));
                        }
                    }

                    int v = -1;
                    int w = -1;
                    if (tree.size() == 1) {
                        v = tree.first()._2;
                        map.computeIfAbsent(c1, candidate -> new ArrayList <>()).add(new Tuple2 <>(v, w));
                    } else if (tree.size() > 1) {
                        for (Tuple2 <Integer, Integer> t : tree) {
                            w = t._2;
                            if (v != -1) {
                                map.computeIfAbsent(c1, candidate -> new ArrayList <>()).add(new Tuple2 <>(v, w));                            }
                            v = w;
                        }
                    }

                    c1.set(i, other.vArray[i]);
                }
            }
        }

//        if (o1 != null) {
//            IntSet set = new IntOpenHashSet(o1.size());
//            for (int order : o1) {
//                set.add(c1.vIndex[order]);
//            }
//            c1.checkIndexes = o1.toIntArray();
//        }
//        if (o2 != null) {
//            IntSet set = new IntOpenHashSet(o2.size());
//            for (int order : o2) {
//                set.add(c2.vIndex[order]);
//            }
//            c2.checkIndexes = o2.toIntArray();
//        }
//
//        List <Candidate> list = new ArrayList <>(1);
//        if (c1 !=  null)
//            list.add(c1);
//        if (c2 != null)
//            list.add(c2);
        return map;
    }

//    public boolean neighbor(Fvalue <LabelMeta> fvalue) {
////        if (checkIndexes == null)
////            return true;
//
//        for (int index : checkIndexes) {
//            boolean check = false;
//            for (int fonl : fvalue.fonl) {
//                check = vIndex[index] == fonl;
//                if (check)
//                    break;
//            }
//            if (!check)
//                return false;
//        }
//        return true;
//    }

    public void set(int index, int vertex) {
        if (vertex == 0)
            return;
        if (vArray[index] == 0) {
            orders[index] = setCount;
            vArray[index] = vertex;
            setCount ++;
        }
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
//                "CheckIndexes: {" + Arrays.toString(checkIndexes) + "}, " +
                "Indexes: {" + Arrays.toString(vArray) + "}" +
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
