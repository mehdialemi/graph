package ir.ac.sbu.graph.utils;

/**
 *
 */
public class MyTuple {

    public int t1;
    public int t2;

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return super.equals(obj);
        MyTuple t = (MyTuple) obj;
        return t1 == t.t1;
    }

    @Override
    public int hashCode() {
        return t1;
    }
}
