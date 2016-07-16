package graph.ktruss.old;

import java.io.Serializable;

public class Triangle implements Serializable {
	private static final long serialVersionUID = 1L;
	
	long first;
	long second;
	long third;
	
	public Triangle() { }
	
	public Triangle(long u, long v, long w) {
		if (u < v) {
			if (v < w) {
				first = u;
				second = v;
				third = w;
			} else {
				third = v;
				if (u < w) {
					first = u;
					second = w;
				} else {
					first = w;
					second = u;
				}
			}
		} else { // u > v
			if (v > w) { // u > v > w
				first = w;
				second = v;
				third = u;
			} else { // u > v & v < w
				first = v;
				if (u < w) {
					second = u;
					third = w;
				} else {
					second = w;
					third = u;
				}
			}
		}
	}

	@Override
	public int hashCode() {
		return new Long(first).hashCode() ^ new Long(second).hashCode() ^ new Long(third).hashCode();
	}
	
	public boolean contains(Edge e) {
		int count = 0;
		if (e.v1 == first || e.v1 == second || e.v1 == third)
			count ++;
		if (e.v2 == first || e.v2 == second || e.v2 == third)
			count ++;
		return count == 2;
	}
	
	@Override
	public boolean equals(Object triangle) {
		if (triangle == null)
			return false;
		
		Triangle t = (Triangle) triangle;
		
		return first == t.first && second == t.second && third == t.third;
	}

	public boolean isEqualsTo(Triangle t) {
		return (first == t.first || first == t.second || first == t.third) &&
		(second == t.first || second == t.second || second == t.third) &&
		(third == t.first || third == t.second || third == t.third);
	}
	
	@Override
	public String toString() {
		return "<" + first + ", " + second + ", " + third + ">";
	}
}
