package ir.ac.sbu.graph.ktruss.old;

import java.io.Serializable;

public class Edge implements Serializable {
	private static final long serialVersionUID = 1L;

	long v1;
	long v2;
	long v3;
	
	public Edge() { }
	
	public Edge(long v1, long v2, long v3) {
		if (v1 < v2) {
			this.v1 = v1;
			this.v2 = v2;
		} else {
			this.v2 = v1;
			this.v1 = v2;
		}
		this.v3 = v3;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		
		Edge e = (Edge) obj;
		boolean equal = (v1 == e.v1 && v2 == e.v2);
//		System.out.println("Comparing " + this + " and " + e + ", result: " + equal);
		return equal;
	}
	
	@Override
	public int hashCode() {
		return (int) (v1 + v2);
	}

	public Triangle createTriangle() {
		if (v3 < v1)
			return new Triangle(v3, v1, v2);
		else if (v3 > v2)
			return new Triangle(v1, v2, v3);
		else
			return new Triangle(v1, v3, v2);
	}
	
	@Override
	public String toString() {
		return "(" + v1 + " , " + v2 + ")(" + v3 + ")";
	}
}
