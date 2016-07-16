package graph.ktruss.old;

import java.io.Serializable;

public class SimpleEdge implements Serializable {
	private static final long serialVersionUID = 1L;

	public long v1;
	public long v2;
	
	public SimpleEdge() { }
	
	public SimpleEdge(long v1, long v2) {
		this.v1 = v1;
		this.v2 = v2;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		SimpleEdge e = (SimpleEdge) obj;
		return (v1 == e.v1 && v2 == e.v2) || (v1 == e.v2 && v2 == e.v1);
	}
	
	@Override
	public int hashCode() {
		return (int) (v1 + v2);
	}
	
	public SimpleEdge reverse() {
		return new SimpleEdge(v2, v1);
	}
	
	@Override
	public String toString() {
		return "(" + v1 + "," + v2 + ")";
	}
}
