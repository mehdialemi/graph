package graph.ktruss.old;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;

public class FdValue implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public int degree;
	public long[] highDegs;
	public long[] lowDegs;
	
	public FdValue() { }
	
	public FdValue(int degree, long[] highDegs, long[] lowDegs) {
		this.degree = degree;
		this.highDegs = highDegs;
		this.lowDegs = lowDegs;
	}

	/**
	 * Remove expired neighbors from high and low neighbor arrays
	 * @param iterator an iterator containing vertexId of expired neighbors
	 */
	public void removeNeighbors(Iterator<Iterable<Long>> iterator) {
		TreeSet<Long> sortedSet = new TreeSet<>();
		while(iterator.hasNext()) {
			Iterable<Long> vertices = iterator.next();
			for (Long vertex : vertices) {
				sortedSet.add(vertex);
			}
		}
		
		highDegs = updateNeighbors(highDegs, sortedSet);
		lowDegs = updateNeighbors(lowDegs, sortedSet);
		degree = highDegs.length + lowDegs.length;
	}

	/**
	 * This function remove expired neighbors from an array of neighbors. Expired nodes are removed in place in neighbors array.
	 * @param neighbors all neighbors
	 * @param expiredNodes neighbors that should be removed
	 * @return
	 */
	private long[] updateNeighbors(long[] neighbors, TreeSet<Long> expiredNodes) {
		// If there is no expired neighbor, skip this function
		if (expiredNodes.size() == 0)
			return neighbors;

		// Store index of elements which are going to be removed from neighbors
		int index = 0;
		Queue<Integer> indexQueue = new ArrayBlockingQueue<Integer>(expiredNodes.size());
		for (long vertex : neighbors) {
			if (expiredNodes.remove(vertex))
				indexQueue.add(index);
			if (expiredNodes.size() == 0)
				break;
			index ++;
		}
		
		int removeLen = indexQueue.size();

		// There are two pointers: c, and i. VertexId in index i of the neighbors is assigned to index c of the neighbors.
		// Index i skip those vertices that is specified by indexQueue.
		// When an element in index i can be written to neighbors, it is assigned to index pointed by variable c.
		if (removeLen > 0) {
			int c = 0, i = 0;
			Integer removeIndex = indexQueue.remove();
			
			for(; i < neighbors.length && indexQueue.size() > 0;) {
				if (i == removeIndex) {
					if (indexQueue.size() > 0)
						removeIndex = indexQueue.remove();
					i ++;
					continue; // Skip to be removed element
				} 
				if (i >= neighbors.length || c >= neighbors.length)
					break;
				
				neighbors[c++] = neighbors[i++];
			}
		}

		long[] newHighDegs = new long[neighbors.length - removeLen]; 
		System.arraycopy(neighbors, 0, newHighDegs, 0, newHighDegs.length);
		return newHighDegs;
	}
	
	@Override
	public String toString() {
		String str = "[";
		str += "Degree: " + degree;
		str += ", HighDegs:";
		for (long highDeg : highDegs) {
			str += " " + highDeg;
		}
		str += ", LowDegs:";
		for (long lowDeg : lowDegs) {
			str += " " + lowDeg;
		}
		str += "]";
		return str;
	}
}
