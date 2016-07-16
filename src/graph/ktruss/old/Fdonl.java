package graph.ktruss.old;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * A class that receive neighborList as key-value: long, long[]
 * Input: 
 * 	Key => vertexId
 * 	Value => vertexIds of neighbors
 * 
 * It is aimed at creating a usable data structure containing key-values
 * Output:
 * 	Key => vertexId
 * 	Value => 1: degree: degree of key vertex
 * 			 2: high vertices: degree based sorted neighbors which 
 * 					have higher degree than the degree of the key vertex
 * 			 3: low vertices: neighbors which have lower degree than the degree of the key vertex 
 * 	For two vertices which have the same degree, that one with a larger Id 
 * 		is considered as a higher degree vertex
 */
public class Fdonl implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private JavaPairRDD<Long, Iterable<Long>> neighborList;

	public Fdonl(JavaPairRDD<Long, Iterable<Long>> neighborList) {
		this.neighborList = neighborList;
	}

	@SuppressWarnings("unused")
	public JavaPairRDD<Long, FdValue> createFdonl() {
		
		// Create a neighbor List that each vertex in the neighbor list has its degree information beside itself
		// vdNeighborList abbreviates Vertex Degree Neighbor List
		JavaPairRDD<Long, Iterable<VertexDegree>> vdNeighborList = neighborList
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long, VertexDegree>() {
			@Override
			public Iterable<Tuple2<Long, VertexDegree>> call(Tuple2<Long, Iterable<Long>> t) throws
				Exception {
				HashSet<Long> neighborSet = new HashSet<Long>();
				for (Long neighbor : t._2) {
					neighborSet.add(neighbor);
				}

				int degree = neighborSet.size();

				VertexDegree vd = new VertexDegree(t._1, degree);

				List<Tuple2<Long, VertexDegree>> degreeList = new ArrayList<>(degree);

				// Add degree information of the current vertex to its neighbor
				for (Long neighbor : neighborSet) {
					degreeList.add(new Tuple2<>(neighbor, vd));
				}
				return degreeList;
			}
		}).groupByKey(); // A neighbor list that each neighbor has its degree
		
		// Create a degree based ordered structure
        JavaPairRDD<Long, FdValue> fdonl = vdNeighborList.mapToPair(new PairFunction<Tuple2<Long, Iterable<VertexDegree>>, Long, FdValue>() {
            @Override
            public Tuple2<Long, FdValue> call(Tuple2<Long, Iterable<VertexDegree>> v) throws Exception {

                int degree = 0;
                // Iterate over neighbors to calculate degree of the current vertex
                for (VertexDegree vd : v._2) {
                    degree ++;
                }

                List<VertexDegree> highDegvdNeighborList = new ArrayList<>(degree / 2);
                List<Long> lowDegNeighborList = new ArrayList<>(degree / 2);

                // Identify highDegree and lowDegree neighbors
                for (VertexDegree vd : v._2) {
                    if (vd.degree > degree)
                        highDegvdNeighborList.add(vd);
                    else if (vd.degree < degree)
                        lowDegNeighborList.add(vd.vId);
                        // If degree of neighbor is the same as key vertex then consider vertex Id
                    else if (vd.vId > v._1)
                        highDegvdNeighborList.add(vd);
                    else
                        lowDegNeighborList.add(vd.vId);
                }

                // We only require sort neighbors with higher degree
                Collections.sort(highDegvdNeighborList, new Comparator<VertexDegree>() {

                    @Override
                    public int compare(VertexDegree vd1, VertexDegree vd2) {
                        if (vd1.degree != vd2.degree)
                            return vd1.degree - vd2.degree;
                        return (int) (vd1.vId - vd2.vId);
                    }
                });

                // Create FdValue per vertex
                long[] highDegArray = new long[highDegvdNeighborList.size()];
                long[] lowDegArray = new long[lowDegNeighborList.size()];

                int i = 0;
                for (VertexDegree vd : highDegvdNeighborList) {
                    highDegArray[i++] = vd.vId;
                }

                i = 0;
                for (long vertex : lowDegNeighborList)
                    lowDegArray[i++] = vertex;

                FdValue fvalue = new FdValue(degree, highDegArray, lowDegArray);
                return new Tuple2<>(v._1, fvalue);
            }
        });
        return fdonl;
	}
	
	public static class VertexDegree implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private long vId;
		private int degree;

		public VertexDegree() {
		}
		
		public VertexDegree(long vId, int degree) {
			this.vId = vId;
			this.degree = degree;
		}
	}
}
