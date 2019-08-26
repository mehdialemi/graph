package ir.ac.sbu.graph.spark.pattern.index;

import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleFonlValue;
import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleMeta;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IndexRow implements Serializable {

    private int vertex;
    private String label;
    private String[] labels;
    public int degree;
    private int[] degrees;
    public int[] fonl;
    private long[] triangleEdges;
    private int tc;
    private int[] vTc;

    public IndexRow() { }

    public static StructType structType() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("vertex", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("fonl", DataTypes.createArrayType(DataTypes.IntegerType), true));
        fields.add(DataTypes.createStructField("label", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("degree", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("labels", DataTypes.createArrayType(DataTypes.StringType), true));
        fields.add(DataTypes.createStructField("degrees", DataTypes.createArrayType(DataTypes.IntegerType), true));
        fields.add(DataTypes.createStructField("triangleEdges", DataTypes.createArrayType(DataTypes.LongType), true));
        fields.add(DataTypes.createStructField("tc", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("vTc", DataTypes.createArrayType(DataTypes.IntegerType), true));

        return DataTypes.createStructType(fields);
    }

    public static Row createRow(int v, int[] fonl, LabelDegreeTriangleMeta meta) {
        return RowFactory.create(v, fonl, meta.getLabel(), meta.getDegree(), meta.getLabels(), meta.getDegrees(),
                meta.getTriangleEdges(), meta.tc(), meta.getvTc());
    }

    private static int[] intArray(WrappedArray<Integer> w) {
        final IntList list = new IntArrayList();
        scala.collection.immutable.List<Integer> scalaList = w.toList();
        for (int i = 0; i < scalaList.size(); i++) {
            list.add(scalaList.apply(i));
        }

        return list.toIntArray();
    }

    private static long[] longArray(WrappedArray<Long> w) {
        final LongList list = new LongArrayList();
        scala.collection.immutable.List<Long> scalaList = w.toList();
        for (int i = 0; i < scalaList.size(); i++) {
            list.add(scalaList.apply(i));
        }

        return list.toLongArray();
    }

    private static String[] stringArray(WrappedArray<String> w) {
        final List<String> list = new ArrayList<>();
        scala.collection.immutable.List<String> scalaList = w.toList();
        for (int i = 0; i < scalaList.size(); i++) {
            list.add(scalaList.apply(i));
        }

        return list.toArray(new String[0]);
    }

    public static Tuple2<Integer, LabelDegreeTriangleFonlValue> createTuple2(Row row) {
        int vertex = row.getInt(0);
        int[] fonl = intArray(row.getAs(1));
        LabelDegreeTriangleMeta meta = new LabelDegreeTriangleMeta();
        LabelDegreeTriangleFonlValue value = new LabelDegreeTriangleFonlValue(fonl, meta);
        value.meta.setLabel(row.getString(2));
        value.meta.setDegree(row.getInt(3));
        value.meta.setLabels(stringArray(row.getAs(4)));
        value.meta.setDegrees(intArray(row.getAs(5)));
        value.meta.setTriangleEdges(longArray(row.getAs(6)));
        value.meta.setTc(row.getInt(7));
        value.meta.setvTc(intArray(row.getAs(8)));
//        int vertex = row.getAs("vertex");
//        int[] fonl = row.getAs("fonl");
//        LabelDegreeTriangleMeta meta = new LabelDegreeTriangleMeta();
//        LabelDegreeTriangleFonlValue value = new LabelDegreeTriangleFonlValue(fonl, meta);
//        value.meta.setLabel(row.getAs("label"));
//        value.meta.setDegree(row.getAs("degree"));
//        value.meta.setLabels(row.getAs("labels"));
//        value.meta.setDegrees(row.getAs("degrees"));
//        value.meta.setTriangleEdges(row.getAs("triangleEdges"));
//        value.meta.setTc(row.getAs("tc"));
//        value.meta.setvTc(row.getAs("vTc"));
        return new Tuple2<>(vertex, value);
    }


    public IndexRow(int vertex, String label, String[] labels, int degree, int[] degrees, int[] fonl,
                    long[] triangleEdges, int tc, int[] vTc) {
        this.vertex = vertex;
        this.label = label;
        this.labels = labels;
        this.degrees = degrees;
        this.fonl = fonl;
        this.triangleEdges = triangleEdges;
        this.vTc = vTc;
        this.tc = tc;
        this.degree = degree;
    }

    public int getVertex() {
        return vertex;
    }

    public void setVertex(int vertex) {
        this.vertex = vertex;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String[] getLabels() {
        return labels;
    }

    public void setLabels(String[] labels) {
        this.labels = labels;
    }

    public int[] getDegrees() {
        return degrees;
    }

    public void setDegrees(int[] degrees) {
        this.degrees = degrees;
    }

    public int[] getFonl() {
        return fonl;
    }

    public void setFonl(int[] fonl) {
        this.fonl = fonl;
    }

    public long[] getTriangleEdges() {
        return triangleEdges;
    }

    public void setTriangleEdges(long[] triangleEdges) {
        this.triangleEdges = triangleEdges;
    }

    public int[] getvTc() {
        return vTc;
    }

    public void setvTc(int[] vTc) {
        this.vTc = vTc;
    }

    public int getTc() {
        return tc;
    }

    public void setTc(int tc) {
        this.tc = tc;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }
}
