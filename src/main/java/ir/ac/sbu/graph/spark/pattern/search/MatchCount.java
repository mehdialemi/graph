package ir.ac.sbu.graph.spark.pattern.search;

public class MatchCount {

    private int vertex;
    private int count;
    private int linkIndex;

    public MatchCount() { }

    public MatchCount(int vertex, int count, int linkIndex) {

        this.vertex = vertex;
        this.count = count;
        this.linkIndex = linkIndex;
    }

    public boolean equalLink(int linkIndex) {
        return this.linkIndex == linkIndex;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(vertex);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        MatchCount mc = (MatchCount) obj;
        return vertex == mc.vertex && count == mc.count;
    }

    public int getVertex() {
        return vertex;
    }

    public void setVertex(int vertex) {
        this.vertex = vertex;
    }

    public long getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getLinkIndex() {
        return linkIndex;
    }

    public void setLinkIndex(int linkIndex) {
        this.linkIndex = linkIndex;
    }
}
