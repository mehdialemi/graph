package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.SparkAppConf;

public class SGMatcherConf extends SparkAppConf {

    protected String lablePath;

    public SGMatcherConf(ArgumentReader reader) {
        inputPath = reader.nextString("/home/mehdi/projects/sbm-big-graphs/src/main/resources/input1-edges.txt");
        lablePath = reader.nextString("/home/mehdi/projects/sbm-big-graphs/src/main/resources/input1-labels.txt");
        cores = reader.nextInt(2);
        partitionNum = reader.nextInt(4);
    }

    public String getLablePath() {
        return lablePath;
    }
}
