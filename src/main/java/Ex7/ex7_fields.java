package Ex7;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class ex7_fields {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Integer-Spout", new IntegerSpout());
        builder.setBolt("Write-to-File-Bolt", new WriteToFileBolt(), 2)
            .fieldsGrouping("Integer-Spout", new Fields("bucket"));

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("dirToWrite", "/storage/Education/Programming/apache-storm/Learn_by_example_ApacheStorm/src/main/java/Ex7/ex7_output/");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Shuffle-Grouping-Topology", conf, builder.createTopology());
            Thread.sleep(20_000);
        } finally {
            cluster.shutdown();
        }
    }
}
