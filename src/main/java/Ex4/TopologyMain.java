package Ex4;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Read-Fields-Spout", new ReadFieldsSpout());
        builder.setBolt("Filter-Fields-to-File-Bolt", new FilterFieldsToFileBolt())
                .shuffleGrouping("Read-Fields-Spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToRead", "/home/n_chernetsov/Education/Programming/Learn_by_example_ApacheStorm/src/main/java/Ex4/sample.txt");
        conf.put("dirToWrite", "/home/n_chernetsov/Education/Programming/Learn_by_example_ApacheStorm/src/main/java/Ex4/stormoutput/");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Write-to-File-Topology", conf, builder.createTopology());
            Thread.sleep(10_000);
        } finally {
            cluster.shutdown();
        }
    }
}
