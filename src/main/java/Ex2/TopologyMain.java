package Ex2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("File-Reader-Spout", new FileReaderSpout());
        builder.setBolt("Simple-Bolt", new SimpleBolt())
                .shuffleGrouping("File-Reader-Spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToRead", "D:/Education/Programming/Learn_by_example_ApacheStorm/src/main/java/Ex2/sample.txt");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("File-Reader-Topology", conf, builder.createTopology());
            Thread.sleep(5000);
        } finally {
            cluster.shutdown();
        }
    }
}
