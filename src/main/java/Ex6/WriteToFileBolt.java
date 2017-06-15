package Ex6;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class WriteToFileBolt extends BaseBasicBolt {
    private PrintWriter writer;

    public void prepare(Map stormConf, TopologyContext context) {
        String fileName = "output" + "-" + context.getThisTaskId() + "-" + context.getThisComponentId() + ".txt";
        try {
            writer = new PrintWriter(stormConf.get("dirToWrite").toString() + fileName, "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getStringByField("integer") + "-" + input.getStringByField("bucket");
        collector.emit(new Values(str));
        writer.println(str);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field"));
    }

    public void cleanup() {
        writer.close();
    }
}
