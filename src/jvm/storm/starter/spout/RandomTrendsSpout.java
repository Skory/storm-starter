package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.uncommons.maths.random.PoissonGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomTrendsSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final static Random random = new Random(System.currentTimeMillis());
    private final int batchSize;
    private final int batchIntervals;


    private final static List<String> networks = new ArrayList<String>(3) {{
        add("facebook");
        add("twitter");
        add("instagram");
    }};
    private final static List<String> sites = new ArrayList<>();
    private final static List<String> tags = new ArrayList<>();

    static {
        readSource("spout/sites.txt", sites);
        readSource("spout/tags.txt", tags);
    }

    public RandomTrendsSpout(int batchSize, int batchIntervals) {
        this.batchSize = batchSize;
        this.batchIntervals = batchIntervals;
    }

    private static void readSource(String filename, List<String> readTo) {
        PoissonGenerator poissonGenerator = new PoissonGenerator(4, random);
        File sitesFile = new File(filename);
        try {
            try (BufferedReader br = new BufferedReader(new FileReader(sitesFile))) {
                for (String line; (line = br.readLine()) != null; ) {
                    Integer repeatTimes = poissonGenerator.nextValue();
                    for (int i = 0; i < repeatTimes; ++i) {
                        readTo.add(line);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(batchIntervals);
        for (int i = 0; i < batchSize; ++i) {
            collector.emit(new Values(getNextValue(networks), getNextValue(sites), getNextValue(tags)));
        }
    }

    private String getNextValue(List<String> valueSource) {
        return valueSource.get(random.nextInt(valueSource.size()));
    }

    @Override
    public void ack(Object id) {
        //TBD
    }

    @Override
    public void fail(Object id) {
        //TBD
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("network", "site", "tag"));
    }
}
