package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.collect.Lists;
import org.uncommons.maths.random.PoissonGenerator;
import storm.starter.TrendingTopology;
import storm.starter.metrics.MetricsManager;
import storm.starter.model.DataModel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class RandomTrendsSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final static Random random = new Random(System.currentTimeMillis());
    private final int batchSize;
    private final int batchIntervals;
    private final Map<DataModel, Long> statisticMap = new HashMap<>();
    private static final List<PoissonGenerator> POISSON_GENERATORS = Lists.newArrayList(
            new PoissonGenerator(2, random),
            new PoissonGenerator(10, random),
            new PoissonGenerator(100, random)
    );

    private final List<String> networks = new ArrayList<String>(3) {{
        add("facebook");
        add("twitter");
        add("instagram");
    }};
    private final List<String> sites = new ArrayList<>();
    private final List<String> tags = new ArrayList<>();

    public RandomTrendsSpout(int batchSize, int batchIntervals) {
        this.batchSize = batchSize;
        this.batchIntervals = batchIntervals;

        readSource("/sites.txt", sites);
        readSource("/tags.txt", tags);
    }

    private void readSource(String resourceName, List<String> readTo) {
        List<String> lines = new ArrayList<>();
        try {
            InputStream resourceAsStream = TrendingTopology.class.getResourceAsStream(resourceName);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(resourceAsStream))) {
                for (String line; (line = br.readLine()) != null; ) {
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0, linesSize = lines.size(); i < linesSize; i++) {
            String line = lines.get(i);
            PoissonGenerator poissonGenerator = getPoissonGenerator(i, linesSize);
            Integer repeat = poissonGenerator.nextValue();
            for (int j = 0; j < repeat; ++j) {
                readTo.add(line);
            }
        }

    }

    private static PoissonGenerator getPoissonGenerator(int index, int totalCount) {
        int percent = index / totalCount * 100;
        if (percent < 60) {
            return POISSON_GENERATORS.get(0);
        } else if (percent >= 60 && percent < 90) {
            return POISSON_GENERATORS.get(1);
        }

        // > 90
        return POISSON_GENERATORS.get(2);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(batchIntervals);

        for (int i = 0; i < batchSize; ++i) {
            DataModel dataModel = new DataModel(getNextValue(networks), getNextValue(sites), getNextValue(tags));
            Values tuple = new Values(dataModel.getNetwork(), dataModel.getSite(), dataModel.getTag());

            MetricsManager.SPOUT_COUNTER.inc();
            MetricsManager.SPOUT_MESSAGES_METER.mark();

            collector.emit(tuple, dataModel);
        }
    }

    private String getNextValue(List<String> valueSource) {
        return valueSource.get(random.nextInt(valueSource.size()));
    }

    @Override
    public void ack(Object id) {
        DataModel dataModel = (DataModel) id;
        Long count = statisticMap.get(dataModel);
        if (count == null) {
            statisticMap.put(dataModel, 0L);
        } else {
            statisticMap.put(dataModel, count + 1);
        }

        MetricsManager.SPOUT_ACK_MESSAGES_METER.mark();
        MetricsManager.SPOUT_COUNTER.dec();
    }

    @Override
    public void fail(Object id) {
        DataModel dataModel = (DataModel) id;
        collector.emit(new Values(dataModel.getNetwork(), dataModel.getSite(), dataModel.getTag()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("network", "site", "tag"));
    }
}
