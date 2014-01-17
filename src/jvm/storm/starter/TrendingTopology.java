package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableMap;
import storm.starter.bolt.*;
import storm.starter.spout.RandomTrendsSpout;

import java.util.EnumSet;

import static storm.starter.model.DataFields.*;

public class TrendingTopology {
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomTrendsSpout(10, 10), 1);

        builder.setBolt("router", new RouterBolt(ImmutableMap.of(EnumSet.of(network, site, tag), "nst",
                EnumSet.of(network, site), "ns",
                EnumSet.of(network, tag), "nt")), 1).noneGrouping("spout");

        for (String group : new String[]{"nst", "ns", "nt"}) {
            String countBoltName = "count-" + group;
            builder.setBolt(countBoltName, new RollingCountBolt(60, 10), 3).fieldsGrouping("router", group, new Fields("tuple"));
            String topNIntermediateBoltName = "topN-intermediate-" + group;
            builder.setBolt(topNIntermediateBoltName, new IntermediateRankingsBolt(10, 5), 3).shuffleGrouping(countBoltName);
            String topNBoltName = "topN-" + group;
            builder.setBolt(topNBoltName, new TotalRankingsBolt(10, 1), 1).globalGrouping(topNIntermediateBoltName);
            builder.setBolt("printer-" + group, new PrinterBolt()).globalGrouping(topNBoltName);
        }

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("trend-count", conf, builder.createTopology());

            Thread.sleep(120 * 1000);

            cluster.shutdown();
        }
    }
}
