package storm.starter.metrics;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import storm.starter.Application;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.RouterBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.spout.RandomTrendsSpout;

import java.util.concurrent.TimeUnit;

public class MetricsManager {
    public static final Counter SPOUT_COUNTER = Application.getMetrics().newCounter(RandomTrendsSpout.class, "pendingTuples");
    public static final Meter SPOUT_ACK_MESSAGES_METER = Application.getMetrics().newMeter(RandomTrendsSpout.class, "ackMessageCount", "ackMessageCount", TimeUnit.SECONDS);
    public static final Meter SPOUT_MESSAGES_METER = Application.getMetrics().newMeter(RandomTrendsSpout.class, "totalMessageCount", "totalMessageCount", TimeUnit.SECONDS);
    public static final Meter ROUTER_BOLT_METER =  Application.getMetrics().newMeter(RouterBolt.class, "grouping", "grouping", TimeUnit.SECONDS);
    public static final Meter INTERMEDIATE_BOLT_METER =  Application.getMetrics().newMeter(IntermediateRankingsBolt.class, "updateRankings", "updateRankings", TimeUnit.SECONDS);
    public static final Meter TOTAL_BOLT_METER =  Application.getMetrics().newMeter(TotalRankingsBolt.class, "updateTotalRankings", "updateTotalRankings", TimeUnit.SECONDS);
    public static final Meter ROLLING_BOLT_METER =  Application.getMetrics().newMeter(RollingCountBolt.class, "incrementCount", "incrementCount", TimeUnit.SECONDS);
}
