package storm.starter.bolt;

import backtype.storm.tuple.Tuple;
import com.yammer.metrics.core.*;
import org.apache.log4j.Logger;
import storm.starter.Application;
import storm.starter.metrics.MetricsManager;
import storm.starter.tools.Rankable;
import storm.starter.tools.RankableObjectWithFields;
import storm.starter.util.Action1;

import java.util.concurrent.TimeUnit;

/**
 * This bolt ranks incoming objects by their count.
 * <p/>
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {
    private String meterName;
    private String timerName;

    private static final long serialVersionUID = -1369800530256637409L;
    private static final Logger LOG = Logger.getLogger(IntermediateRankingsBolt.class);

    public IntermediateRankingsBolt() {
        super();
        registerMetrics();
    }

    public IntermediateRankingsBolt(int topN) {
        super(topN);
        registerMetrics();
    }

    public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
        registerMetrics();
    }

    private void registerMetrics() {
        MetricName metricName = new MetricName(IntermediateRankingsBolt.class, "requests");
        Meter requests = Application.getMetrics().newMeter(metricName, "execute", TimeUnit.SECONDS);
        meterName = metricName.toString();
        MetricsManager.register(meterName, requests);

        MetricName timerMetric = new MetricName(IntermediateRankingsBolt.class, "updateRankings");
        Timer timer = Application.getMetrics().newTimer(timerMetric, TimeUnit.SECONDS, TimeUnit.SECONDS);
        timerName = timerMetric.toString();
        MetricsManager.register(timerName, timer);
    }

    @Override
    void updateRankingsWithTuple(final Tuple tuple) {
        MetricsManager.interactWith(timerName, new Action1<Timer>() {
            @Override
            public void invoke(Timer arg) {
                TimerContext time = arg.time();
                try {
                    Rankable rankable = RankableObjectWithFields.from(tuple);
                    getRankings().updateWith(rankable);
                } finally {
                    time.stop();
                }
            }
        });

    }

    @Override
    void onExecute() {
        MetricsManager.interactWith(meterName, new Action1<Meter>() {
            @Override
            public void invoke(Meter arg) {
                arg.mark();
            }
        });
    }

    @Override
    Logger getLogger() {
        return LOG;
    }
}
