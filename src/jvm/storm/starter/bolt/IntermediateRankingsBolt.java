package storm.starter.bolt;

import backtype.storm.tuple.Tuple;
import com.yammer.metrics.core.*;
import org.apache.log4j.Logger;
import storm.starter.metrics.MetricsManager;
import storm.starter.tools.Rankable;
import storm.starter.tools.RankableObjectWithFields;

/**
 * This bolt ranks incoming objects by their count.
 * <p/>
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {
    private static final long serialVersionUID = -1369800530256637409L;
    private static final Logger LOG = Logger.getLogger(IntermediateRankingsBolt.class);

    public IntermediateRankingsBolt() {
        super();
    }

    public IntermediateRankingsBolt(int topN) {
        super(topN);
    }

    public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    void updateRankingsWithTuple(final Tuple tuple) {
        MetricsManager.INTERMEDIATE_BOLT_METER.mark();

        Rankable rankable = RankableObjectWithFields.from(tuple);
        getRankings().updateWith(rankable);
    }

    @Override
    Logger getLogger() {
        return LOG;
    }
}
