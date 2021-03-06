package storm.starter.bolt;

import backtype.storm.tuple.Tuple;
import com.yammer.metrics.core.*;
import org.apache.log4j.Logger;
import storm.starter.metrics.MetricsManager;
import storm.starter.tools.Rankings;

/**
 * This bolt merges incoming {@link Rankings}.
 * <p/>
 * It can be used to merge intermediate rankings generated by {@link IntermediateRankingsBolt} into a final,
 * consolidated ranking. To do so, configure this bolt with a globalGrouping on {@link IntermediateRankingsBolt}.
 */
public final class TotalRankingsBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -8447525895532302198L;
    private static final Logger LOG = Logger.getLogger(TotalRankingsBolt.class);

    public TotalRankingsBolt() {
        super();
    }

    public TotalRankingsBolt(int topN) {
        super(topN);
    }

    public TotalRankingsBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    void updateRankingsWithTuple(final Tuple tuple) {
        MetricsManager.TOTAL_BOLT_METER.mark();

        Rankings rankingsToBeMerged = (Rankings) tuple.getValue(0);
        Rankings rankings = getRankings();
        rankings.updateWith(rankingsToBeMerged);
        rankings.pruneZeroCounts();
    }

    @Override
    Logger getLogger() {
        return LOG;
    }

}
