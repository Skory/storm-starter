package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.*;
import com.yammer.metrics.core.Timer;
import storm.starter.Application;
import storm.starter.metrics.MetricsManager;
import storm.starter.model.DataFields;
import storm.starter.model.DataModel;
import storm.starter.util.Action1;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class RouterBolt extends BaseRichBolt {

    private static final String DEFAULT_STREAM = "default";
    private final String meterName;
    private final String timerName;
    private OutputCollector collector;

    private Map<? extends Set<DataFields>, String> routingMap = ImmutableMap.of(
            EnumSet.allOf(DataFields.class), DEFAULT_STREAM);

    public RouterBolt(Map<? extends Set<DataFields>, String> routingMap) {
        this.routingMap = routingMap;

        MetricName metricName = new MetricName(RouterBolt.class, "requests");
        Meter requests = Application.getMetrics().newMeter(metricName, "execute", TimeUnit.SECONDS);
        meterName = metricName.toString();
        MetricsManager.register(meterName, requests);

        MetricName timerMetric = new MetricName(RouterBolt.class, "grouping");
        Timer timer = Application.getMetrics().newTimer(timerMetric, TimeUnit.SECONDS, TimeUnit.SECONDS);
        timerName = timerMetric.toString();
        MetricsManager.register(timerName, timer);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(final Tuple tuple) {
        MetricsManager.interactWith(meterName, new Action1<Meter>() {
            @Override
            public void invoke(Meter arg) {
                arg.mark();
            }
        });

        MetricsManager.interactWith(timerName, new Action1<Timer>() {
            @Override
            public void invoke(Timer arg) {
                TimerContext time = arg.time();
                try {
                    DataModel model = new DataModel(tuple.getStringByField(DataFields.network.name()),
                            tuple.getStringByField(DataFields.site.name()),
                            tuple.getStringByField(DataFields.tag.name()));

                    for (Map.Entry<? extends Set<DataFields>, String> routingEntry : routingMap.entrySet()) {
                        List<String> fieldValues = new ArrayList<>();
                        for (DataFields dataField : routingEntry.getKey()) {
                            fieldValues.add(model.get(dataField));
                        }

                        collector.emit(routingEntry.getValue(), ImmutableList.<Object>of(fieldValues));
                    }

                    collector.ack(tuple);
                } finally {
                    time.stop();
                }
            }
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        for (Map.Entry<? extends Set<DataFields>, String> routingEntry : routingMap.entrySet()) {
            outputFieldsDeclarer.declareStream(routingEntry.getValue(), new Fields("tuple"));
        }
    }
}
