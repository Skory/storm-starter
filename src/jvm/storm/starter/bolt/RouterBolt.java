package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import storm.starter.model.DataFields;
import storm.starter.model.DataModel;

import java.util.*;

public class RouterBolt extends BaseRichBolt {

    private static final String DEFAULT_STREAM = "default";
    private OutputCollector collector;

    private Map<? extends Set<DataFields>, String> routingMap = ImmutableMap.of(
            EnumSet.allOf(DataFields.class), DEFAULT_STREAM);

    public RouterBolt() {
    }

    public RouterBolt(Map<? extends Set<DataFields>, String> routingMap) {
        this.routingMap = routingMap;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        DataModel model = new DataModel(tuple.getStringByField(DataFields.network.name()),
                tuple.getStringByField(DataFields.site.name()),
                tuple.getStringByField(DataFields.tag.name()));

        for (Map.Entry<? extends Set<DataFields>, String> routingEntry : routingMap.entrySet()) {
            List<String> fieldValues = new ArrayList<String>();
            for (DataFields dataField : routingEntry.getKey()) {
                fieldValues.add(model.get(dataField));
            }
            collector.emit(routingEntry.getValue(), ImmutableList.<Object>of(fieldValues));
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        for (Map.Entry<? extends Set<DataFields>, String> routingEntry : routingMap.entrySet()) {
            outputFieldsDeclarer.declareStream(routingEntry.getValue(), new Fields("tuple"));
        }
    }
}
