package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PrinterBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(PrinterBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        logger.info(tuple.getValue(0).toString());
//        System.out.println(tuple.getValue(0));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
