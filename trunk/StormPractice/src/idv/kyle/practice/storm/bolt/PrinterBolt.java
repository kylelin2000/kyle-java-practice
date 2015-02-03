package idv.kyle.practice.storm.bolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PrinterBolt extends BaseBasicBolt {
  private static final Logger LOG = LoggerFactory.getLogger(PrinterBolt.class);

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    LOG.info("print tuple: " + tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}
