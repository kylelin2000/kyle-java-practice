package idv.kyle.practice.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.storm.EsBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class FromKafkaToESTopology {
  public static class PrinterBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      System.out.println("print tuple: " + tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

  }

  public static class SplitSentence extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String line = tuple.toString();
      String all[] = line.split(" ");
      for (String word : all) {
        collector.emit(new Values(word));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class WordCount extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      LOG.info("read word : " + word);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 4) {
      System.out
          .println("Wrong number of arguments!!!"
              + "arg1: topology_name(Ex:TP), arg2: zk root(Ex:/kafka-storm), arg3: topic_name, arg4: ES index/type(Ex:stormtest/docs)");
    } else {
      TopologyBuilder builder = new TopologyBuilder();

      BrokerHosts brokerHosts = new ZkHosts("sparkvm.localdomain:2181");
      SpoutConfig spoutConfig =
          new SpoutConfig(brokerHosts, args[2], args[1], "consumer1");
      spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
      spoutConfig.forceFromStart = true;
      spoutConfig.startOffsetTime = -1;

      builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
      builder.setBolt("print", new PrinterBolt()).shuffleGrouping("spout");

      /*
      builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
      builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split",
          new Fields("word"));
      builder.setBolt("es-bolt", new EsBolt(args[3]), 5).shuffleGrouping(
          "count");
          */

      Config conf = new Config();
      conf.setDebug(true);
      conf.put("es.index.auto.create", "true");

      conf.setNumWorkers(2);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
  }
}
