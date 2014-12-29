package idv.kyle.practice.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.elasticsearch.storm.EsBolt;

public class ESWriteTopology {

  public static class SentenceSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;

    @Override
    public void open(Map conf, TopologyContext context,
        SpoutOutputCollector collector) {
      _collector = collector;
      _rand = new Random();
    }

    @Override
    public void nextTuple() {
      for (int i = 0; i < 100; i++) {
        Utils.sleep(100);
        String[] sentences =
            new String[] { "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs", "i am at two with nature" };
        String sentence = sentences[_rand.nextInt(sentences.length)];
        _collector.emit(new Values(sentence));
      }

      return;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
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

  public static class WordCount extends BaseRichBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    OutputCollector _collector;

    transient CountMetric _countMetric;
    transient MultiCountMetric _wordCountMetric;
    transient ReducedMetric _wordLengthMeanMetric;

    @Override
    public void prepare(Map conf, TopologyContext context,
        OutputCollector collector) {
      _collector = collector;

      initMetrics(context);
    }

    void initMetrics(TopologyContext context) {
      _countMetric = new CountMetric();
      _wordCountMetric = new MultiCountMetric();
      _wordLengthMeanMetric = new ReducedMetric(new MeanReducer());

      context.registerMetric("execute_count", _countMetric, 5);
      context.registerMetric("word_count", _wordCountMetric, 60);
      context.registerMetric("word_length", _wordLengthMeanMetric, 60);
    }

    @Override
    public void execute(Tuple tuple) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      _collector.emit(new Values(word, count));
      updateMetrics(word);
    }

    void updateMetrics(String word) {
      _countMetric.incr();
      _wordCountMetric.scope(word).incr();
      _wordLengthMeanMetric.update(word.length());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new SentenceSpout(), 5);

    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split",
        new Fields("word"));
    builder.setBolt("es-bolt", new EsBolt("storm/docs"), 5).shuffleGrouping(
        "count");

    Config conf = new Config();
    conf.setDebug(true);
    conf.put("es.index.auto.create", "true");
    conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
