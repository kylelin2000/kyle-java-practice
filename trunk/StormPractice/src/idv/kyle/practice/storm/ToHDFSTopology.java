package idv.kyle.practice.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;

public class ToHDFSTopology {

  public static class RandomSentenceSpout extends BaseRichSpout {
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
      Utils.sleep(100);
      String[] sentences =
          new String[] { "the cow jumped over the moon",
              "an apple a day keeps the doctor away",
              "four score and seven years ago",
              "snow white and the seven dwarfs", "i am at two with nature" };
      String sentence = sentences[_rand.nextInt(sentences.length)];
      _collector.emit(new Values(sentence));
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

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
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
    if (args == null || args.length != 3) {
      System.out
          .println("Wrong number of arguments!!!"
              + "arg1: topology_name(Ex:TP), arg2: URL(Ex.hdfs://10.1.193.226:8020), arg3: HDFS file path(Ex:/tmp/maillog-out/)");
    } else {
      Map<String, Object> confMap = new HashMap<String, Object>();
      confMap.put("fs.hdfs.impl",
          "org.apache.hadoop.hdfs.DistributedFileSystem");

      Config conf = new Config();
      conf.setDebug(true);
      conf.put("es.index.auto.create", "true");
      conf.put("hdfs.config", confMap);

      FileNameFormat fileNameFormat =
          new DefaultFileNameFormat().withPath("/tmp/storm_out/")
              .withExtension(".out");

      RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(" ");

      // sync the filesystem after every 1k tuples
      SyncPolicy syncPolicy = new CountSyncPolicy(1000);

      // rotate files when they reach 5MB
      FileRotationPolicy rotationPolicy =
          new FileSizeRotationPolicy(50.0f, FileSizeRotationPolicy.Units.MB);

      HdfsBolt hdfsBolt =
          new HdfsBolt().withConfigKey("hdfs.config").withFsUrl(args[1])
              .withFileNameFormat(fileNameFormat).withRecordFormat(format)
              .withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy)
              .addRotationAction(new MoveFileAction().toDestination(args[2]));

      conf.setNumWorkers(2);

      TopologyBuilder builder = new TopologyBuilder();

      builder.setSpout("spout", new RandomSentenceSpout(), 5);

      builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
      builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split",
          new Fields("word"));
      builder.setBolt("hdfs", hdfsBolt, 4).shuffleGrouping("count");

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
  }
}
