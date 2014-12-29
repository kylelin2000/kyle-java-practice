package idv.kyle.practice.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import org.elasticsearch.storm.EsSpout;

public class ESReadTopology {

  public static class PrinterBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      System.out.println("tuple: " + tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 3) {
      System.out
          .println("Wrong number of arguments!!!"
              + "arg1: topology_name(Ex:TP), arg2: index/type, arg3: query string(Ex:?q=*:*)");
    } else {
      Config conf = new Config();
      conf.setDebug(true);
      conf.put("es.index.auto.create", "true");
      conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);

      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("esSpout", new EsSpout(args[1], args[2]), 5);
      builder.setBolt("bolt", new PrinterBolt()).shuffleGrouping("esSpout");

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
}
