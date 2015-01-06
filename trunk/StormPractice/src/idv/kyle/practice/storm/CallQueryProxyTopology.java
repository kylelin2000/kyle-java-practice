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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.storm.EsBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallQueryProxyTopology {
  public static class HDFSFileReaderSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    String _uri;

    public HDFSFileReaderSpout(String uri) {
      _uri = uri;
    }

    @Override
    public void open(Map conf, TopologyContext context,
        SpoutOutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void nextTuple() {
      InputStream in = null;
      try {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(URI.create(_uri), conf);
        System.out.println("fs: " + fs);
        in = fs.open(new Path(_uri));
        // IOUtils.copyBytes(in, System.out, 4096, false);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while (null != (line = br.readLine())) {
          _collector.emit(new Values(line));
          Utils.sleep(100);
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        IOUtils.closeStream(in);
      }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("url"));
    }
  }

  public static class CallQueryProxy extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory
        .getLogger(CallQueryProxy.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      Utils.sleep(100);
      String url = tuple.getString(0);
      LOG.info("Input line : " + url);
      try {
        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(url);
        HttpResponse response = httpclient.execute(httpget);
        String status = response.getStatusLine().toString();
        LOG.info("Response status : " + status);
        HttpEntity entity = response.getEntity();
        BufferedReader rd =
            new BufferedReader(new InputStreamReader(entity.getContent()));
        String inputLine;
        while ((inputLine = rd.readLine()) != null) {
          LOG.info("inputLine : " + inputLine);
          JSONObject jsonObj = new JSONObject(inputLine);
          collector.emit(new Values(jsonObj.get("status"), jsonObj
              .get("result"), jsonObj.get("service")));
        }
        rd.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("status", "result", "service"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 3) {
      System.out
          .println("Wrong number of arguments!!!"
              + "arg1: topology_name(Ex:TP), arg2: HDFS file path(Ex:hdfs://192.168.1.10:8020/tmp/maillog), arg3: ES index/type(Ex:stormtest/docs)");
    } else {
      TopologyBuilder builder = new TopologyBuilder();

      builder.setSpout("spout", new HDFSFileReaderSpout(args[1]), 5);

      builder.setBolt("query", new CallQueryProxy(), 8)
          .shuffleGrouping("spout");
      builder.setBolt("es-bolt", new EsBolt(args[2]), 5).shuffleGrouping(
          "query");

      Config conf = new Config();
      conf.setDebug(true);
      conf.put("es.index.auto.create", "true");

      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
  }
}
