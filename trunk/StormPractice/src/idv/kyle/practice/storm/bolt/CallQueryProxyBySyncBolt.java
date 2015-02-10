package idv.kyle.practice.storm.bolt;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class CallQueryProxyBySyncBolt extends BaseBasicBolt {
  private static final Logger LOG = LoggerFactory
      .getLogger(CallQueryProxyBySyncBolt.class);

  private boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
        && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Utils.sleep(100);
    Thread t = Thread.currentThread();
    LOG.info("Thread name: " + t.getName() + ", Thread id: " + t.getId());
    if (isTickTuple(tuple)) {
      LOG.info("skip tick tuple");
      return;
    }
    String url = tuple.getString(0);
    LOG.info("query proxy url : " + url);
    CloseableHttpClient httpclient = HttpClientBuilder.create().build();
    try {
      int retry = 0;
      while (retry <= 3) {
        HttpGet httpget = new HttpGet(url);
        HttpResponse response = httpclient.execute(httpget);
        int status = response.getStatusLine().getStatusCode();
        LOG.info("Response status : " + status);
        if (status == HttpStatus.SC_OK) {
          HttpEntity entity = response.getEntity();
          String queryProxyResult = EntityUtils.toString(entity);
          LOG.info("result from query proxy : " + queryProxyResult);
          JSONObject jsonObj = new JSONObject(queryProxyResult);
          if ("200".equals(jsonObj.get("status").toString())) {
            collector.emit(new Values(url, jsonObj.get("status").toString(),
                jsonObj.get("result").toString(), jsonObj.get("service")
                    .toString()));
            break;
          } else {
            retry++;
            LOG.warn("result status is not ok. status : "
                + jsonObj.get("status").toString() + ", retry: " + retry);
          }
        } else {
          LOG.warn("query fail. status : " + status);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        httpclient.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("url", "status", "result", "service"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
