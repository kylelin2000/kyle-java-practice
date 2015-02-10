package idv.kyle.practice.storm.bolt;

import java.util.Map;
import java.util.concurrent.Future;

import org.apache.http.HttpStatus;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class CallQueryProxyByAsync2Bolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory
      .getLogger(CallQueryProxyByAsync2Bolt.class);

  OutputCollector _collector;

  @Override
  public void prepare(Map conf, TopologyContext context,
      OutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    Utils.sleep(100);
    Thread t = Thread.currentThread();
    LOG.info("Thread name: " + t.getName() + ", Thread id: " + t.getId());
    if (isTickTuple(tuple)) {
      LOG.info("skip tick tuple");
      return;
    }
    String url = tuple.getString(0);
    LOG.info("query proxy url : " + url);
    AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    Future<Response> future =
        asyncHttpClient.prepareGet(url).execute(
            new AsyncCompletionHandler<Response>() {
              @Override
              public Response onCompleted(Response response) throws Exception {
                return response;
              }

              @Override
              public void onThrowable(Throwable t) {
                t.printStackTrace();
              }
            });

    try {
      Response response = future.get();
      int status = response.getStatusCode();
      LOG.info("Response status : " + status);
      if (status == HttpStatus.SC_OK) {
        String queryProxyResult = response.getResponseBody();
        LOG.info("result from query proxy : " + queryProxyResult);
        JSONObject jsonObj = new JSONObject(queryProxyResult);
        if ("200".equals(jsonObj.get("status").toString())) {
          synchronized (_collector) {
            _collector.emit(tuple, new Values(jsonObj.get("status").toString(),
                jsonObj.get("result").toString(), jsonObj.get("service")
                    .toString()));
            _collector.ack(tuple);
          }
        } else {
          LOG.warn("result status is not ok. status : "
              + jsonObj.get("status").toString());
        }
      } else {
        LOG.warn("query fail. status : " + status);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
        && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
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
