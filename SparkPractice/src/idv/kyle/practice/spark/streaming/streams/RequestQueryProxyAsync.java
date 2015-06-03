package idv.kyle.practice.spark.streaming.streams;

import idv.kyle.practice.spark.streaming.ConstantUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.asynchttpclient.extras.registry.AsyncHttpClientFactory;
import org.asynchttpclient.providers.netty4.NettyAsyncHttpProviderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestQueryProxyAsync implements FlatMapFunction<String, String> {
  private static final Logger LOG = LoggerFactory
      .getLogger(RequestQueryProxyAsync.class);
  private static final long serialVersionUID = 7569717442952954475L;

  @Override
  public Iterable<String> call(String postBody) {
    LOG.info("query proxy postBody : " + postBody.trim());
    Properties prop = new Properties();
    Path pt = new Path(ConstantUtil.propertiesFileName);
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      prop.load(new InputStreamReader(fs.open(pt)));
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    String queryProxyUrl = prop.getProperty("queryproxy.url.batch");

    NettyAsyncHttpProviderConfig providerConfig =
        new NettyAsyncHttpProviderConfig();
    AsyncHttpClientConfig clientConfig =
        new AsyncHttpClientConfig.Builder()
            .setAllowPoolingConnections(true)
            .setAsyncHttpClientProviderConfig(providerConfig)
            .setMaxConnectionsPerHost(2).setMaxConnections(2).build();
    AsyncHttpClient asyncHttpClient =
        AsyncHttpClientFactory.getAsyncHttpClient(clientConfig);
    Future<Response> future =
        asyncHttpClient.preparePost(queryProxyUrl)
            .setBody(postBody.trim())
            .execute(new AsyncCompletionHandler<Response>() {
              @Override
              public Response onCompleted(Response response)
                  throws Exception {
                LOG.info("async client onCompleted");
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
      LOG.info("query proxy response status : " + status);
      if (status == HttpStatus.SC_OK) {
        InputStream resStream = response.getResponseBodyAsStream();
        BufferedReader br =
            new BufferedReader(new InputStreamReader(resStream));
        StringBuffer resBuffer = new StringBuffer();
        List<String> results = new ArrayList<String>();
        try {
          String resTemp = "";
          while ((resTemp = br.readLine()) != null) {
            LOG.info("query proxy result line: " + resTemp);
            results.add(resTemp);
            resBuffer.append(resTemp);
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          br.close();
        }
        String queryProxyResult = resBuffer.toString();
        LOG.info("query proxy result: " + queryProxyResult);
        return results;
      } else {
        LOG.warn("query fail. status : " + status);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      asyncHttpClient.close();
    }
    LOG.info("should not run here");
    return new ArrayList<String>();
  }
}
