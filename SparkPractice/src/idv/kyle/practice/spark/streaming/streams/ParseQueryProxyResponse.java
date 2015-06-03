package idv.kyle.practice.spark.streaming.streams;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseQueryProxyResponse implements FlatMapFunction<Future<Response>, String> {
  private static final Logger LOG = LoggerFactory
      .getLogger(ParseQueryProxyResponse.class);
  private static final long serialVersionUID = 7293036169143951183L;

  @Override
  public Iterable<String> call(Future<Response> future) {
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
    }
    LOG.info("should not run here");
    return new ArrayList<String>();
  }
}
