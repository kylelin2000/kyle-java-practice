package idv.kyle.practice.spark.streaming.streams;

import idv.kyle.practice.spark.streaming.ConstantUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestQueryProxy implements FlatMapFunction<String, String> {
  private static final Logger LOG = LoggerFactory
      .getLogger(RequestQueryProxy.class);
  private static final long serialVersionUID = -5134257311931490911L;

  @Override
  public Iterable<String> call(String inputMessage) {
    Properties prop = new Properties();
    Path pt = new Path(ConstantUtil.propertiesFileName);
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      prop.load(new InputStreamReader(fs.open(pt)));
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    String queryProxyUrl = prop.getProperty("queryproxy.url.batch");

    HttpClient httpclient = new HttpClient();
    PostMethod method = new PostMethod(queryProxyUrl);

    try {
      String[] lines = inputMessage.trim().split("\n");
      StringBuffer requestContent = new StringBuffer();
      LOG.info("use POST to send " + lines.length
          + " query request to query proxy");
      for (String line : lines) {
        try {
          JSONObject jsonObj = new JSONObject(line);
          SimpleDateFormat sdf =
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          String context =
              jsonObj.get("context") + ", spark read from kafka at "
                  + sdf.format(new Date());
          jsonObj.put("context", context);
          requestContent.append("\n" + jsonObj.toString());
        } catch (JSONException jsonex) {
          LOG.warn("Not send request to query proxy. Not a valid json string: "
              + line);
        }
      }
      String postBody = requestContent.toString().trim();
      if (!"".equals(postBody) || !postBody.isEmpty()) {
        StringRequestEntity requestEntity =
            new StringRequestEntity(postBody, "application/json", "UTF-8");
        method.setRequestEntity(requestEntity);
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
            new DefaultHttpMethodRetryHandler(3, false));

        int statusCode = httpclient.executeMethod(method);
        LOG.info("query proxy response code: " + statusCode);
        if (statusCode != HttpStatus.SC_OK) {
          LOG.warn("Method failed: " + method.getStatusLine());
        }
        InputStream resStream = method.getResponseBodyAsStream();
        BufferedReader br =
            new BufferedReader(new InputStreamReader(resStream));
        StringBuffer resBuffer = new StringBuffer();
        List<String> results = new ArrayList<String>();
        try {
          String resTemp = "";
          while ((resTemp = br.readLine()) != null) {
            LOG.debug("query proxy result line: " + resTemp);
            results.add(resTemp);
            resBuffer.append(resTemp);
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          br.close();
        }
        String queryProxyResult = resBuffer.toString();
        LOG.debug("query proxy result: " + queryProxyResult);
        return results;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      method.releaseConnection();
    }
    LOG.info("should not run here");
    return new ArrayList<String>();
  }
}
