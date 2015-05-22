package idv.kyle.practice.spark.streaming.streams;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseFromQueryProxy implements
    Function<String, Map<String, String>> {
  private static final Logger LOG = LoggerFactory
      .getLogger(ResponseFromQueryProxy.class);
  private static final long serialVersionUID = 7293036169143951183L;

  @Override
  public Map<String, String> call(String line) {
    Map<String, String> resultMap = new HashMap<String, String>();
    String result = line.toString();
    LOG.debug("query proxy return single result: " + result);
    if (!result.isEmpty()) {
      try {
        JSONObject jsonObj = new JSONObject(result);
        if ("200".equals(jsonObj.get("status").toString())) {
          SimpleDateFormat sdf =
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          jsonObj.append("returnTimeFromQueryProxy", sdf.format(new Date()));
          Iterator<String> keys = jsonObj.keys();
          while (keys.hasNext()) {
            String keyValue = (String) keys.next();
            String valueString = jsonObj.getString(keyValue);
            resultMap.put(keyValue, valueString);
          }
          return resultMap;
        } else {
          LOG.error("query proxy result status is not 200");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return resultMap;
  }
}
