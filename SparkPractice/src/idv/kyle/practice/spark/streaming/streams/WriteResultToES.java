package idv.kyle.practice.spark.streaming.streams;

import idv.kyle.practice.spark.streaming.ConstantUtil;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteResultToES implements
    Function<JavaRDD<Map<String, String>>, Void> {
  private static final Logger LOG = LoggerFactory
      .getLogger(WriteResultToES.class);
  private static final long serialVersionUID = 6272424972267329328L;

  @Override
  public Void call(JavaRDD<Map<String, String>> rdd) throws Exception {
    Properties prop = new Properties();
    Path pt = new Path(ConstantUtil.propertiesFileName);
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      prop.load(new InputStreamReader(fs.open(pt)));
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    String esIndex = prop.getProperty("es.index");
    List<Map<String, String>> collect = rdd.collect();
    LOG.info("collect size: " + collect.size());
    if (collect.size() > 1) {
      for (Map<String, String> map : collect) {
        if (map.size() > 0) {
          LOG.info("ES index: " + esIndex);
          JavaEsSpark.saveToEs(rdd, esIndex);
          break;
        }
      }
    }

    return (Void) null;
  }
}
