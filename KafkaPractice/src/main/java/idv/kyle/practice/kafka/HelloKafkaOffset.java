package idv.kyle.practice.kafka;

import java.util.HashMap;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class HelloKafkaOffset {
  public static void getLastOffset(SimpleConsumer consumer, String topic,
      int partition, long whichTime, String clientName) {
    TopicAndPartition topicAndPartition =
        new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
        whichTime, 1));
    kafka.javaapi.OffsetRequest request =
        new kafka.javaapi.OffsetRequest(requestInfo,
            kafka.api.OffsetRequest.CurrentVersion(), clientName);
    System.out.println("offset current version: "
        + kafka.api.OffsetRequest.CurrentVersion());
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      System.out
          .println("Error fetching data Offset Data the Broker. ErrorCode: "
          + response.errorCode(topic, partition) + ", Error:"
          + response.toString());
    }
    long[] offsets = response.offsets(topic, partition);
    if (offsets.length > 0) {
      System.out.println("partion: " + partition + ", offset: " + offsets[0]
          + ", offset count: " + offsets.length);
    }
  }

  public static void main(String[] args) throws Exception {
    // long whichTime = kafka.api.OffsetRequest.LatestTime();
    long whichTime = -1L;
    // long whichTime = kafka.api.OffsetRequest.EarliestTime();
    System.out.println("whichTime: " + whichTime);
	for (int i = 0; i < 7; i++) {
	    HelloKafkaOffset.getLastOffset(new SimpleConsumer("10.201.84.43",
		    9092,
          100000, 64 * 1024, "leaderLookup"), "syslog_tp11", i, whichTime,
          "test_client");
    }
  }
}
