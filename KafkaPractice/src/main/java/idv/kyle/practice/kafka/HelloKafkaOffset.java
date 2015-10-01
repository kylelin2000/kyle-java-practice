package idv.kyle.practice.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class HelloKafkaOffset {
    private List<String> replicaBrokers = new ArrayList<String>();

    public HelloKafkaOffset() {
	replicaBrokers = new ArrayList<String>();
    }

    public long getLastOffset(SimpleConsumer consumer, String topic,
	    int partition, long whichTime, String clientName) {
	TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
		partition);
	Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
	requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
		whichTime, 1));
	kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
		requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
		clientName);
	OffsetResponse response = consumer.getOffsetsBefore(request);

	if (response.hasError()) {
	    System.out
		    .println("Error fetching data Offset Data the Broker. ErrorCode: "
			    + response.errorCode(topic, partition)
			    + ", Error:"
			    + response.toString());
	}
	long[] offsets = response.offsets(topic, partition);
	return offsets[0];
    }

    public long getFetchOffset(SimpleConsumer consumer, String groupId,
	    String topic,
 int partition, String clientName) {
	TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
		partition);
	List<TopicAndPartition> requestInfo = new ArrayList<TopicAndPartition>();
	requestInfo.add(topicAndPartition);
	short versionId = 0;
	int correlationId = 0;
	kafka.javaapi.OffsetFetchRequest request = new OffsetFetchRequest(groupId, requestInfo, versionId, correlationId, clientName);
	OffsetFetchResponse response = consumer.fetchOffsets(request);
	return response.offsets().get(topicAndPartition).offset();
    }

    // Find the Lead Broker for a Topic Partition
    private PartitionMetadata findLeader(List<String> seedBrokers, int port,
	    String topic, int partition) {
	for (String seed : seedBrokers) {
	    SimpleConsumer consumer = null;
	    try {
		consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024,
			"leaderLookup");
		List<String> topics = Collections.singletonList(topic);
		TopicMetadataRequest req = new TopicMetadataRequest(topics);
		kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

		List<TopicMetadata> metaData = resp.topicsMetadata();
		for (TopicMetadata item : metaData) {
		    for (PartitionMetadata part : item.partitionsMetadata()) {
			if (part.partitionId() == partition) {
			    replicaBrokers.clear();
			    for (kafka.cluster.Broker replica : part.replicas()) {
				replicaBrokers.add(replica.host());
			    }
			    return part;
			}
		    }
		}
	    } catch (Exception e) {
		System.out.println("Error communicating with Broker [" + seed
			+ "] to find Leader for [" + topic + ", " + partition
			+ "] Reason: " + e);
	    } finally {
		if (consumer != null)
		    consumer.close();
	    }
	}
	return null;
    }

    public static void main(String[] args) throws Exception {
	HelloKafkaOffset worker = new HelloKafkaOffset();
	List<String> brokers = new ArrayList<String>();
	brokers.add("10.1.192.4");
	String topic = "syslog_tp01";
	int port = 9092;
	for (int partition = 0; partition < 5; partition++) {
	    PartitionMetadata metadata = worker.findLeader(brokers, port,
		    topic, partition);
	    if (metadata == null) {
		System.out
			.println("Can't find metadata for Topic and Partition. Exiting");
		return;
	    }
	    if (metadata.leader() == null) {
		System.out
			.println("Can't find Leader for Topic and Partition. Exiting");
		return;
	    }
	    String leadBroker = metadata.leader().host();
	    System.out.println("leadBroker: " + leadBroker);
	    String clientName = "Client_" + topic + "_" + partition;

	    SimpleConsumer consumer = new SimpleConsumer(leadBroker, port,
		    100000, 64 * 1024, clientName);
	    long lastOffset = worker.getLastOffset(consumer, topic, partition,
		    kafka.api.OffsetRequest.LatestTime(), clientName);
	    long fetchOffset = worker.getFetchOffset(consumer,
		    "syslog-tp01-group", topic, partition, clientName);
	    System.out.println("partition: " + partition + ", lastOffset: "
		    + lastOffset + ", lag: " + (lastOffset - fetchOffset));
	}
    }
}
