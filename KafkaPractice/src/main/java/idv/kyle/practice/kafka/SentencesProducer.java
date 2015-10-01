package idv.kyle.practice.kafka;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SentencesProducer {
    public static void main(String[] args) {
	long events = 20;
	Random rnd = new Random();

	Properties props = new Properties();
	props.put("metadata.broker.list", "10.1.193.227:9092");
	props.put("serializer.class", "kafka.serializer.StringEncoder");
	props.put("partitioner.class", "idv.kyle.practice.kafka.SimplePartitioner");
	props.put("request.required.acks", "1");

	ProducerConfig config = new ProducerConfig(props);

	Producer<String, String> producer = new Producer<String, String>(config);

	String[] sentences = new String[] { "the cow jumped over the moon",
		"an apple a day keeps the doctor away",
		"four score and seven years ago",
		"snow white and the seven dwarfs", "i am at two with nature" };

	for (long nEvents = 0; nEvents < events; nEvents++) {
	    String sentence = sentences[rnd.nextInt(sentences.length)];
	    KeyedMessage<String, String> data = new KeyedMessage<String, String>(
		    "topic02", "partitionKey", sentence);
	    producer.send(data);
	}
	producer.close();
    }
}
