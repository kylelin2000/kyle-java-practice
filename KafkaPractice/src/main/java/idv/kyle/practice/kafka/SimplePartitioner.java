package idv.kyle.practice.kafka;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
    private static final Logger LOG = LoggerFactory.getLogger(SimplePartitioner.class);

    private AtomicInteger counter = new AtomicInteger(0);

    public SimplePartitioner(VerifiableProperties props) {
	LOG.info("call partitioner");
    }

    public int partition(Object key, int a_numPartitions) {
	LOG.info("key: " + key + ", num of partition: " + a_numPartitions);
	int partition = counter.incrementAndGet() % a_numPartitions;
	if (counter.get() > 10) {
	    counter.set(0);
	}
	LOG.info("use partition: " + partition);
	return partition;
    }
}
