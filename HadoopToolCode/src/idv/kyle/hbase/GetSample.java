package idv.kyle.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class GetSample {
	static Logger LOG = Logger.getLogger(GetSample.class.getName());
	
	public GetSample() throws IOException {
	}
	
	public void queryHTable() throws Exception {
		String tableName = "table99";
		String cf = "cf";
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "host3");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		HTablePool pool = null;
		HTableInterface htable = null;
		try {
			pool = new HTablePool(config, 8);
			htable = pool.getTable(tableName);
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(cf));
			ResultScanner resultScanner = htable.getScanner(scan);
			LOG.info("size of boolean = " + Bytes.SIZEOF_BOOLEAN);
			LOG.info("size of byte = " + Bytes.SIZEOF_BYTE);
			LOG.info("size of char = " + Bytes.SIZEOF_CHAR);
			LOG.info("size of double = " + Bytes.SIZEOF_DOUBLE);
			LOG.info("size of float = " + Bytes.SIZEOF_FLOAT);
			LOG.info("size of int = " + Bytes.SIZEOF_INT);
			LOG.info("size of long = " + Bytes.SIZEOF_LONG);
			LOG.info("size of short = " + Bytes.SIZEOF_SHORT);
			for (Result result : resultScanner) {
				List<KeyValue> kvs = result.list();
				for (KeyValue kv : kvs) {
					LOG.info("Key:" + new String(kv.getRow())
							+ ", Qualifier:" + new String(kv.getQualifier())
							+ ", Value Length:" + kv.getValueLength());
					if (Bytes.SIZEOF_BOOLEAN == kv.getValueLength()) {
						LOG.info("Boolean:" + Bytes.toBoolean(kv.getValue()));
					} else if (Bytes.SIZEOF_BYTE == kv.getValueLength()) {
						LOG.info("Byte:" + kv.getValue());
					} else if (Bytes.SIZEOF_CHAR == kv.getValueLength()) {
						LOG.info("Char:" + Bytes.toString(kv.getValue()));
					} else if (Bytes.SIZEOF_DOUBLE == kv.getValueLength()) {
						LOG.info("Double:" + Bytes.toDouble(kv.getValue()));
					} else if (Bytes.SIZEOF_FLOAT == kv.getValueLength()) {
						LOG.info("Float:" + Bytes.toFloat(kv.getValue()));
					} else if (Bytes.SIZEOF_INT == kv.getValueLength()) {
						LOG.info("Int:" + Bytes.toInt(kv.getValue()));
					} else if (Bytes.SIZEOF_LONG == kv.getValueLength()) {
						LOG.info("Long:" + Bytes.toLong(kv.getValue()));
					} else if (Bytes.SIZEOF_SHORT == kv.getValueLength()) {
						LOG.info("Short:" + Bytes.toShort(kv.getValue()));
					}
				}
				LOG.info(result.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (htable != null) {
				htable.close();
			}
			if (pool != null) {
				pool.close();
			}
		}
	}
	
	public static void main(String args[]) throws Exception {
		GetSample getSample = new GetSample();
		getSample.queryHTable();
	}
}
