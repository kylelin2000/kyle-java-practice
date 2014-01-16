package idv.kyle.hbase;

import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class TableDescriptorTest {
	
	public void runTest() {
		try{
			String tableName = "table2";
			String cf = "cf";
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "host3");
			config.set("hbase.zookeeper.property.clientPort", "2181");

			HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
			HTableDescriptor htableDesc = hbaseAdmin.getTableDescriptor(Bytes.toBytes(tableName));
			Map<ImmutableBytesWritable, ImmutableBytesWritable> maps = htableDesc.getValues();
			Set<Entry<ImmutableBytesWritable, ImmutableBytesWritable>> sets = maps.entrySet();
			for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entrySet : sets) {
				String stringKey = Bytes.toString(entrySet.getKey().get());
				String stringValue = Bytes.toString(entrySet.getValue().get());
				System.out.println("key:" + stringKey + ", value:" + stringValue);
			}
			hbaseAdmin.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void main(String args[]){
		TableDescriptorTest test = new TableDescriptorTest();
		test.runTest();
	}
}
