package idv.kyle.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class DataTypePutSample {
	
	public void runPut() {
		try{
			String tableName = "table99";
			String cf = "cf";
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "host3");
			config.set("hbase.zookeeper.property.clientPort", "2181");

			HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
			if (!hbaseAdmin.tableExists(tableName)) {
				HTableDescriptor htableDesc = new HTableDescriptor(tableName);
				HColumnDescriptor hcolumnDesc = new HColumnDescriptor(cf); 
				htableDesc.addFamily(hcolumnDesc);
				hbaseAdmin.createTable(htableDesc);
			}
			
			HTablePool pool = new HTablePool(config, 5);
			HTableInterface htable = pool.getTable(tableName);
			
			List<Put> puts = new ArrayList<Put>();
			Put put = new Put(Bytes.toBytes("rowkey2"));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("integer"), Bytes.toBytes(new Integer(1234567)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("long"), Bytes.toBytes(new Long(1235342)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("float"), Bytes.toBytes(new Float(1234.423)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("double"), Bytes.toBytes(new Double(123.954)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("string1"), Bytes.toBytes(new String("1243")));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("string2"), Bytes.toBytes(new String("ab5c")));
			puts.add(put);
			
			htable.put(puts);
			htable.flushCommits();
			htable.close();
			pool.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void main(String args[]){
		DataTypePutSample putSample = new DataTypePutSample();
		putSample.runPut();
	}
}
