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

public class PutSample {
	
	public void runPut() {
		try{
			String tableName = "table3";
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
			Put put = new Put(Bytes.toBytes("rowkey1"));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("column1"), Bytes.toBytes(new Float(1.1)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("column2"), Bytes.toBytes(new Float(2.2)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("column3"), Bytes.toBytes(new Float(3.3)));
			puts.add(put);
			
			put = new Put(Bytes.toBytes("rowkey2"));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("column1"), Bytes.toBytes(new Float(4.4)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("column2"), Bytes.toBytes(new Float(5.5)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("column3"), Bytes.toBytes(new Float(6.6)));
			puts.add(put);
			
			put = new Put(Bytes.toBytes("rowkey3"));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("column1"), Bytes.toBytes(new Float(7.7)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("column2"), Bytes.toBytes(new Float(8.8)));
			put.add(Bytes.toBytes(cf), Bytes.toBytes("column3"), Bytes.toBytes(new Float(9.9)));
			puts.add(put);
			
			htable.put(puts);
			htable.flushCommits();
			htable.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void main(String args[]){
		PutSample putSample = new PutSample();
		putSample.runPut();
	}
}
