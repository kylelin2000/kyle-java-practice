package idv.kyle.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutListSample {
	
	public void runPut() {
		try{
			String[] names = {"Andy", "Bob", "David", "Mandy"};
			String[] cities = {"Taipei", "Tainan", "Hsinchu"};
			String tableName = "k1";
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
			for (int i = 0; i < 2000; i++) {
				Put put = new Put(Bytes.toBytes("rowkey" + i));
				put.add(Bytes.toBytes(cf), Bytes.toBytes("name"),
						Bytes.toBytes(names[i%4]));
				put.add(Bytes.toBytes(cf), Bytes.toBytes("city"),
						Bytes.toBytes(cities[i%3]));
				put.add(Bytes.toBytes(cf), Bytes.toBytes("column1"),
						Bytes.toBytes("1.1"));
				put.add(Bytes.toBytes(cf), Bytes.toBytes("column2"),
						Bytes.toBytes("2.2"));
				put.add(Bytes.toBytes(cf), Bytes.toBytes("column3"),
						Bytes.toBytes("3.3"));
				puts.add(put);
			}
			
			htable.put(puts);
			htable.flushCommits();
			htable.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void main(String args[]){
		PutListSample putSample = new PutListSample();
		putSample.runPut();
	}
}
