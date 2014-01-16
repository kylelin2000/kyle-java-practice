package idv.kyle.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteSample {
	public void runDelete() {
		long start = System.currentTimeMillis();
		try{
			String tableName = "table3";
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "host3");
			config.set("hbase.zookeeper.property.clientPort", "2181");

			HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
			if (!hbaseAdmin.tableExists(tableName)) {
				System.out.println("Table(" + tableName + ") not exist!!!");
				return;
			}
			HTablePool pool = new HTablePool(config, 8);
			HTableInterface htable = pool.getTable(tableName);
			
			List<Delete> dels = new ArrayList<Delete>();
			
			Delete del = new Delete(Bytes.toBytes("rowkey1"));
			dels.add(del);
			del = new Delete(Bytes.toBytes("rowkey2"));
			dels.add(del);
			del = new Delete(Bytes.toBytes("rowkey3"));
			dels.add(del);
			
			htable.delete(dels);
			htable.flushCommits();
			htable.close();
		}catch(Exception e){
			throw new RuntimeException();
		}
		long end = System.currentTimeMillis();
    System.out.println("Cost Time: " + ((end - start) / 1000) + " sec.");
	}

	public static void main(String args[]){
		DeleteSample deleteSample = new DeleteSample();
		deleteSample.runDelete();
	}
}
