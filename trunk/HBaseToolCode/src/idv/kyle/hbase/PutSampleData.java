package idv.kyle.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutSampleData {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hdp2-n1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-secure");

		String tableName = "test";
		String columnFamily = "cf";

		HBaseAdmin admin = new HBaseAdmin(conf);
		System.out.println("init");
		if (!admin.tableExists(tableName)) {
			System.out.println("init to create table.");
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}

		System.out.println("init to put data");
		HTable testTable = new HTable(conf, tableName);
		testTable.setAutoFlush(false, true);
		for (int i = 0; i < 10; i++) {
			Put put = new Put(Bytes.toBytes("rowKey_" + i));
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes("name" + i));
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("phone"), Bytes.toBytes("phone" + i));
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("city"), Bytes.toBytes("city" + i));
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("address"), Bytes.toBytes("address" + i));
			testTable.put(put);
		}
		testTable.flushCommits();

		testTable.close();
		System.out.println("finished.");
	}
}
