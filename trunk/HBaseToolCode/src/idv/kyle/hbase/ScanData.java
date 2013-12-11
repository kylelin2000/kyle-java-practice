package idv.kyle.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class ScanData {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hdp2-n1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-secure");

		String tableName = "ambarismoketest";
		String columnFamily = "family";

        HTable table = new HTable(conf, tableName);
        Scan s = new Scan();
        ResultScanner ss = table.getScanner(s);
        for(Result r:ss){
            for(KeyValue kv : r.raw()){
               System.out.print(new String(kv.getRow()) + " ");
               System.out.print(new String(kv.getFamily()) + ":");
               System.out.print(new String(kv.getQualifier()) + " ");
               System.out.print(kv.getTimestamp() + " ");
               System.out.println(new String(kv.getValue()));
            }
        }
	}
}
