package idv.kyle.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class PageFilterSample {
	static Logger LOG = Logger.getLogger(PageFilterSample.class.getName());
	
	public PageFilterSample() throws IOException {
	}
	
	public void queryHTable() {
		try{
			String tableName = "table36";
			String cf = "cf";
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "host3");
			config.set("hbase.zookeeper.property.clientPort", "2181");
			
			HTablePool pool = new HTablePool(config, 8);
			HTableInterface htable = pool.getTable(tableName);
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(cf));
			PageFilter rowFilter = new PageFilter(10);
			scan.setFilter(rowFilter);
			ResultScanner resultScanner = htable.getScanner(scan);
			for(Result result:resultScanner){
				LOG.info("row:" + new String(result.getRow()));
				LOG.info(result.toString());
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) throws IOException {
		PageFilterSample getSample = new PageFilterSample();
		getSample.queryHTable();
	}
}
