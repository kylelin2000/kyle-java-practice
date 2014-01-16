package idv.kyle.hbase;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

public class TableDeployInfo {
	static Logger LOG = Logger.getLogger(TableDeployInfo.class.getName());

	public void getTableDeployInfo() throws Exception {
		String tableName = "user";

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "host3");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		HTable table = new HTable(conf, tableName);
		Map<HRegionInfo, ServerName> regions = table.getRegionLocations();

		for (Map.Entry<HRegionInfo, ServerName> hriEntry : regions.entrySet()) {
			HRegionInfo regionInfo = hriEntry.getKey();
			ServerName addr = hriEntry.getValue();

			String urlRegionServer = null;

			if (addr != null) {
				urlRegionServer = "http://" + addr.getHostname().toString() + ":60030/";
			}

			LOG.info("region name:" + regionInfo.getRegionName());

			if (urlRegionServer != null) {
				LOG.info("region server name:" + urlRegionServer);
				LOG.info("region server:" + addr.getHostname().toString());
			} else {
				LOG.info("not deployed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			}
		}
	}

	public static void main(String args[]) throws Exception {
		TableDeployInfo info = new TableDeployInfo();
		info.getTableDeployInfo();
	}
}
