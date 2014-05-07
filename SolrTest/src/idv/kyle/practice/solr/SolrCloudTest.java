package idv.kyle.practice.solr;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;

public class SolrCloudTest {
	public static void main(String[] args) throws Exception {
		SolrCloudTest instance = new SolrCloudTest();
		Date startDt = new Date();
		instance.addIndex();
		Date endDt = new Date();
		System.out.println("Costed mili seconds: " + (endDt.getTime() - startDt.getTime()));
	}
	public  void addIndex() throws Exception {
		//String url = "10.201.193.93:9983,10.201.193.94:9983,10.201.193.95:9983";
		String url = "192.168.56.101:9983,192.168.56.101:9993,192.168.56.101:10003";
		CloudSolrServer cloudSolrServer = new CloudSolrServer(url);
		//cloudSolrServer.setDefaultCollection("luwakcollection");
		cloudSolrServer.setDefaultCollection("c26");
		cloudSolrServer.setZkClientTimeout(20000);
		cloudSolrServer.setZkConnectTimeout(1000);
		cloudSolrServer.connect();
		ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();
		ClusterState clusterState = zkStateReader.getClusterState();
		System.out.println("clusterState: " + clusterState);

		System.out.println("Start Input...");
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		for (int i = 32001; i <= 62000; i++) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", "newid-" + i, 1.0f);
			doc.addField("name", "name-" + i, 1.0f);
			docs.add(doc);
		}
		cloudSolrServer.add(docs);
		cloudSolrServer.commit();
	}

}