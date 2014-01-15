import java.util.ArrayList;
import java.util.Collection;
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
		//String url = "10.201.193.93:9983,10.201.193.94:9983,10.201.193.95:9983";
		String url = "192.168.56.101:9983,192.168.56.101:9993,192.168.56.101:10003";
		CloudSolrServer cloudSolrServer = new CloudSolrServer(url);
		//cloudSolrServer.setDefaultCollection("luwakcollection");
		cloudSolrServer.setDefaultCollection("c8");
		cloudSolrServer.setZkClientTimeout(20000);
		cloudSolrServer.setZkConnectTimeout(1000);
		cloudSolrServer.connect();
		ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();
		ClusterState clusterState = zkStateReader.getClusterState();
		System.out.println("clusterState: " + clusterState);

		System.out.println("Start Input...");
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		for (int i = 1; i < 4; i++) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", "pencil " + i, 1.0f);
			doc.addField("title", "cup " + i, 1.0f);
			doc.addField("comments", "paper " + i, 1.0f);
			doc.addField("cat", "light " + i, 1.0f);
			docs.add(doc);
		}
		cloudSolrServer.add(docs);
		cloudSolrServer.commit();
		
		System.out.println("Start Query...");
		SolrQuery query = new SolrQuery();
		query.setQuery("*:*");
		query.setStart(0);
		query.setRows(100);
		QueryResponse rsp = cloudSolrServer.query(query);
		SolrDocumentList docsList = rsp.getResults();
		System.out.println("Num Found: " + docsList.size());
		for (Iterator<SolrDocument> solrDoc = docsList.iterator(); solrDoc.hasNext();) {
			SolrDocument d = solrDoc.next();
			System.out.print(d.getFieldValue("id") + "->");
			System.out.println(d.getFieldValue("title"));
		}
	}

}