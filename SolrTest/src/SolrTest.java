import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolrTest {
	public static void main(String[] args) throws Exception {
		//String url = "http://10.1.192.104:8993/solr";
		//String url = "http://10.201.193.93:8983/solr/luwakcollection_shard3_replica1";
		//String url = "http://192.168.56.101:8993/solr/col9_shard3_replica3";
		String url = "http://localhost:8983/solr/collection1";
		DefaultHttpClient httpClient = new DefaultHttpClient();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("kyle");
		//SolrServer server = new ConcurrentUpdateSolrServer(url, httpClient, 1000, 10);
		SolrServer server = new HttpSolrServer(url);

		System.out.println("Start Input...");
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		for (int i = 0; i < 5; i++) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", "test" + i, 1.0f);
			doc.addField("title", "ready" + i, 1.0f);
			docs.add(doc);
		}
		server.add(docs);
		server.commit();
		
		/*
		UpdateRequest req = new UpdateRequest();
		req.setAction(UpdateRequest.ACTION.COMMIT, false, false);
		req.add(docs);
		UpdateResponse rsp1 = req.process(server);
		*/
		
		System.out.println("Start Query...");
		SolrQuery query = new SolrQuery();
		query.setQuery("*:*");
		query.setStart(0);
		query.setRows(10);
		QueryResponse rsp = server.query(query);
		SolrDocumentList docsList = rsp.getResults();
		System.out.println("Num Found: " + docsList.size());
		for (Iterator<SolrDocument> solrDoc = docsList.iterator(); solrDoc.hasNext();) {
			SolrDocument d = solrDoc.next();
			System.out.print(d.getFieldValue("id") + "->");
			System.out.println(d.getFieldValue("fname"));
		}
	}

}