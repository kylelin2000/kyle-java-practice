import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class ConcurrentSolrTest {
	public static void main(String[] args) throws Exception {
		//String url = "http://10.1.192.104:8993/solr";
		//String url = "http://10.201.193.93:8983/solr/luwakcollection_shard3_replica1";
		String url = "http://192.168.56.101:8983/solr/col2";

		//SolrServer server = new HttpSolrServer(url);
		
		DefaultHttpClient httpClient = new DefaultHttpClient();							
		Credentials credentials = new UsernamePasswordCredentials("kyle");
		httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, credentials);
		SolrServer server = new ConcurrentUpdateSolrServer(url, httpClient, 1000, 32);

		System.out.println("Start Input...");
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		for (int i = 0; i < 3; i++) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", "let" + i, 1.0f);
			doc.addField("title", "go" + i, 1.0f);
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
			System.out.println(d.getFieldValue("title"));
		}
	}

}