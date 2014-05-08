package idv.kyle.practice.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class BulkIndex {
	public static void main(String[] args) throws IOException{
		BulkIndex instance = new BulkIndex();
		Date startDt = new Date();
		instance.addIndexToCluster();
		Date endDt = new Date();
		System.out.println("Costed mili seconds: " + (endDt.getTime() - startDt.getTime()));
	}
	
	public void addIndexToCluster() throws IOException {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "escluster").build();
		Client client = new TransportClient(settings)
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.101", 9300))
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.101", 9301))
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.101", 9302))
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.101", 9303))
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.101", 9304));
		
		BulkRequestBuilder bulkRequest = client.prepareBulk();

		for (int i = 20002001; i <= 20010000; i++) {
			bulkRequest.add(client.prepareIndex("user50", "test", "" + i)
					.setSource(
							jsonBuilder().startObject()
									.field("user", "user" + i)
									.field("location", "location" + i)
									.field("id", "id" + i)
									.field("name", "name" + i).endObject()));
		}

		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
		    // process failures by iterating through each bulk response item
			System.out.println("Bulk Import has error......");
		}

		// on shutdown
		client.close();
	}
}
