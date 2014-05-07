package idv.kyle.practice.elasticsearch;
import java.io.IOException;
import java.util.Date;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class ImportIndex {
	public static void main(String[] args) throws ElasticsearchException,
			IOException {
		ImportIndex instance = new ImportIndex();
		Date startDt = new Date();
		instance.addIndexToCluster();
		Date endDt = new Date();
		System.out.println("Costed mili seconds: "
				+ (endDt.getTime() - startDt.getTime()));
	}

	public void addIndexToNode() throws ElasticsearchException, IOException {
		// on startup
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "escluster").build();
		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						"192.168.56.102", 9300));

		IndexResponse response = client
				.prepareIndex("twitter", "tweet", "2")
				.setSource(
						jsonBuilder().startObject().field("user", "kent")
								.field("postDate", new Date())
								.field("message", "trying out Elasticsearch")
								.endObject()).execute().actionGet();

		// on shutdown
		client.close();
	}

	public void addIndexToCluster() throws ElasticsearchException, IOException {
		// on startup
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "escluster").build();
		Client client = new TransportClient(settings)
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.102", 9300))
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.102", 9301))
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.102", 9302))
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.102", 9303))
				.addTransportAddress(
						new InetSocketTransportAddress("192.168.56.102", 9304));

		for (int i = 79; i <= 93; i++) {
			IndexResponse response = client
					.prepareIndex("user18", "location", "" + i)
					.setSource(
							jsonBuilder().startObject().field("user", "user" + i).field("city", "Madrid").field("sex", "female").endObject())
					.execute().actionGet();
		}

		// on shutdown
		client.close();
	}
}
