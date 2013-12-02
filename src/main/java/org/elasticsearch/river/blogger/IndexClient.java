package org.elasticsearch.river.blogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import net.sf.json.JSONObject;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class IndexClient {

	protected HashMap<String, String> queryMap = new HashMap<String, String>();

	protected Node node;
	protected Client IndexClient;
	protected UnresolvedAddress clusterAddress;
	protected boolean useJson = false;
	protected String clusterName = "BloggerCluster";
	protected boolean useCluster = true;

	protected String clusterServer = "127.0.0.1";
	protected int clusterPort = 9300;

	protected DefaultHttpClient httpClient;
	protected Properties props;
	protected boolean ownsClient;
	protected boolean autoOptimizePercs;

	protected AtomicLong outstandingAsyncRequests = new AtomicLong();

	private static class UnresolvedAddress {

		public UnresolvedAddress(String host, int port) {
			this.host = host;
			this.port = port;
		}

		public String host;
		public int port;
	}

	public void initClient() {
		if (useCluster) {
			clusterAddress = new UnresolvedAddress(clusterServer, clusterPort);
		} else {
			clusterAddress = new UnresolvedAddress("localhost", 9200);
		}
	}

	public void initiateConnection() {
		if (IndexClient == null) {
			// only need node to make clients
			if (useCluster) {
				Settings settings = ImmutableSettings.settingsBuilder()
						.put("cluster.name", clusterName)
						.put("client.transport.sniff", true)
						.put("client.transport.nodes_sampler_interval", "60s")
						.put("client.transport.ping_timeout", "30s").build();
				IndexClient = new TransportClient(settings)
						.addTransportAddress(new InetSocketTransportAddress(
								clusterAddress.host, clusterAddress.port));
			} else {
				if (node == null) {
					node = NodeBuilder.nodeBuilder().node().start();
				}
				IndexClient = node.client();
			}
			ownsClient = true;
		}
		if (useJson) {
			httpClient = new DefaultHttpClient();
		}
	}

	public boolean ensureIndex(String index, boolean recreate) {
		return ensureIndex(index, recreate, null, 1, 1);
	}

	public boolean ensureIndex(String index, boolean recreate,
			XContentBuilder settings, int numShards, int numReplicas) {
		boolean retval = true;
		try {
			boolean exists = IndexClient.admin().indices()
					.exists(new IndicesExistsRequest(index)).actionGet()
					.isExists();
			if (exists && recreate) {
				try {
					GetRequest getRequest = new GetRequest(index);
					System.out.println("index " + getRequest.index());
					System.out.println("type " + getRequest.type());
					System.out.println("id " + getRequest.id());
					// GetResponse getResponse =
					// client.get(getRequest).actionGet();
					// System.out.println("Index" + getResponse.getIndex());
				} catch (Exception e) {
					e.printStackTrace();
				}

				System.out.println("Previous index exists, will remove...");
				IndexClient.admin().indices()
						.delete(new DeleteIndexRequest(index)).actionGet();
				exists = false;
			}
			if (!exists) {
				if (settings == null) {
					settings = XContentFactory
							.jsonBuilder()
							.startObject()
							.startObject("analysis")
							.startObject("analyzer")

							// facet analyzer
							.startObject("facetAnalyzer")
							.field("type", "custom")
							.field("tokenizer", "facetTokenizer")
							.endObject()

							// search analyzer
							.startObject("standard")
							// aliasing for now, though may make sense to split
							// up
							.array("alias", "searchall")
							.field("tokenizer", "standard")
							.array("filter", "standard", "lowercase",
									"snowball", "stop").endObject()

							.endObject().startObject("tokenizer")
							.startObject("facetTokenizer")
							.field("type", "pattern")
							.field("pattern", "[|\\s]").endObject().endObject()
							.endObject();

					// settings.field("index.refresh_interval", 1);
					settings.field("index.merge.policy.merge_factor", 30);
					settings.field("index.number_of_shards", numShards);
					settings.field("index.number_of_replicas", numReplicas);
				}

				System.out.println("Creating index...");
				IndexClient
						.admin()
						.indices()
						.create(new CreateIndexRequest(index)
								.settings(settings)).actionGet();
				System.out.println("Complete.");
			}

		} catch (Exception ex) {
			retval = false;
			ex.printStackTrace();
		}
		return retval;
	}

	public void index(String index, String type, XContentBuilder docBuilder) {
		IndexClient.prepareIndex(index, type).setOperationThreaded(false)
				.setListenerThreaded(false).setSource(docBuilder).execute()
				.actionGet();
	}

	public String insertTextPercolateQuery(String index, String value) {
		return insertTextPercolateQuery(index, "_all", value);
	}

	public String insertTextPercolateQuery(String index, String field,
			String value) {
		return insertTextPercolateQuery(index, field, value, null);
	}

	public String insertTextPercolateQuery(String index, String field,
			String value, String id) {
		System.out.print("Inserting percolate text query for " + field + "="
				+ value + ": ");
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder()
					.startObject().startObject("query").startObject("term")
					.field(field, value);
			System.out.println(builder.string());
			id = insertPercolateQuery(index, builder, id);
		} catch (Exception ex) {
			System.out.println();
			ex.printStackTrace();
		}
		return id;
	}

	public String insertPercolateQuery(String index,
			XContentBuilder requestContent) {
		return insertPercolateQuery(index, requestContent, null);
	}

	public String insertPercolateQuery(String index,
			XContentBuilder requestContent, String id) {
		try {
			if (useJson) {
				HttpEntityEnclosingRequestBase request = null;
				if (id == null) {
					request = new HttpPost("http://" + clusterServer + ":"
							+ clusterPort + "/_percolator/" + index + "/");
				} else {
					request = new HttpPut("http://" + clusterServer + ":"
							+ clusterPort + "/_percolator/" + index + "/" + id);
				}
				StringEntity strEntity = new StringEntity(
						requestContent.string());
				request.setHeader(new BasicHeader(HTTP.CONTENT_TYPE,
						"application/json;charset=UTF-8"));
				request.setEntity(strEntity);
				System.out.println("Sending: " + requestContent.string());
				HttpResponse response = httpClient.execute(request);
				HttpEntity respEntity = response.getEntity();
				if (respEntity != null) {
					String str = EntityUtils.toString(respEntity);
					if (str != null) {
						JSONObject jobj = JSONObject.fromObject(str);
						id = jobj.getString("_id");
					}
				}
			} else {
				// long ticId = Instrument.tic("indexQuery");
				IndexResponse response = IndexClient
						.prepareIndex("_percolator", index)
						.setSource(requestContent).setId(id)
						.setOperationThreaded(false).execute().actionGet();
				// Instrument.toc("indexQuery", ticId);
				id = response.getId();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return id;
	}

	public List<String> insertTextPercolateQueries(String index,
			List<String> values, boolean bulkMode) {
		return insertTextPercolateQueries(index, "_all", values, bulkMode);
	}

	public List<String> insertTextPercolateQueries(String index, String field,
			List<String> values, boolean bulkMode) {
		List<String> idList = null;
		if (bulkMode) {
			throw new UnsupportedOperationException();
		} else {
			idList = new ArrayList<String>(values.size());
			for (String value : values) {
				idList.add(insertTextPercolateQuery(index, field, value));
			}
		}
		return idList;
	}

}
