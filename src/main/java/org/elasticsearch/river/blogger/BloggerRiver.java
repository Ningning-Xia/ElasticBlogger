/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.blogger;

import java.util.Map;


import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;


/**
 * 
 * @author Ningning Xia
 */
public class BloggerRiver extends AbstractRiverComponent implements River {

	public final String defaultUser = "user";
	public final String defaultPassword = "password";
	public final String defaultContextFactory = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
	public final String defaultConnectionFactoryName = "ConnectionFactory";
	public final String defaultProviderUrl = "vm://localhost";
	public static final String defaultSourceType = "queue"; // topic
	public static final String defaultSourceName = "elasticsearch";
	public final String defaultConsumerName;
	public final boolean defaultCreateDurableConsumer = false;
	public final String defaultTopicFilterExpression = "";

	
	private String user;
	private String password;
	private String providerUrl;
	private String sourceType;
	private String sourceName;

	private final Client client;

	private final int bulkSize;
	private final TimeValue bulkTimeout;
	private final boolean ordered;

	private volatile boolean closed = false;

	private volatile Thread thread;

	private IndexClient indexClient;

	@SuppressWarnings({ "unchecked" })
	@Inject
	public BloggerRiver(RiverName riverName, RiverSettings settings,
			Client client) {
		super(riverName, settings);
		this.client = client;
		this.defaultConsumerName = "blogger_elasticsearch_river_"
				+ riverName().name();

		if (settings.settings().containsKey("jms")) {
			Map<String, Object> jmsSettings = (Map<String, Object>) settings
					.settings().get("blogger");
			user = XContentMapValues.nodeStringValue(jmsSettings.get("user"),
					defaultUser);
			password = XContentMapValues.nodeStringValue(
					jmsSettings.get("pass"), defaultPassword);
		} else {
			user = defaultUser;
			password = defaultPassword;
		}

		if (settings.settings().containsKey("index")) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings
					.settings().get("index");
			bulkSize = XContentMapValues.nodeIntegerValue(
					indexSettings.get("bulkSize"), 100);

			if (indexSettings.containsKey("bulkTimeout")) {
				bulkTimeout = TimeValue.parseTimeValue(
						XContentMapValues.nodeStringValue(
								indexSettings.get("bulkTimeout"), "10s"),
						TimeValue.timeValueMillis(10000));
			} else {
				bulkTimeout = TimeValue.timeValueMillis(10);
			}

			ordered = XContentMapValues.nodeBooleanValue(
					indexSettings.get("ordered"), false);
		} else {
			bulkSize = 100;
			bulkTimeout = TimeValue.timeValueMillis(10);
			ordered = false;
		}
	}
	
	

	@Override
	public void start() {
		logger.info(
				"Creating an blogger river: user [{}], broker [{}], sourceType [{}], sourceName [{}]",
				user, providerUrl, sourceType, sourceName);

		thread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
				"blogger_river").newThread(new Consumer());
		thread.start();
	}

	@Override
	public void close() {
		if (closed) {
			return;
		}

		if (indexClient.node != null) {
			indexClient.node.close();
		}

		if (indexClient.httpClient != null) {
			indexClient.httpClient.getConnectionManager().shutdown();
		}

		logger.info("Closing the Blogger river");
		closed = true;
		thread.interrupt();
	}

	private class Consumer implements Runnable {
		@Override
		public void run() {
			while (true) {
				if (closed) {
					break;
				}
				
				boolean recreateIndex = true;
				indexClient = new IndexClient();
				indexClient.initClient();
				indexClient.initiateConnection();
				indexClient.ensureIndex("blogger", recreateIndex);
				testPrint();
			}

		}
	}

	private void testPrint() {
		for (int i = 0; i < 10; i++) {
			System.out.println("TESTPRINT " + i);
		}

		// BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		// IndexRequest request = new IndexRequest();
		// bulkRequestBuilder.add(request);
		
		// make the query
		indexClient.insertTextPercolateQuery("blogger", "jason");
		indexClient.insertTextPercolateQuery("blogger", "content", "nyse");
		indexClient.insertTextPercolateQuery("blogger", "content", "google");
		indexClient.insertTextPercolateQuery("blogger", "content", "apple");
		closed = true;
	}
	
}
