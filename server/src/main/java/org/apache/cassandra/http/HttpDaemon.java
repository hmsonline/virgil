/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.http;

import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.cassandra.http.index.SolrIndexer;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpDaemon extends CassandraDaemon {
	private static Logger logger = LoggerFactory.getLogger(HttpDaemon.class);
	private static HttpDaemon httpDaemon = null;
	private static IHttpServer http;
	private static CassandraStorage dataService;

	@Override
	public void startServer() {
		super.startServer();
		http.start();
	}

	@Override
	public void stopServer() {
		super.stopServer();
		logger.info("Shutting down HttpDaemon and HttpServer.");
		http.stop();
	}

	@Override
	protected void setup() throws IOException {
		super.setup();
		try {
			SolrIndexer indexer = new SolrIndexer();
			dataService = new CassandraStorage(indexer, new CassandraServer());
			logger.info("Starting server on [" + listenAddr + ":"
					+ VirgilConfig.getListenPort() + "]");
			http = new ApacheCxfHttpServer(VirgilConfig.getBindHost(),
					VirgilConfig.getListenPort(), dataService);
		} catch (Exception wtf) {
			throw new RuntimeException(wtf);
		}
	}

	public static CassandraStorage getDataService() {
		return dataService;
	}

	public static void shutdown() {
		httpDaemon.stopServer();
		httpDaemon.deactivate();
	}

	public static void showUsage() {
		System.out
				.println("Usage: bin/virgil -h CASSANDRA_HOST [-p CASSANDRA_PORT]\n");
		System.out.println("Usage for embedded Cassandra: bin/virgil -e");
		System.exit(-1);
	}

	public static void main(String args[]) {
		if (args.length == 0) {
			HttpDaemon.showUsage();
		} else {
			OptionParser parser = new OptionParser();
			OptionSpec<String> cassandraHost = parser.accepts("host")
					.withRequiredArg().ofType(String.class);
			OptionSpec<String> yaml = parser.accepts("yaml").withRequiredArg()
					.ofType(String.class).defaultsTo("cassandra.yaml");
			OptionSpec<Void> embedCassandra = parser.accepts("embedded");
			OptionSpec<Integer> cassandraPort = parser.accepts("port")
					.withOptionalArg().ofType(Integer.class).defaultsTo(9160);
			OptionSet options = parser.parse(args);

			if (options.has(embedCassandra)) {
				System.out
						.println("Starting virgil with embedded cassandra server.");
				try {
					String yamlFile = yaml.value(options);
					System.setProperty("cassandra.config", yamlFile);
					System.setProperty("cassandra-foreground", "true");
					System.setProperty(VirgilConfig.CASSANDRA_PORT_PROPERTY,
							"9160");
					System.setProperty(VirgilConfig.CASSANDRA_HOST_PROPERTY,
							"localhost");

					httpDaemon = new HttpDaemon();
					httpDaemon.activate();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			} else {
				if (options.hasArgument(cassandraHost)) {
					System.setProperty(VirgilConfig.CASSANDRA_HOST_PROPERTY,
							cassandraHost.value(options));
				} else {
					HttpDaemon.showUsage();
				}
				System.setProperty(VirgilConfig.CASSANDRA_PORT_PROPERTY,
						Integer.toString(cassandraPort.value(options)));

				try {
					String host = VirgilConfig.getCassandraHost();
					Integer port = VirgilConfig.getCassandraPort();
					System.out
							.println("Starting virgil against remote cassandra server ["
									+ host + ":" + port + "]");
					TTransport tr = new TFramedTransport(
							new TSocket(host, port));
					TProtocol proto = new TBinaryProtocol(tr);
					tr.open();
					Cassandra.Client client = new Cassandra.Client(proto);
					SolrIndexer indexer = new SolrIndexer();
					CassandraStorage storage = new CassandraStorage(indexer,
							client);
					IHttpServer server = new ApacheCxfHttpServer(
							VirgilConfig.getBindHost(),
							VirgilConfig.getListenPort(), storage);
					server.start();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
