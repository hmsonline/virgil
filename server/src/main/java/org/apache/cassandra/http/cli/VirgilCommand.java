package org.apache.cassandra.http.cli;

import org.apache.cassandra.http.CassandraStorage;
import org.apache.cassandra.http.config.VirgilConfiguration;
import org.apache.cassandra.http.index.SolrIndexer;
import org.apache.cassandra.http.ws.VirgilService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.yammer.dropwizard.AbstractService;
import com.yammer.dropwizard.cli.ServerCommand;

public class VirgilCommand extends ServerCommand<VirgilConfiguration> {
	public VirgilCommand(String name) {
		super(VirgilConfiguration.class);
	}

	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options options = new Options();
		OptionGroup runMode = new OptionGroup();
		
		Option host = OptionBuilder.withArgName("h").hasArg().withDescription("Host name for Cassandra.")
				.create("host");
		Option port = OptionBuilder.withArgName("p").hasArg().withDescription("Port for Cassandra.")
				.create("port");
		Option embedded = OptionBuilder.withArgName("e").withDescription("Run in embedded mode").create("embedded");

		Option yaml = OptionBuilder.withArgName("y").hasArg().withDescription("Cassandra configuration file.")
				.create("yaml");

		runMode.addOption(host);
		runMode.addOption(embedded);
		options.addOptionGroup(runMode);
		
		OptionGroup yamlGroup = new OptionGroup();
		yamlGroup.addOption(yaml);
		options.addOption(port);
		options.addOptionGroup(yamlGroup);
		return options;
	}

	private CassandraStorage createCassandraStorage(CommandLine params, VirgilConfiguration config)
			throws TTransportException {
		SolrIndexer indexer = new SolrIndexer(config);

		if (params.hasOption("embedded")) {
			System.out.println("Starting virgil with embedded cassandra server.");
			String yamlFile = config.getCassandraYaml();
			if (yamlFile == null)
				yamlFile = "cassandra.yaml";
			System.setProperty("cassandra.config", yamlFile);
			System.setProperty("cassandra-foreground", "true");
			System.setProperty(VirgilConfiguration.CASSANDRA_PORT_PROPERTY, "9160");
			System.setProperty(VirgilConfiguration.CASSANDRA_HOST_PROPERTY, "localhost");
			CassandraDaemon.main(null);
			return new CassandraStorage("localhost", 9160, config, indexer, true);
		} else {
			String cassandraHost = params.getOptionValue("host");
			if (cassandraHost == null)
				throw new RuntimeException("Need to specify a host if not running in embedded mode. (-e)");
			System.setProperty(VirgilConfiguration.CASSANDRA_HOST_PROPERTY, cassandraHost);
			String cassandraPort = params.getOptionValue("port");
			if (cassandraPort == null)
				cassandraPort = "9160";
			System.setProperty(VirgilConfiguration.CASSANDRA_PORT_PROPERTY, cassandraPort);
			System.out.println("Starting virgil against remote cassandra server [" + cassandraHost + ":"
					+ cassandraPort + "]");
			return new CassandraStorage(cassandraHost, Integer.parseInt(cassandraPort), config, indexer, false);
		}
	}

	@Override
	protected void run(AbstractService<VirgilConfiguration> service, VirgilConfiguration config, CommandLine params)
			throws Exception {
		assert (service instanceof VirgilService);
		VirgilService virgil = (VirgilService) service;
		CassandraStorage storage = this.createCassandraStorage(params, config);
		virgil.setStorage(storage);
		virgil.setConfig(config);
		super.run(service, config, params);
	}
}
