package org.apache.cassandra.http.ws;

import org.apache.cassandra.http.CassandraStorage;
import org.apache.cassandra.http.cli.VirgilCommand;
import org.apache.cassandra.http.config.VirgilConfiguration;
import org.apache.cassandra.http.resource.DataResource;
import org.apache.cassandra.http.resource.MapReduceResource;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.bundles.AssetsBundle;
import com.yammer.dropwizard.config.Environment;

public class VirgilService extends Service<VirgilConfiguration> {
	public static CassandraStorage storage = null;
	VirgilConfiguration config = null;
	
	public static void main(String[] args) throws Exception {
		new VirgilService().run(args);
	}

	protected VirgilService() {
		super("cirrus");
		addBundle(new AssetsBundle("/ui", "/"));
		addCommand(new VirgilCommand("cassandra"));
	}

	@Override
	protected void initialize(VirgilConfiguration conf, Environment env)
			throws Exception {		
        env.addResource(new MapReduceResource(this));
		env.addResource(new DataResource(this));
	}

	public CassandraStorage getStorage() {
		return storage;
	}

	public void setStorage(CassandraStorage storage) {
		VirgilService.storage = storage;
	}

	public VirgilConfiguration getConfig() {
		return config;
	}

	public void setConfig(VirgilConfiguration config) {
		this.config = config;
	}	
}
