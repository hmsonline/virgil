package org.apache.virgil.ws;

import org.apache.virgil.CassandraStorage;
import org.apache.virgil.cli.VirgilCommand;
import org.apache.virgil.config.VirgilConfiguration;
import org.apache.virgil.health.CassandraHealthCheck;
import org.apache.virgil.resource.DataResource;
import org.apache.virgil.resource.MapReduceResource;

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
    protected void initialize(VirgilConfiguration conf, Environment env) throws Exception {
        env.addResource(new MapReduceResource(this));
        env.addResource(new DataResource(this));
        env.addHealthCheck(new CassandraHealthCheck(this));
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
