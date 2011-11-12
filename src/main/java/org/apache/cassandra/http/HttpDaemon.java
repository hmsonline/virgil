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
import java.net.URL;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.http.index.SolrIndexer;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpDaemon extends CassandraDaemon
{
    private static Logger logger = LoggerFactory.getLogger(HttpDaemon.class);
    private static HttpDaemon httpDaemon = null;
    private static IHttpServer http;
    private static CassandraStorage dataService;

    @Override
    public void startServer()
    {
    	super.startServer();
    	http.start();
    }

    @Override
    public void stopServer()
    {
    	super.stopServer();
    	logger.info("Shutting down HttpDaemon and HttpServer.");
        http.stop();
    }

    @Override
    protected void setup() throws IOException
    {
        super.setup();
        try
        {
            SolrIndexer indexer = new SolrIndexer();
            dataService = new CassandraStorage(indexer);
            logger.info("Starting server on [" + listenAddr + ":" + VirgilConfig.getListenPort() + "]");
            http = new ApacheCxfHttpServer(this.listenAddr.getHostName(), VirgilConfig.getListenPort(), dataService);

        }
        catch (Exception wtf)
        {
            throw new RuntimeException(wtf);
        }
    }

    public static CassandraStorage getDataService()
    {
        return dataService;
    }
    
    public static void shutdown()
    {
        httpDaemon.stopServer();
        httpDaemon.deactivate();
    }

    public static void main(String args[])
    {
    	// TDOD need to fix this so the "cassandra.yaml" file can live in a "conf" directory of a distribution
    	// we should not be loading from a classpath resource (i.e. the user can't easily edit the config)
        String CONFIG_URL = args[0];//"cassandra.yaml";
        ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
        URL url = loader.getResource(CONFIG_URL);
        try {
            url.openStream();
            System.setProperty("cassandra.config", CONFIG_URL);
            System.setProperty("cassandra-foreground", "true");
            httpDaemon = new HttpDaemon();
            httpDaemon.activate();      
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
