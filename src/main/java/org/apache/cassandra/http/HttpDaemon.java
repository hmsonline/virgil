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
import org.apache.cassandra.service.AbstractCassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpDaemon extends AbstractCassandraDaemon
{
    private static Logger logger = LoggerFactory.getLogger(HttpDaemon.class);

    private IHttpServer http;
    private static CassandraStorage dataService;

    @Override
    public void startServer()
    {
        http.start();
    }

    @Override
    public void stopServer()
    {
        http.start();
    }

    @Override
    protected void setup() throws IOException
    {
        super.setup();
        try
        {
            listenPort = 8080;
            dataService = new CassandraStorage();
            logger.info("Starting server on [" + listenAddr + ":" + listenPort + "]");
            http = new ApacheCxfHttpServer(this.listenAddr.getHostName(), this.listenPort, dataService);
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

    public static void main(String args[])
    {
        String CONFIG_URL = "cassandra.yaml";
        ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
        URL url = loader.getResource(CONFIG_URL);
        try {
            url.openStream();
            System.setProperty("cassandra.config", CONFIG_URL);
            new HttpDaemon().activate();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
