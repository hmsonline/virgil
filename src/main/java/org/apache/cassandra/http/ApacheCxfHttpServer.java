/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.http;

import org.apache.cxf.endpoint.Server;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.lifecycle.SingletonResourceProvider;
import org.apache.cxf.transport.Destination;
import org.apache.cxf.transport.http_jetty.JettyHTTPDestination;
import org.apache.cxf.transport.http_jetty.JettyHTTPServerEngine;
import org.apache.cxf.transport.http_jetty.ServerEngine;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;


public class ApacheCxfHttpServer implements IHttpServer
{
    JAXRSServerFactoryBean sf = null;

    public ApacheCxfHttpServer(String host, int port, CassandraStorage cassandraStorage) throws Exception
    {
        sf = new JAXRSServerFactoryBean();
        sf.setResourceClasses(CassandraRestService.class);
        sf.setResourceProvider(CassandraRestService.class,
                new SingletonResourceProvider(new CassandraRestService(cassandraStorage)));
        sf.setAddress("http://" + host + ":" + port + "/");
        Server cxfServer = sf.create();

        // Add static content using this:
        // http://cxf.apache.org/docs/standalone-http-transport.html
        this.addStaticContent(sf, cxfServer);
    }

    public void addStaticContent(JAXRSServerFactoryBean serviceFactory, Server cxfServer) throws Exception{
    	Destination dest = cxfServer.getDestination(); 
        JettyHTTPDestination jettyDestination = JettyHTTPDestination.class.cast(dest); 
        ServerEngine engine = jettyDestination.getEngine(); 
        JettyHTTPServerEngine serverEngine = JettyHTTPServerEngine.class.cast(engine); 
        org.eclipse.jetty.server.Server httpServer = serverEngine.getServer(); 
        
        // Had to start the server to get the Jetty Server instance. 
        // Have to stop it to add the custom Jetty handler. 
        httpServer.stop(); 
        httpServer.join(); 
        
        Handler[] existingHandlers = httpServer.getHandlers(); 
        
        ResourceHandler resourceHandler = new ResourceHandler(); 
        resourceHandler.setDirectoriesListed(true); 
        resourceHandler.setWelcomeFiles(new String[] {"index.html"}); 
        resourceHandler.setResourceBase("./src/main/webapp/"); 

        HandlerList handlers = new HandlerList(); 
        handlers.addHandler(resourceHandler); 
        if (existingHandlers != null) { 
                for (Handler h : existingHandlers) { 
                        handlers.addHandler(h); 
                } 
        }	
        httpServer.setHandler(handlers); 

        httpServer.start(); 
        System.out.println("Started..."); 
    }
    
    public void start()
    {
        try
        {
            sf.create();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void stop()
    {

    }
}
