//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
package io.warp10.plugins.influxdb;

import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.egress.CORSHandler;
import io.warp10.warp.sdk.AbstractWarp10Plugin;

import java.net.URL;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

public class InfluxDBWarp10Plugin extends AbstractWarp10Plugin implements Runnable {
  
  private static final String CONF_INFLUXDB_PORT = "influxdb.port";
  private static final String CONF_INFLUXDB_HOST = "influxdb.host";
  private static final String CONF_INFLUXDB_IDLE_TIMEOUT = "influxdb.idle.timeout";
  private static final String CONF_INFLUXDB_JETTY_THREADPOOL = "influxdb.jetty.threadpool";
  private static final String CONF_INFLUXDB_JETTY_MAXQUEUESIZE = "influxdb.jetty.maxqueuesize";
  private static final String CONF_INFLUXDB_ACCEPTORS = "influxdb.acceptors";
  private static final String CONF_INFLUXDB_SELECTORS = "influxdb.selectors";
  private static final String CONF_INFLUXDB_WARP10_ENDPOINT = "influxdb.warp10.endpoint";
  private static final String CONF_INFLUXDB_DEFAULT_TOKEN = "influxdb.default.token";

  private int port;
  private String host;
  private int idleTimeout;
  private int maxThreads;
  private int acceptors;
  private int selectors;
  private URL url;
  private String token;
  
  private BlockingQueue<Runnable> queue;
  
  @Override
  public void run() {
    Server server = new Server(new QueuedThreadPool(maxThreads, 8, idleTimeout, queue));
    ServerConnector connector = new ServerConnector(server, acceptors, selectors);
    connector.setIdleTimeout(idleTimeout);
    connector.setPort(port);
    connector.setHost(host);
    connector.setName("Continuum Ingress");
    
    server.setConnectors(new Connector[] { connector });

    HandlerList handlers = new HandlerList();
    
    Handler cors = new CORSHandler();
    handlers.addHandler(cors);

    handlers.addHandler(new InfluxDBHandler(url, token));
    
    server.setHandler(handlers);
    
    JettyUtil.setSendServerVersion(server, false);

    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void init(Properties properties) {
    this.acceptors = Integer.parseInt(properties.getProperty(CONF_INFLUXDB_ACCEPTORS, "4"));
    this.selectors = Integer.parseInt(properties.getProperty(CONF_INFLUXDB_SELECTORS, "2"));
    this.maxThreads = Integer.parseInt(properties.getProperty(CONF_INFLUXDB_JETTY_THREADPOOL, Integer.toString(1 + acceptors + acceptors * selectors)));
    this.idleTimeout = Integer.parseInt(properties.getProperty(CONF_INFLUXDB_IDLE_TIMEOUT, "30000"));
    this.port = Integer.parseInt(properties.getProperty(CONF_INFLUXDB_PORT, "8086"));
    this.host = properties.getProperty(CONF_INFLUXDB_HOST, "127.0.0.1");
    this.token = properties.getProperty(CONF_INFLUXDB_DEFAULT_TOKEN);
    
    try {
      this.url = new URL(properties.getProperty(CONF_INFLUXDB_WARP10_ENDPOINT));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    if (properties.containsKey(CONF_INFLUXDB_JETTY_MAXQUEUESIZE)) {
      int queuesize = Integer.parseInt(properties.getProperty(CONF_INFLUXDB_JETTY_MAXQUEUESIZE));
      queue = new BlockingArrayQueue<Runnable>(queuesize);
    }
    
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("[InfluxDBWarp10Plugin " + host + ":" + port + "]");
    t.start();
  }
}
