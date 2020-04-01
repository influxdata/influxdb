//
//   Copyright 2019  SenX S.A.S.
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

package io.warp10;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import io.warp10.continuum.Configuration;

public class SSLUtils {
  
  private static final String DEFAULT_SSL_ACCEPTORS = "2";
  private static final String DEFAULT_SSL_SELECTORS = "4";
  
  public static ServerConnector getConnector(Server server, String prefix) {
    int sslAcceptors = Integer.parseInt(WarpConfig.getProperty(prefix + Configuration._SSL_ACCEPTORS, DEFAULT_SSL_ACCEPTORS));
    int sslSelectors = Integer.parseInt(WarpConfig.getProperty(prefix + Configuration._SSL_SELECTORS, DEFAULT_SSL_SELECTORS));

    int sslPort = Integer.parseInt(WarpConfig.getProperty(prefix + Configuration._SSL_PORT));
    String sslHost = WarpConfig.getProperty(prefix + Configuration._SSL_HOST);
    int sslTcpBacklog = Integer.parseInt(WarpConfig.getProperty(prefix + Configuration._SSL_TCP_BACKLOG, "0"));

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(WarpConfig.getProperty(prefix + Configuration._SSL_KEYSTORE_PATH));
    sslContextFactory.setCertAlias(WarpConfig.getProperty(prefix + Configuration._SSL_CERT_ALIAS));
    
    if (null != WarpConfig.getProperty(prefix + Configuration._SSL_KEYSTORE_PASSWORD)) {
      sslContextFactory.setKeyStorePassword(WarpConfig.getProperty(prefix + Configuration._SSL_KEYSTORE_PASSWORD));
    }
    if (null != WarpConfig.getProperty(prefix + Configuration._SSL_KEYMANAGER_PASSWORD)) {
      sslContextFactory.setKeyManagerPassword(WarpConfig.getProperty(prefix + Configuration._SSL_KEYMANAGER_PASSWORD));
    }

    ServerConnector connector = new ServerConnector(server, sslAcceptors, sslSelectors, sslContextFactory);
    
    connector.setPort(sslPort);
    connector.setAcceptQueueSize(sslTcpBacklog);
    
    if (null != sslHost) {
      connector.setHost(sslHost);
    }
    
    String idle = WarpConfig.getProperty(prefix + Configuration._SSL_IDLE_TIMEOUT);
    
    if (null != idle) {
      connector.setIdleTimeout(Long.parseLong(idle));
    }

    return connector;
  }
}
