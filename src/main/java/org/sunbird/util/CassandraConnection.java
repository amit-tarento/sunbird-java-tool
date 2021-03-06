package org.sunbird.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import org.apache.commons.lang3.StringUtils;

public class CassandraConnection {

  private Cluster cluster;

  private Session session;

  private static CassandraConnection instance;

  public static CassandraConnection getInstance(String cassandraIp) {
    if (null == instance) {
      instance = new CassandraConnection(cassandraIp);
      return instance;
    } else {
      return instance;
    }
  }

  private CassandraConnection(String cassandraIp) {
    connect(cassandraIp);
  }

  public void connect(String nodes) {
    String[] hosts =  null;
    if (StringUtils.isNotBlank(nodes)) {
      hosts =  nodes.split(",");
    } else {
      hosts = new String[] { "localhost" };
    }
    cluster = Cluster.builder().addContactPoints(hosts)
        .withRetryPolicy(DefaultRetryPolicy.INSTANCE).build();
    session = cluster.connect();
  }

  public Session getSession() {
    return this.session;
  }

  public void close() {
    session.close();
    cluster.close();
  }
  
}


