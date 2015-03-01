package org.jboss.perf.pg;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.tomcat.jdbc.pool.PoolProperties;


public class BaseBench {

   protected static final int MIN_POOL_SIZE = 1;
   
   protected static volatile DataSource ds;
   
   public void createPool()
   {
      try {
         PoolProperties props = new PoolProperties();
         props.setUrl("jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=false&prepareThreshold=1&tcpKeepAlive=true");
         props.setDriverClassName("org.apache.tomcat.jdbc.pool.DataSource");
         props.setUsername(System.getProperty("user"));
         props.setPassword(System.getProperty("password"));
         props.setInitialSize(MIN_POOL_SIZE);
         props.setMinIdle(MIN_POOL_SIZE);
         String max = System.getProperty("maxPoolSize");
         props.setMaxIdle(Integer.valueOf(max).intValue());
         props.setMaxActive(Integer.valueOf(max).intValue());
         props.setMaxWait(8000);
         props.setDefaultAutoCommit(false);
         props.setRollbackOnReturn(true);
         props.setMinEvictableIdleTimeMillis((int) TimeUnit.MINUTES.toMillis(30));
         props.setTestOnBorrow(true);
         props.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         props.setValidationQuery("VALUES 1");
         props.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.StatementCache(prepared=true,max=16)");
         ds = new org.apache.tomcat.jdbc.pool.DataSource(props);
      }
      catch (Exception e)
      {
         throw new RuntimeException(e.getMessage());
      }
   }
   
   
//   Statement s = connection.createStatement();
//   s.execute("drop table orderline if exists");
//   s.close();
//   connection.createStatement();
//   s.execute("create table orderline ( orderLineId bigint not null, description varchar (20) );");

}
