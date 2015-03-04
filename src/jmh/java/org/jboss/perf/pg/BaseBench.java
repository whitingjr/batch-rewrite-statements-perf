package org.jboss.perf.pg;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.sun.xml.internal.fastinfoset.sax.Properties;

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
   
   public void tearDown()
   {
      ((org.apache.tomcat.jdbc.pool.DataSource) ds).close();
   }

   public void loadProperties()
   {
      Properties props = new Properties();
      props.load( new BufferedInputStream(  ClassLoader.getSystemResourceAsStream("bench.properties") ) );
      
   }
}
