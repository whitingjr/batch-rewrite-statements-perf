package org.jboss.perf.pg;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.tomcat.jdbc.pool.PoolProperties;

public class BaseBench {

   protected static final int MIN_POOL_SIZE = 1;
   
   protected static volatile DataSource ds;
   private static Logger logger = Logger.getLogger(BaseBench.class.getName());
   
   public void createPool()
   {
      try 
      {
         PoolProperties props = new PoolProperties();
         props.setUrl(String.format("jdbc:postgresql://%1$s/%2$s?ssl=false&sslmode=disable&prepareThreshold=1&tcpKeepAlive=%3$s", System.getProperty("host"), System.getProperty("database"), System.getProperty("keepalive")));
         props.setDriverClassName("org.postgresql.Driver");
         props.setUsername(System.getProperty("user"));
         props.setPassword(System.getProperty("password"));
         props.setInitialSize(MIN_POOL_SIZE);
         props.setMinIdle(MIN_POOL_SIZE);
         String max = System.getProperty("maxPoolSize");
         props.setMaxIdle(Integer.valueOf(max).intValue());
         props.setMaxActive(Integer.valueOf(max).intValue());
         props.setMaxWait(8000);
         props.setDefaultAutoCommit(false);
         props.setRollbackOnReturn(false);
         props.setMinEvictableIdleTimeMillis((int) TimeUnit.MINUTES.toMillis(30));
         props.setTestOnBorrow(true);
         props.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         props.setValidationQuery("SELECT 1");
         props.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.StatementCache(prepared=true,max=8)");
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

   /**
    * Properties loading from configuration file. Properties passed to jvm will
    *  override configuration file settings.
    */
   public static void loadProperties()
   {
      Properties props = new Properties();
      try {
         props.load( new BufferedInputStream(  ClassLoader.getSystemResourceAsStream("bench.properties") ) );
      } catch (IOException ioe)
      {
         logger.log(Level.SEVERE, ioe.getMessage());
      }
      props.putAll(System.getProperties()); //overwrite
      System.getProperties().putAll(props);
   }
}
