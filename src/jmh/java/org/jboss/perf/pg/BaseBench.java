package org.jboss.perf.pg;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.jboss.perf.BatchSQLEnum;

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
   
   /**
    * Base class for state that is associated with the Thread.
    * @author whitingjr
    *
    */
   static class BaseThreadState
   {
      volatile Connection conn;

      public void setUp() throws SQLException
      {
         this.conn = ds.getConnection();
         this.conn.setAutoCommit(false);
      }
      public void tearDown() throws SQLException
      {
         this.conn.close();
      }
   }
   
   static abstract class BaseBenchmarkState
   {
      BatchSQLEnum SMALL = BatchSQLEnum.SMALL;
      BatchSQLEnum MEDIUM = BatchSQLEnum.MEDIUM;
      BatchSQLEnum LARGE = BatchSQLEnum.LARGE;
      AtomicInteger iteration = new AtomicInteger();
      
      public void setUpSQL() 
      {
         SMALL.setCount(getSize("small.batch.size"));
         MEDIUM.setCount(getSize("medium.batch.size"));
         LARGE.setCount(getSize("large.batch.size"));
         
         SMALL.setSQL( buildSQL(SMALL) );
         MEDIUM.setSQL( buildSQL(MEDIUM) );
         LARGE.setSQL( buildSQL(LARGE) );
         Connection c = null;
         Statement s = null;
         try 
         {
            c = ds.getConnection();
            c.setAutoCommit(false);
            s = c.createStatement();
            s.execute("DROP TABLE IF EXISTS orderline;");
            DbUtils.closeQuietly( s );
            s = c.createStatement();
            s.execute("CREATE TABLE orderline ( orderLineId bigint NOT NULL, description VARCHAR (20), PRIMARY KEY (orderLineId) );");
            c.commit();
         } catch ( SQLException sqle ) 
         {
            logger.log(java.util.logging.Level.SEVERE, sqle.getMessage());
         }
         finally 
         {
            DbUtils.closeQuietly( s );
            DbUtils.closeQuietly( c );
         }
      }
      /* Delegate the building of the sql to the concrete impl. */
      abstract protected String buildSQL(BatchSQLEnum sql);

      private long getSize(String key)
      {
         return Long.parseLong(System.getProperty(key));
      }
   }
}
