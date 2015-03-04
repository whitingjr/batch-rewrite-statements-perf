package org.jboss.perf.pg;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.dbutils.DbUtils;
import org.jboss.perf.BatchSQLEnum;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class MultiInsertStatementTest extends BaseBench {
   
   @Benchmark
   @Measurement(iterations=100, timeUnit=TimeUnit.MINUTES)
   public void do1MultirowInserts( ThreadState state, MultiRowBenchmarkState benchmarkState )
   {  
      try {
         executeAsBatch(state, benchmarkState.SMALL);
      } catch (SQLException sqle )
      {
         throw new RuntimeException(sqle.getMessage() + " cause :" + sqle.getCause().getMessage());
      }
   }
   
   @Benchmark
   public void do11MultirowInserts( ThreadState state, MultiRowBenchmarkState benchmarkState  )
   {
      try {
         executeAsBatch(state, benchmarkState.MEDIUM);
      } catch (SQLException sqle )
      {
         throw new RuntimeException(sqle.getMessage() + " cause :" + sqle.getCause().getMessage());
      }
   }
   
   @Benchmark
   public void do51MultirowInserts( ThreadState state, MultiRowBenchmarkState benchmarkState )
   {
      try {
         executeAsBatch(state, benchmarkState.LARGE);
      } catch (SQLException sqle )
      {
         throw new RuntimeException(sqle.getMessage() + " cause :" + sqle.getCause().getMessage());
      }
   }

   /** 
    * Benchmark state that is set up once beforehand.
    * @author whitingjr
    */
   @State (Scope.Benchmark)
   public static class MultiRowBenchmarkState
   {
      BatchSQLEnum SMALL = BatchSQLEnum.SMALL;
      BatchSQLEnum MEDIUM = BatchSQLEnum.MEDIUM;
      BatchSQLEnum LARGE = BatchSQLEnum.LARGE;
      private static Logger logger = Logger.getLogger(MultiRowBenchmarkState.class.getName());
      
      @Setup (Level.Trial)
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
      protected String buildSQL(BatchSQLEnum sql) 
      {
         if (sql.getCount() < 1l)
         {
            throw new RuntimeException("Invalid count value." + sql.getCount());
         }
         StringBuilder builder = new StringBuilder();
         builder.append("INSERT INTO orderline VALUES (?,?)");
         for (long c = 1l; c < sql.getCount()  ; c += 1l  )
         {
            builder.append(",(?,?)");
         }
         return builder.toString();
      }
      private long getSize(String key)
      {
         return Long.parseLong(System.getProperty(key));
      }
   }
   
   /**
    * Benchmark state per thread.
    * @author whitingjr
    */
   @State (Scope.Thread)
   public static class ThreadState
   {
      Connection conn;
      volatile int iteration = 0;

      @Setup (Level.Invocation)
      public void setUp() throws SQLException
      {
         this.conn = ds.getConnection();
         this.conn.setAutoCommit(false);
      }
      @TearDown (Level.Invocation)
      public void tearDown() throws SQLException
      {
         this.conn.close();
         iteration += 1;
      }
   }
   
   /**
    * Bind the values to each position in the statement.
    * @param ps
    * @param sql
    * @param state
    * @return
    * @throws SQLException
    */
   private PreparedStatement bindValues( PreparedStatement ps, BatchSQLEnum sql, ThreadState state ) throws SQLException
   {
      long count = sql.getCount();
      long initialId = state.iteration * sql.getCount(); 
      int pos = 1;
      for (long c = 0l; c < count ; c += 1l  )
      {
         ps.setLong(pos, initialId + c);
         pos += 1;
         ps.setString(pos, Long.toString(c));
         pos += 1;
      }
      return ps;
   }
   
   /**
    * Execute the sql batch with the multi row insert.
    * @param state
    * @param sql enum for the batch size
    * @return the total number of rows inserted
    * @throws SQLException
    */
   private int executeAsBatch(ThreadState state, BatchSQLEnum sql) throws SQLException
   {
      PreparedStatement ps = null;
      int rv = 0;
      try 
      {
         ps = state.conn.prepareStatement(sql.getSQL());
         bindValues(ps, sql, state);
         ps.addBatch();
         int uc[] = ps.executeBatch();
         state.conn.commit();
         for (int c: uc)
         {
            rv += c;
         }
      }
      finally {
         if (null != ps) { ps.close(); }
      }
      return rv;
   }
   
   @Setup
   public void setUp()
   {
      MultiInsertStatementTest.loadProperties();
      super.createPool();
   }
   @TearDown
   public void tearDown()
   {
      super.tearDown();
   }
}
