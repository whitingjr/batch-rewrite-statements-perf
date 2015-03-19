package org.jboss.perf.pg;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.jboss.perf.BatchSQLEnum;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class MultiInsertStatementTest extends BaseBench {
   
   private static Logger logger = Logger.getLogger(MultiInsertStatementTest.class.getName());
   @Benchmark
   public void doSMALLMultirowInserts( ThreadState state, MultiRowBenchmarkState benchmarkState )
      throws SQLException
   {  
      try {
         int i = benchmarkState.iteration.getAndIncrement();
         executeAsBatch(state, benchmarkState.SMALL, i);
      } catch (SQLException sqle )
      {
         logger.log(java.util.logging.Level.SEVERE, sqle.getMessage());
         throw new RuntimeException(sqle.getMessage() );
      }
   }
   
   @Benchmark
   public void doMEDIUMMultirowInserts( ThreadState state, MultiRowBenchmarkState benchmarkState  )
   {
      try {
         int i = benchmarkState.iteration.getAndIncrement();
         executeAsBatch(state, benchmarkState.MEDIUM, i);
      } catch (SQLException sqle )
      {
         logger.log(java.util.logging.Level.SEVERE, sqle.getMessage());
         throw new RuntimeException(sqle.getMessage() );
      }
   }
   
   @Benchmark
   public void doLARGEMultirowInserts( ThreadState state, MultiRowBenchmarkState benchmarkState )
   {
      try {
         int i = benchmarkState.iteration.getAndIncrement();
         executeAsBatch(state, benchmarkState.LARGE, i);
      } catch (SQLException sqle )
      {
         logger.log(java.util.logging.Level.SEVERE, sqle.getMessage());
         throw new RuntimeException(sqle.getMessage() );
      }
   }

   /**
    * Bind the values to each position in the statement.
    * Increment the range of identifiers for this iteration. Increment it using
    * the largest sized range. This will avoid duplicate key errors. 
    * @param ps
    * @param sql
    * @param state
    * @return
    * @throws SQLException
    */
   protected PreparedStatement bindValues( PreparedStatement ps, BatchSQLEnum sql, int iteration ) throws SQLException
   {
      final long count = sql.getCount();
      long initialId = iteration * sql.LARGE.getCount(); 
      int pos = 1;
      final Timestamp now = new Timestamp(System.currentTimeMillis());
      final String text = "multimultimultimulti";
      for (long c = 0l; c < count ; c += 1l  )
      {
         long id = initialId + c;
         ps.setLong(pos, id);
         pos += 1;
         ps.setLong(pos, id);
         pos += 1;
         ps.setString(pos, text);
         pos += 1;
         ps.setInt(pos, 1);
         pos += 1;
         ps.setFloat(pos, 5.5f);
         pos += 1;
         ps.setFloat(pos, 99.99f);
         pos += 1;
         ps.setInt(pos, 1);
         pos += 1;
         ps.setTimestamp(pos, now);
         pos += 1;
         ps.setInt(pos, 1);
         pos += 1;
      }
      return ps;
   }
   
   /**
    * Execute the sql in a batch with the multi row insert. In this situation there is only one statement in the batch.
    * Am inclined to leave the batch as typical orm use case will bundle the statement in a batch with other statements. 
    * @param state
    * @param sql enum for the batch size
    * @return the total number of rows inserted
    * @throws SQLException
    */
   protected int executeAsBatch(BaseThreadState state, BatchSQLEnum sql, int iteration) throws SQLException
   {
      PreparedStatement ps = null;
      int rv = 0;
      try 
      {
         ps = state.conn.prepareStatement(sql.getSQL());
         bindValues(ps, sql, iteration);
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
   
   /** 
    * Benchmark state that is set up once beforehand.
    * @author whitingjr
    */
   @State (Scope.Benchmark)
   public static class MultiRowBenchmarkState extends BaseBenchmarkState
   {
      @Setup (Level.Trial)
      public void setUpSQL() 
      {
         super.setUpSQL();
      }
      protected String buildSQL(BatchSQLEnum sql) 
      {
         if (sql.getCount() < 1l)
         {
            throw new RuntimeException("Invalid count value." + sql.getCount());
         }
         StringBuilder builder = new StringBuilder();
         builder.append("INSERT INTO orderline VALUES (?,?,?,?,?,?,?,?,?)");
         for (long c = 1l; c < sql.getCount()  ; c += 1l  )
         {
            builder.append(",(?,?,?,?,?,?,?,?,?)");
         }
         return builder.toString();
      }
   }
   
   /**
    * Benchmark state per thread.
    * @author whitingjr
    */
   @State (Scope.Thread)
   public static class ThreadState extends BaseThreadState
   {
      volatile Connection conn;

      @Setup (Level.Invocation)
      public void setUp() throws SQLException
      {
         super.setUp();
      }
      @TearDown (Level.Invocation)
      public void tearDown() throws SQLException
      {
         super.tearDown();
      }
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
