package org.jboss.perf.pg;

import java.sql.PreparedStatement;
import java.sql.SQLException;
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
public class IndividualStatementsTest extends BaseBench {

   private static Logger logger = Logger.getLogger(IndividualStatementsTest.class.getName());
   @Benchmark
   public void doSMALLMultiStatementInserts( ThreadState state, IndividualStatementsBenchmarkState benchmarkState )
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
   public void doMEDIUMMultiStatementInserts( ThreadState state, IndividualStatementsBenchmarkState benchmarkState  )
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
   public void doLARGEMultiStatementInserts( ThreadState state, IndividualStatementsBenchmarkState benchmarkState )
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
      final String s = sql.getSQL();
      final long count = sql.getCount();
      final long initialId = iteration * sql.LARGE.getCount(); 
      
      try 
      {
         ps = state.conn.prepareStatement(s);
         for (long c = 1l; c < count; c += 1l){
            long id = initialId + c;
            ps.setLong(1, id);
            ps.setString(2, Long.toString(id));
            ps.addBatch();
         }
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
   public static class IndividualStatementsBenchmarkState extends BaseBenchmarkState
   {
      private static final String SQL = "INSERT INTO orderline VALUES (?,?);";
      @Setup (Level.Trial)
      public void setUpSQL() 
      {
         super.setUpSQL();
      }
      protected String buildSQL(BatchSQLEnum sql) 
      {
         return this.SQL;
      }
   }
   
   /**
    * Benchmark state per thread.
    * @author whitingjr
    */
   @State (Scope.Thread)
   public static class ThreadState extends BaseThreadState
   {
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
