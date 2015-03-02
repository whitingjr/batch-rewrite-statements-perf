package org.jboss.perf.pg;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class MultiInsertStatementTest extends BaseBench {

   
   @Benchmark
   public void do1MultirowInserts( ThreadState state )
   {
   }
   
   @Benchmark
   public void do11MultirowInserts()
   {
   }
   
   @Benchmark
   public void do51MultirowInserts()
   {
   }

   @State (Scope.Thread)
   public static class ThreadState
   {
      BatchSQLEnum SMALL = BatchSQLEnum.SMALL;
      BatchSQLEnum MEDIUM = BatchSQLEnum.MEDIUM;
      BatchSQLEnum LARGE = BatchSQLEnum.LARGE;
      Connection conn;
      private volatile long iteration = 0L;
      
      @Setup (Level.Iteration)
      public void setUp() throws SQLException
      {
         this.conn = ds.getConnection(); 
      }
      @Setup (Level.Trial)
      public void setUpSQL() 
      {
         SMALL.setCount(getSize("small.batch.size"));
         MEDIUM.setCount(getSize("medium.batch.size"));
         LARGE.setCount(getSize("large.batch.size"));
         
         buildSQL(SMALL);
         buildSQL(MEDIUM);
         buildSQL(LARGE);
      }
      
      @TearDown (Level.Iteration)
      public void tearDown() throws SQLException
      {
         this.conn.close();
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
   
   private PreparedStatement bindValues( PreparedStatement ps, BatchSQLEnum sql, long iterationCount ) throws SQLException
   {
      long count = sql.getCount();
      long initialId = iterationCount * sql.getCount(); 
      int pos = 1;
      for (long c = 1l; c < count ; c += 1l  )
      {
         ps.setLong(pos, initialId + c);
         pos += 1;
         ps.setString(pos, Long.toString(c));
         pos += 1;
      } 
      return ps;
   }
   
   private int executeAsBatch(ThreadState state, BatchSQLEnum sql, long iterCount) throws SQLException
   {
      PreparedStatement ps = state.conn.prepareStatement(sql.getSQL());
      bindValues(ps, sql, iterCount);
      int uc[] = ps.executeBatch();
      
   }
}
