package org.jboss.perf.pg;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

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
      Connection conn;
      private volatile long iteration = 0L;
      
      @Setup (Level.Iteration)
      public void setUp() throws SQLException
      {
         this.conn = ds.getConnection(); 
      }
      
      @TearDown (Level.Iteration)
      public void tearDown() throws SQLException
      {
         this.conn.close();
      }
   }

   private String buildSQL(int tupleCount)
   {
      StringBuilder builder = new StringBuilder();
      builder.append("INSERT INTO orderline VALUES (?,?)");
      for (int c = 1; c < tupleCount  ; c += 1  )
      {
         builder.append(",(?,?)");
      }
      return builder.toString(); 
   }
   
   private PreparedStatement bindValues( PreparedStatement ps, int tupleCount, long iteration ) throws SQLException
   {
      int initialId = iteration * ; 
      for (int c = 1; c < tupleCount  ; c += 1  )
      {
         ps.setLong(1, initialId + c);
      } 
      return ps;
   }
   
   private int executeAsBatch(String sql) throws SQLException
   {
      int count = -1;
      
      
      return count;
   }
}
