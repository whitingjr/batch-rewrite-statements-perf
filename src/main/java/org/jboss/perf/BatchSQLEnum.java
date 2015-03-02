package org.jboss.perf;

public enum BatchSQLEnum {

   SMALL, MEDIUM, LARGE;
   
   private long count = Long.MAX_VALUE;
   private String sql = null;
   
   public void setCount( long c)
   {
      this.count = c;
   }
   public long getCount()
   {
      return this.count;
   }
   
   public void setSQL( String s)
   {
      this.sql = s;
   }
   public String getSQL()
   {
      return this.sql;
   }
}
