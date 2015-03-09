#### PostgreSQL statement performance comparison benchmark.

 This is a benchmark project to allow comparison of two forms of INSERT statement. Intended for PostgreSQL.
 The benchmark will create a simple table for the purposes of performance testing. The JMH framework has been used to configure the behaviour of the performance tests.
 The results are intended for direct comparison of INSERT statement forms rather than finding optimal database performance. The workload characteristic is overly simplistic for producing meaningful database performance tuning results.  

 **Benchmark group types**
 
 The benchmarks are split into 2 logical groups.
 
 The first measures INSERT statements in a single batch
```
 batch begin
  | INSERT
  | INSERT
  | INSERT
  | INSERT
  | n INSERT
 batch end
```
 the second uses an individual multi-row INSERT statement.
 ```
 INSERT INTO orderline VALUES (?,?),(?,?),(?,?),(?,?),(n,n)
 ```
 
 Both types has 3 individual benchmarks with varying numbers of statement/row. There is a benchmark called SMALL, MEDIUM and LARGE. 
 The count for each is configurable. See Configuration section later for details. 

**Configuration**
 
 It is intended that users provide configuration values in the properties file called gradle.properties. Values in this file override the defaults.

 **PostgreSQL database configuration** 
 
 To use the benchmark framework you will need to create a database and a user/role. The role will require necessary permissions to CREATE and DROP a table.
 The properties to configure the connection details are "host", "database", "port", "user", "password". The default port number is 5432. 

 **Tomcat connection pool configuration**
 
 To limit the maximum number of connections created by the pool use the "maxPoolSize" property. The default size is 12.

 **Batch/row count configuration**

 The SMALL, MEDIUM and LARGE size counts can be changed using "small.batch.size", "medium.batch.size" and "large.batch.size". The defaults are 5, 11, 51 respectively.
 
 **Concurrency**
 
 To change the concurrency of the run use the property "threads" to control the number of threads. Default is 1.
 
**Usage**

 After initially configure the database and the gradle.properties with connection details you are ready to run the benchmarks. Using the bundled Gradle wrapper for respective platforms will get you started.
```
$ ./gradlew jmh
```
or
```
> gradle.bat jmh
```

 The project will use the Gradle JMH plugin to build the benchmark artifact and run the performance benchmark tests. The benchmark is expected to finish after x minutes.
 
**Results**
 
 You will find a human readable file with the results of the performance tests. Located here:

$ build/reports/jmh/human.txt
> build\reports\jmh\human.txt
 
**Outcome**
 
 The results display details of each run as iterations progress. At the end is a summary showing ops per second.
 
 ..... tbc
 
 
 
 