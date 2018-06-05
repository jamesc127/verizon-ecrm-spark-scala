name := "Verizon_Spark_Job"

version := "1.0"

//scalaVersion := "2.12.6"
scalaVersion := "2.11.12"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"