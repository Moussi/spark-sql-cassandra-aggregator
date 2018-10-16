name := "spark-cassandra-aggregator"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.1"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)


libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7"

