name := "GraphClustering"
version := "0.1"

scalaVersion := "2.12.12"

resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.30"
, "org.apache.spark" %% "spark-core" % sparkVersion
, "org.apache.spark" %% "spark-sql" % sparkVersion
//, "org.apache.spark" %% "spark-graphx" % sparkVersion
//, "org.apache.spark" %% "spark-mllib" % sparkVersion
)

test in assembly := {}
