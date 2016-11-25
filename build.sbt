name := "Calc Recommend Items"
version := "0.1"
scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.2",
  "org.apache.spark" %% "spark-mllib" % "2.0.2",
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.10.0-hadoop2")
