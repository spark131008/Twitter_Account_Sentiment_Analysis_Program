scalaVersion := "2.12.10"

name := "filtered_twitter_batch"
organization := "net.Revature"
version := "1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12"
libraryDependencies += "com.azure" % "azure-ai-textanalytics" % "5.0.0"
