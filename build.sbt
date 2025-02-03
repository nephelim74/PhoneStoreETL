name := "PhoneStoreETL"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "com.crealytics" %% "spark-excel" % "0.13.7",
  "mysql" % "mysql-connector-java" % "8.0.26"
)