name := "SecureWorksAssignment"

version := "0.1"

scalaVersion := "2.12.4"

scalacOptions ++=
  Seq("-encoding", "UTF8", "-unchecked", "-deprecation", "-language:postfixOps", "-language:implicitConversions", "-language:higherKinds", "-language:reflectiveCalls")


val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "com.github.scopt" %% "scopt" % "3.5.0"
)