name := "bts-rtda-lab-5"

version := "0.1.1"

scalaVersion := "2.11.0"

val sparkVersion = "2.4.0"

val sparkTestingBase = "2.4.0_0.11.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "com.holdenkarau" %% "spark-testing-base" % sparkTestingBase % Test
)

//  SBT testing java options are too small to support running many of the tests due to the need to
//  launch Spark in local mode. Need to be increased
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")