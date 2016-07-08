name := "flink-test2"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
//  "org.fusesource" % "sigar" % "1.6.4"
//  "org.apache.flink" % "flink-connector-kafka" % "1.0.2",
//  "org.apache.flink" % "flink-streaming-scala_2.11" % "1.0.2",
//  "org.apache.flink" % "flink-clients" % "0.10.2-hadoop1"
  "org.apache.flink" % "flink-shaded-hadoop2" % "1.0.3"
)

// unmanagedJars in Compile += file("lib/vertica_jdk_5.jar")

fork in run := true

javaOptions in run ++= Seq("-Xms8G", "-Xmx8G", "-XX:MaxPermSize=8G")

// javaOptions in run += "-Djava.library.path=\"./native/\""

