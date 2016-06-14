name := "flink-test2"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
//  "org.apache.flink" % "flink-connector-kafka" % "1.0.2",
//  "org.apache.flink" % "flink-streaming-scala_2.11" % "1.0.2",
//  "org.apache.flink" % "flink-clients" % "0.10.2-hadoop1"
)

// unmanagedJars in Compile += file("lib/vertica_jdk_5.jar")

//fork in run := true

//javaOptions in run ++= Seq("-Xms256M", "-Xmx256M", "-XX:MaxPermSize=256M")
