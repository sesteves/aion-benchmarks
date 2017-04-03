name := "flink-test2"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
//  "org.fusesource" % "sigar" % "1.6.4"
//  "org.apache.flink" % "flink-connector-kafka" % "1.0.2",
//  "org.apache.flink" % "flink-streaming-scala_2.11" % "1.0.2",
//  "org.apache.flink" % "flink-clients" % "0.10.2-hadoop1"
  "org.apache.flink" % "flink-shaded-hadoop2" % "1.0.3"
//  "net.sf.flexjson" % "flexjson" % "3.3",
//  "org.apache.commons" % "commons-math3" % "3.6.1"
)

// unmanagedJars in Compile += file("lib/vertica_jdk_5.jar")

fork in run := true

val heapSize = Option(System.getProperty("heapsize")).getOrElse("8G")

javaOptions in run ++= Seq(s"-Xms$heapSize", s"-Xmx$heapSize")

// javaOptions in run += "-Djava.library.path=\"./native/\""

