import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

/**
  * Created by sesteves on 21-06-2016.
  */

//http://stackoverflow.com/questions/5552960/how-to-connect-to-a-java-program-on-localhost-jvm-using-jmx
object CollectMemoryStats {

  def main(args: Array[String]): Unit = {

    val host = "localhost"  // or some A.B.C.D
    val port = 1234
    val url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi"
    val serviceUrl = new JMXServiceURL(url)
    val jmxConnector = JMXConnectorFactory.connect(serviceUrl, null)

    try {
      val mbeanConn = jmxConnector.getMBeanServerConnection()
      // now query to get the beans or whatever
       val beanSet = mbeanConn.queryNames(null, null)

    } finally {
      jmxConnector.close();
    }
  }

}