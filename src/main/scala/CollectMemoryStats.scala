import javax.management.ObjectName
import javax.management.openmbean.CompositeData
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

import com.sun.tools.attach.VirtualMachine
import org.hyperic.sigar.Sigar
import org.hyperic.sigar.ptql.ProcessFinder

import scala.collection.JavaConversions._

/**
  * Created by sesteves on 21-06-2016.
  *
  * http://stackoverflow.com/questions/5552960/how-to-connect-to-a-java-program-on-localhost-jvm-using-jmx
  */
object CollectMemoryStats {

  def main(args: Array[String]): Unit = {

    val processName = "com.intellij.idea.Main"

    val vmsDesc = VirtualMachine.list()
    val vmDesc = vmsDesc.find(desc => {
      println(s"name: ${desc.displayName()}, id: ${desc.id()}")
      desc.displayName() == processName
    })

    if(vmDesc.isEmpty) return

    try {
      val vm = VirtualMachine.attach(vmDesc.get.id)
      val props = vm.getAgentProperties
      val connectorAddress = props.getProperty("com.sun.management.jmxremote.localConnectorAddress")

      val url = new JMXServiceURL(connectorAddress)
      val connector = JMXConnectorFactory.connect(url)

      val mbeanConn = connector.getMBeanServerConnection
      for(i <- 1 to 10) {
        val bean = mbeanConn.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage")
         .asInstanceOf[CompositeData]
        val values = bean.getAll(Array("used", "committed", "max"))
        values.foreach(println)
        Thread.sleep(1000)
      }
    } catch {
      case ex: Exception => ex.printStackTrace
    }

    val sigar = new Sigar
    // val pids = ProcessFinder.find(sigar, s"CredName.User.eq=sesteves")
    // pids.foreach(println)

    val swap = sigar.getSwap
    swap.getUsed




  }

}