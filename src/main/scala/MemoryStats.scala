import java.io.{File, PrintWriter}
import javax.management.ObjectName
import javax.management.openmbean.CompositeData
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

import com.sun.tools.attach.{VirtualMachine, VirtualMachineDescriptor}
import org.hyperic.sigar.Sigar

import scala.collection.JavaConversions._

/**
  * Created by sesteves on 29-09-2016.
  */
object MemoryStats {

  def main(args: Array[String]): Unit = {

    val processName = "run-main Main"
    // val processName = "DataGenerator"

    while (true) {
      val vmsDesc = VirtualMachine.list()

      vmsDesc.foreach(desc => {

        println(s"name: ${desc.displayName()}, id: ${desc.id()}")

        val vm = VirtualMachine.attach(desc.id)

        val PropertyConnectorAddress = "com.sun.management.jmxremote.localConnectorAddress"
        val connectorAddress = {
          if (vm.getAgentProperties.getProperty(PropertyConnectorAddress) == null) {
            // no connector address, so we start the JMX agent
            val agent = vm.getSystemProperties().getProperty("java.home") + File.separator + "lib" + File.separator +
              "management-agent.jar"
            vm.loadAgent(agent)
            println(vm.getAgentProperties)
          }
          vm.getAgentProperties.getProperty(PropertyConnectorAddress)
        }


        val url = new JMXServiceURL(connectorAddress)
        val connector = JMXConnectorFactory.connect(url)

        val mbeanConn = connector.getMBeanServerConnection
        val bean = mbeanConn.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage").asInstanceOf[CompositeData]
        val values = bean.getAll(Array("used", "committed", "max"))
        println(s"used: ${values(0)}")
      })

      Thread.sleep(1000)
    }

  }
}
