import java.io.{File, PrintWriter}
import javax.management.ObjectName
import javax.management.openmbean.CompositeData
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

import com.sun.tools.attach.{VirtualMachine, VirtualMachineDescriptor}
import org.hyperic.sigar.Sigar

import scala.collection.JavaConversions._

/**
  * Created by sesteves on 21-06-2016.
  *
  * http://stackoverflow.com/questions/5552960/how-to-connect-to-a-java-program-on-localhost-jvm-using-jmx
  */
object CollectMemoryStats {

  def main(args: Array[String]): Unit = {

    val processName = "run-main Main"
    // val processName = "DataGenerator"

    var vmDesc: Option[VirtualMachineDescriptor] = None
    while(vmDesc.isEmpty) {
      val vmsDesc = VirtualMachine.list()
      vmDesc = vmsDesc.find(desc => {
        println(s"name: ${desc.displayName()}, id: ${desc.id()}")
        desc.displayName().contains(processName)
      })
      Thread.sleep(1000)
    }


    while(true) {
      val fname = "stats-memory-%d.csv".format(System.currentTimeMillis)
      val pw = new PrintWriter(new File(fname))

      try {
        pw.println("timestamp, used, commited, max, swap")

        val vm = VirtualMachine.attach(vmDesc.get.id)

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
        while (true) {
          val bean = mbeanConn.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage")
            .asInstanceOf[CompositeData]
          val values = bean.getAll(Array("used", "committed", "max"))

          val sigar = new Sigar
          // val pids = ProcessFinder.find(sigar, s"CredName.User.eq=sesteves")
          // pids.foreach(println)
          val swap = sigar.getSwap

          val stat = "%d,%d,%d,%d,%d".format(System.currentTimeMillis(), values(0), values(1), values(2), swap.getUsed)
          pw.println(stat)

          Thread.sleep(1000)
        }
      } catch {
        case ex: Exception => ex.printStackTrace
      } finally {
        pw.close
      }
    }

  }

}