import java.io.{File, PrintWriter}
import javax.management.{Attribute, ObjectName}
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

    val processName = "Main"
    // val processName = "DataGenerator"

    val sigar = new Sigar

    while (true) {
      var vmDesc: Option[VirtualMachineDescriptor] = None
      while (vmDesc.isEmpty) {
        val vmsDesc = VirtualMachine.list()
        vmDesc = vmsDesc.find(desc => {
          println(s"name: ${desc.displayName()}, id: ${desc.id()}")
          desc.displayName().startsWith(processName)
        })
        Thread.sleep(1000)
      }

      val fname = "stats-memory-%d.csv".format(System.currentTimeMillis)
      val pw = new PrintWriter(new File(fname))

      try {
        pw.println("timestamp, used, committed, max, swap")

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

        var previousTime = -1l
        var previousReadBytes = -1l
        var previousWriteBytes = -1l

        val mbeanConn = connector.getMBeanServerConnection
        while (true) {

//          val mbeans = mbeanConn.queryNames(null, null)
//          mbeans.filter(_.toString.contains("GarbageCollector")) foreach(mbean => {
//            System.out.println("bean: " + mbean + " canonicalname: " + mbean.getCanonicalName)
//            val info = mbeanConn.getMBeanInfo(mbean)
//            info.getAttributes.foreach(attr => {
//              System.out.println(attr.getName)
//            })
//          })

          val bean = mbeanConn.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage")
            .asInstanceOf[CompositeData]
          val values = bean.getAll(Array("used", "committed", "max"))

          // get scavenge collector bean
          val gcScavenge = mbeanConn.getAttributes(new ObjectName("java.lang:type=GarbageCollector,name=PS Scavenge"),
            Array("CollectionTime", "CollectionCount"))
          val gcScavengeCollectionTime = gcScavenge.get(0).asInstanceOf[Attribute].getValue.asInstanceOf[Long]
          val gcScavengeCollectionCount = gcScavenge.get(1).asInstanceOf[Attribute].getValue.asInstanceOf[Long]

          // get marksweep collector bean
          val gcMarkSweep = mbeanConn.getAttributes(new ObjectName("java.lang:type=GarbageCollector,name=PS MarkSweep"),
            Array("CollectionTime", "CollectionCount"))
          val gcMarkSweepCollectionTime = gcMarkSweep.get(0).asInstanceOf[Attribute].getValue.asInstanceOf[Long]
          val gcMarkSweepCollectionCount = gcMarkSweep.get(1).asInstanceOf[Attribute].getValue.asInstanceOf[Long]

          val uptime = mbeanConn.getAttribute(new ObjectName("java.lang:type=Runtime"), "Uptime").asInstanceOf[Long]

          // disk usage
          val usage = sigar.getDiskUsage("/")
          val now = System.currentTimeMillis()
          val readBytes = usage.getReadBytes
          val writeBytes = usage.getWriteBytes
          if(previousTime < 0) {
            previousReadBytes = readBytes
            previousWriteBytes = writeBytes
          }
          val interval = (now - previousTime) / 1000
          val readRate = (readBytes - previousReadBytes) / interval
          val writeRate = (writeBytes - previousWriteBytes) / interval
          previousTime = now
          previousReadBytes = readBytes
          previousWriteBytes = writeBytes

          // val pids = ProcessFinder.find(sigar, s"CredName.User.eq=sesteves")
          // pids.foreach(println)
          val swap = sigar.getSwap

          val stat = "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d".format(System.currentTimeMillis(), values(0), values(1), values(2),
            readRate, writeRate, swap.getUsed, gcScavengeCollectionTime, gcScavengeCollectionCount,
            gcMarkSweepCollectionTime,gcMarkSweepCollectionCount, uptime)
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