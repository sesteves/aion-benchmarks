import java.io.{File, PrintWriter}
import java.net.{InetAddress, Socket}

import scala.io.BufferedSource

/**
  * Created by Sergio on 02/07/2016.
  */
object DataDump {

  def main(args: Array[String]): Unit = {

    for(i <- 1 to 9) {
      val socket = new Socket(InetAddress.getByName("localhost"), 9990)
      val pw = new PrintWriter(new File("records-" + i * 100000 + ".txt"))
      lazy val in = new BufferedSource(socket.getInputStream()).getLines()
      in.foreach(pw.println)

      pw.close()
      socket.close()
    }
  }
}
