import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.{ServerSocket, SocketException}

import scala.io.Source

/**
  * Created by Sergio on 16/02/2017.
  */
object LinearRoadDataGenerator {

  def main(args: Array[String]) {
//    if (args.length != 4) {
//      System.err.println("Usage: LinearRoadDataGenerator <port> <file> <l-factor> <sleepMillis>")
//      System.exit(1)
//    }
//    val (port, file, lFactor, sleepMillis) = (args(0).toInt, args(1), args(2).toInt, args(3).toInt)

    val (port, file, lFactor) = (9999, "lrb.txt", 2)

    val serverSocket = new ServerSocket(port)
    println("Listening on port " + port)

    val lines = Source.fromFile(file).getLines().toArray

    while (true) {
      val socket = serverSocket.accept()
      println("Got a new connection")

      val out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
      try {

        for(line <- lines) {
          // (Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos)
          // 0,7,193,21,0,0,0,43,227378,-1,-1,-1,-1,-1,-1
          val mask = line.replaceFirst("(^\\d+,\\d+,\\d+,\\d+,)\\d+(,.+)", "$1%s$2\n")
          1.to(lFactor).foreach(i => out.write(mask.format(i)))
          out.flush()
        }

        println("Input file have been totally consumed.")
      } catch {
        case e: SocketException =>
          println("Client disconnected")
          out.close()
          socket.close()
      }
    }
  }
}
