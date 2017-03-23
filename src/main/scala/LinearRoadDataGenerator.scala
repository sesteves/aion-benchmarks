import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.{ServerSocket, SocketException}

import scala.io.Source

/**
  * Created by Sergio on 16/02/2017.
  */
object LinearRoadDataGenerator {

  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println("Usage: LinearRoadDataGenerator <l-factor> <ingestionRate>")
      System.exit(1)
    }

    val (port, file, lFactor, ingestionRate) = (9999, "lrb.txt", args(0).toInt, args(1).toInt)

    val serverSocket = new ServerSocket(port)
    println("Listening on port " + port)

    val lines = Source.fromFile(file).getLines().toArray

    while (true) {
      val socket = serverSocket.accept()
      println("Got a new connection")

      val out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
      try {
        var lineIndex = 0
        var count = 0
        var globalTickStart = System.currentTimeMillis()
        while(true) {
          val startTick = System.nanoTime()

          val line = lines(lineIndex)

          // (Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos)
          // 0,7,193,21,0,0,0,43,227378,-1,-1,-1,-1,-1,-1
          val mask = line.replaceFirst("(^\\d+,)\\d+(,\\d+,\\d+,)\\d+(,.+)", "$1%s$2%s$3\n")
          1.to(lFactor).foreach(i => out.write(mask.format(System.currentTimeMillis(), i)))
          out.flush()
          lineIndex += 1
          if (lineIndex == lines.size) lineIndex = 0

          count += lFactor
          val globalTickEnd = System.currentTimeMillis()
          if (globalTickEnd - globalTickStart >= 3000) {
            println(s"rate ~${count / ((globalTickEnd - globalTickStart) / 1000)} records / sec")
            count = 0
            globalTickStart = globalTickEnd
          }

          val endTick = System.nanoTime()
          val diff = endTick - startTick
          val waitNanos = 1000000000 / ingestionRate * lFactor - diff

          while (waitNanos > 0 && System.nanoTime() - endTick < waitNanos) {
            // do nothing
          }
        }

      } catch {
        case _: SocketException =>
          println("Client disconnected")
          socket.close()
      }
    }
  }
}
