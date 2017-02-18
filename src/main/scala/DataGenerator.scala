import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.{ServerSocket, SocketException}

import scala.io.Source
import scala.util.Random

/**
  * Created by sesteves on 19-05-2016.
  */
object DataGenerator {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: DataGenerator <workload> <ingestionRate>")
      System.exit(1)
    }
    val (workload, ingestionRate) = (args(0).toInt, args(1).toInt)
    val port = workload match {
      case 0 => 9990
      case 1 => 9991
      case 2 => 9992
      case 3 => 9993
    }

    // workload 1
    val lines = if(workload == 1) {
      val filename = "twitter-sample.txt"
      Some(Source.fromFile(filename).getLines().toArray)
    } else {
      None
    }

    // workload 2
    val random = new Random(100)
    val symbols = 1.to(1000).map(i => s"SYM$i")

    val serverSocket = new ServerSocket(port)
    println("Listening on port " + port)

    while (true) {
      val socket = serverSocket.accept()
      println("Got a new connection")

      // val out = new PrintWriter(socket.getOutputStream)
      val out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
      try {
        var i = 0
        var count = 0
        var lineIndex = 0
        var globalTickStart = System.currentTimeMillis()
        while (true) {

          val startTick = System.nanoTime()

          workload match {
            case 0 => out.write("X"*2253 + " 1 " + i.toString + "\n")
            case 1 =>
              out.write(lines.get(lineIndex))
              out.newLine()
              lineIndex += 1
              if (lineIndex == lines.get.size) lineIndex = 0
            case 2 =>
              val symbol = symbols(random.nextInt(symbols.size))
              val price = 1000.0 + random.nextGaussian * 10
              val ts = System.currentTimeMillis()
              out.write(s"$symbol $price $ts\n")
            case 3 =>
              val s = 1.to(10).map(symbols(random.nextInt(symbols.size))).mkString(" ")
              val ts = System.currentTimeMillis()
              out.write(s"$s $ts\n")
          }
          out.flush()
          count += 1
          i += 1
          if(i == 1000000) i = 0

          val globalTickEnd = System.currentTimeMillis()
          if (globalTickEnd - globalTickStart >= 3000) {
            println(s"rate ~${count / ((globalTickEnd - globalTickStart) / 1000)} records / sec")
            count = 0
            globalTickStart = globalTickEnd
          }

          val endTick = System.nanoTime()
          val diff = endTick - startTick
          val waitNanos = 1000000000 / ingestionRate - diff

          while (waitNanos > 0 && System.nanoTime() - endTick < waitNanos) {
            // do nothing
          }
        }

      } catch {
        case ex: SocketException => println("connection closed")
      } finally {
        // out.close()
        socket.close()
      }
    }
  }
}
