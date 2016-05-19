import java.io.{IOException, PrintWriter}
import java.net.ServerSocket

/**
  * Created by sesteves on 19-05-2016.
  */
object DataGenerator {

  def main(args: Array[String]): Unit = {

//    if (args.length != 2) {
//      System.err.println("Usage: DataGenerator <port> <sleepMillis>")
//      System.exit(1)
//    }
//    // Parse the arguments using a pattern match
//    val (port, sleepMillis) = (args(0).toInt, args(1).toLong)

    val (port, sleepMillis) = (9990, 10)

    val serverSocket = new ServerSocket(port)
    println("Listening on port " + port)

    while(true) {
      val socket = serverSocket.accept()
      println("Got a new connection")

      val out = new PrintWriter(socket.getOutputStream)
      try {

        (1 to 300).foreach(i => {
          out.println(i + " 1")
          out.flush()
          Thread.sleep(sleepMillis)
        })

        println("Done")
      } catch {
        case ex: IOException => ex.printStackTrace()
      } finally {
        out.close()
        socket.close()
      }
    }


  }

}
