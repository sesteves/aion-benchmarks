import java.util.concurrent.TimeUnit

import org.apache.flink.runtime.state.hybrid.MemoryFsStateBackend
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by Sergio on 16/02/2017.
  */
object LinearRoadBenchmark {

  val MaxSegment = 100

  def main(args: Array[String]): Unit = {

    if (args.length != 8) {
      System.err.println("Usage: StockPrices <maxTuplesInMemory> <tuplesAfterSpillFactor> <tuplesToWatermarkThreshold> "
        + "<complexity> <windowDurationSec> <slideDurationSec> <numberOfPastWindows> <maximumWatermarks>")
      System.exit(1)
    }
    val (maxTuplesInMemory, tuplesAfterSpillFactor, tuplesWkThreshold, complexity, windowDurationSec, slideDurationSec,
    numberOfPastWindows, maxWatermarks) = (args(0).toInt, args(1).toDouble, args(2).toLong, args(3).toInt,
      args(4).toInt, args(5).toInt, args(6).toInt, args(7).toInt)


    val windowDuration = Time.of(windowDurationSec, TimeUnit.SECONDS)
    val windowDurationNanos = TimeUnit.SECONDS.toNanos(windowDurationSec)
    val lateness = Time.of(windowDurationNanos * numberOfPastWindows - 1, TimeUnit.NANOSECONDS)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(windowDurationSec * 1000)
    // env.setStateBackend(new MemoryFsStateBackend(maxTuplesInMemory, tuplesAfterSpillFactor, 5))

    val rawStream = env.socketTextStream("localhost", 9999)


    val vehicleReports = rawStream.filter(_.startsWith("0")).map(VehicleReport(_))


    // Note that some vehicles might emit two position reports during this minute
    val averageSpeedAndNumberOfCars = vehicleReports.map(vr => (vr.absoluteSeg, vr.speed, 1)).keyBy(0)
      .reduce((a, b) => (a._1, a._2 + b._2, a._3 + b._3)).map(t => (t._1, t._2 / t._3, t._3))

    // accident on a given segment whenever two or more vehicles are stopped
    // in that segment at the same lane and position.
    // check vehicles that are stopped: when they report the same position 4 consecutive times
    // location = (absoluteSegment, lane, dir, pos)
    val stoppedVehicles = vehicleReports.keyBy("carId", "location")
      .fold((null: VehicleReport , 0))((acc, vr) => (vr, acc._2 + 1)).filter(_._2 >= 4).map(_._1)

    val accidents = stoppedVehicles.map((_, 1)).keyBy("location").sum(1).filter(_._2 >= 2)
      .map(p => (p._1.absoluteSeg, 0))




    val tolls = averageSpeedAndNumberOfCars.filter(t => !(t._2 >= 40 || t._3 <= 50)).map(t => (t._1, t._3))
      .union(accidents).keyBy(0).









    warningsPerStock.union(tweetsPerStock).keyBy("symbol")
      .timeWindow(windowDuration).allowedLateness(lateness).trigger(trigger)
      .apply((key: Tuple, tw: TimeWindow, in: Iterable[Count], out: Collector[(Int, Int)]) => {
        in.groupBy(_.symbol).foreach({case (_, it) =>
          val v = (it.head.count, it.last.count)
          out.collect(v) })
      })



    env.execute()
  }

  case class VehicleReport(time: Long, carId: Int, speed: Int, xway: Int, lane: Int, dir: Int, seg: Int, pos: Int) {
    val absoluteSeg = xway * MaxSegment + seg
    val location = (absoluteSeg, lane, pos)
  }
  object VehicleReport {
    def apply(str: String): VehicleReport = {
      val el = str.split(',')
      VehicleReport(el(0).toLong, el(1).toInt, el(2).toInt, el(3).toInt, el(4).toInt, el(5).toInt, el(6).toInt,
        el(7).toInt)
    }
  }
}
