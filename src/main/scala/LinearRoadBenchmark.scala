import java.io.{File, FileOutputStream, PrintWriter}
import java.util.concurrent.TimeUnit

import org.apache.commons.math3.distribution.LogNormalDistribution
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Sergio on 16/02/2017.
  */
object LinearRoadBenchmark {

  val MaxSegment = 100

  def main(args: Array[String]): Unit = {

    if (args.length != 9) {
      System.err.println("Usage: StockPrices <useHybridBackend> <maxTuplesInMemory> <tuplesAfterSpillFactor> " +
        "<tuplesToWatermarkThreshold> <complexity> <windowDurationSec> <slideDurationSec> <numberOfPastWindows> " +
        "<maximumWatermarks>")
      System.exit(1)
    }
    val (useHybridBackend, maxTuplesInMemory, tuplesAfterSpillFactor, tuplesWkThreshold, complexity, windowDurationSec,
    slideDurationSec, numberOfPastWindows, maxWatermarks) = (args(0).toBoolean, args(1).toInt, args(2).toDouble,
      args(3).toLong, args(4).toInt, args(5).toInt, args(6).toInt, args(7).toInt, args(8).toInt)

    val windowDuration = Time.of(windowDurationSec, TimeUnit.SECONDS)
    val windowDurationMillis = TimeUnit.SECONDS.toMillis(windowDurationSec)
    val windowDurationNanos = TimeUnit.SECONDS.toNanos(windowDurationSec)
    val lateness = Time.of(windowDurationNanos * numberOfPastWindows - 1, TimeUnit.NANOSECONDS)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(windowDurationSec * 1000)
    // env.setStateBackend(new MemoryFsStateBackend(maxTuplesInMemory, tuplesAfterSpillFactor, 5))

    val rawStream = env.socketTextStream("ginja-a5", 9999)

    val vehicleReportAssigner = new AssignerWithPeriodicWatermarks[VehicleReport] {
      var watermarkCount = 0

      // scale and shape (or mean and stddev) are 0 and 1 respectively
      val logNormalDist = new LogNormalDistribution()
      logNormalDist.reseedRandomGenerator(100)

      def sample(): Int = {
        val i = math.round(logNormalDist.sample()).toInt - 1
        if (i <= Math.min(numberOfPastWindows, watermarkCount)) {
          if (i < 0) 0 else i
        } else sample()
      }

      override def extractTimestamp(vr: VehicleReport, l: Long) = {
        val windowIndex = sample()
        vr.time - windowIndex * windowDurationMillis
      }

      override def getCurrentWatermark = {
        if(watermarkCount == maxWatermarks) System.exit(0)
        watermarkCount += 1
        val ts = System.currentTimeMillis()
        println(s"### E M I T T I N G   W A T E R M A R K (#$watermarkCount) at ts: $ts")
        new Watermark(ts)
      }
    }

    val vehicleReports = rawStream.filter(_.startsWith("0")).map(line => VehicleReport(line.substring(2)))
      .assignTimestampsAndWatermarks(vehicleReportAssigner)

    // Note that some vehicles might emit two position reports during this minute
    val averageSpeedAndNumberOfCars = vehicleReports.map(vr => (vr.absoluteSegment, vr.speed, 1)).keyBy(0)
      .timeWindow(windowDuration).allowedLateness(lateness).trigger(new DefaultTrigger)
      .reduce((a, b) => (a._1, a._2 + b._2, a._3 + b._3)).map(t => (t._1, t._2 / t._3, t._3))

    // accident on a given segment whenever two or more vehicles are stopped
    // in that segment at the same lane and position.
    // check vehicles that are stopped: when they report the same position 4 consecutive times
    // location = (absoluteSegment, lane, dir, pos)
    val stoppedVehicles = vehicleReports.keyBy("carId", "location")
      .timeWindow(windowDuration).allowedLateness(lateness).trigger(new DefaultTrigger)
      .fold((None: Option[VehicleReport], 0))((acc, vr) => (Some(vr), acc._2 + 1)).filter(_._2 >= 4).map(_._1.get)

    val accidents = stoppedVehicles.map(vr => (vr.location, vr.absoluteSegment, 1)).keyBy(0)
      .timeWindow(windowDuration).allowedLateness(lateness).trigger(new DefaultTrigger)
      .sum(2).filter(_._3 >= 2).map(t => (t._2, 0, 0))

//    val tolls = averageSpeedAndNumberOfCars.union(accidents).keyBy(0)
//      .timeWindow(windowDuration).allowedLateness(lateness).trigger(new RegisterTrigger)
//      .apply((key: Tuple, tw: TimeWindow, in: Iterable[(Int, Int, Int)], out: Collector[(Int, Double)]) => {
//        val it = in.iterator.toIterable
//        val toll = {
//          if (it.size == 1) {
//            if (it.head._2 > 40 || it.head._3 <= 50) {
//              0
//            } else {
//              // 2 * (numvehicles - 50)^2
//              2 * math.pow(it.head._3 - 50, 2)
//            }
//          } else {
//            0
//          }
//        }
//        out.collect((it.head._1, toll))
//      })

    val computeStartFName = s"compute-time-${System.currentTimeMillis()}.txt"

    val tolls = averageSpeedAndNumberOfCars.union(accidents)
      .timeWindowAll(windowDuration).allowedLateness(lateness).trigger(new RegisterTrigger)
      .apply((tw: TimeWindow, in: Iterable[(Int, Int, Int)], out: Collector[(Int, Double)]) => {

        val startTick = System.currentTimeMillis()
        val iterator = in.iterator.toIterable
        iterator.groupBy(_._1).map({ case (absoluteSegm, it) =>
          val toll = {
            if (it.size == 1) {
              if (it.head._2 > 40 || it.head._3 <= 50) {
                0
              } else {
                // 2 * (numvehicles - 50)^2
                2 * math.pow(it.head._3 - 50, 2)
              }
            } else {
              0
            }
          }
          out.collect((absoluteSegm, toll))
        })
        val endTick = System.currentTimeMillis()

        println("### I T E R A T O R : " + iterator.size + ", time: " + (endTick - startTick))

        val pw = new PrintWriter(new FileOutputStream(new File(computeStartFName), true), true)
        pw.println(s"${tw.maxTimestamp()},$startTick,$endTick,${iterator.size}")

      })

    tolls.print()

    env.execute()
  }

  case class VehicleReport(time: Long, carId: Int, speed: Int, xway: Int, lane: Int, dir: Int, seg: Int, pos: Int,
                           absoluteSegment: Int, location: (Int, Int, Int))
  object VehicleReport {
    def apply(time: Long, carId: Int, speed: Int, xway: Int, lane: Int, dir: Int, seg: Int, pos: Int): VehicleReport = {
      val absoluteSeg = xway * MaxSegment + seg
      val location = (absoluteSeg, lane, pos)
      VehicleReport(time, carId, speed, xway, lane, dir, seg, pos, absoluteSeg, location)
    }
    def apply(str: String): VehicleReport = {
      val el = str.split(',')
      this(el(0).toLong, el(1).toInt, el(2).toInt, el(3).toInt, el(4).toInt, el(5).toInt, el(6).toInt,
        el(7).toInt)
    }
  }

  class DefaultTrigger extends Trigger[Any, TimeWindow] {
    val firedOnWatermarkDescriptor =
      new ValueStateDescriptor[java.lang.Boolean]("FIRED_ON_WATERMARK", classOf[java.lang.Boolean], false)

    override def onElement(t: Any, l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult = {
      triggerContext.registerEventTimeTimer(w.maxTimestamp())
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, timeWindow: TimeWindow, triggerContext: TriggerContext): TriggerResult
    = ???

    override def onEventTime(time: Long, timeWindow: TimeWindow, triggerContext: TriggerContext): TriggerResult = {
      // println(s"### onEventTime called (time: $time, window: $timeWindow)")

      if (time == timeWindow.maxTimestamp) {
        val firedOnWatermark = triggerContext.getPartitionedState(firedOnWatermarkDescriptor)
        if (firedOnWatermark.value()) {
          TriggerResult.CONTINUE
        } else {
          firedOnWatermark.update(true)
          TriggerResult.FIRE
        }
      } else {
        TriggerResult.FIRE_AND_PURGE
      }
    }

    override def clear(window: TimeWindow, ctx: TriggerContext) = {
      ctx.getPartitionedState(firedOnWatermarkDescriptor).clear()
    }
  }

  class RegisterTrigger extends Trigger[Any, TimeWindow] {
    val fireFName = s"fire-${System.currentTimeMillis()}.txt"
    val fireAndPurgeFName = s"fire-and-purge-${System.currentTimeMillis()}.txt"

    val firedOnWatermarkDescriptor =
      new ValueStateDescriptor[java.lang.Boolean]("FIRED_ON_WATERMARK", classOf[java.lang.Boolean], false)

    override def onElement(t: Any, l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult = {
      triggerContext.registerEventTimeTimer(w.maxTimestamp())
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, timeWindow: TimeWindow, triggerContext: TriggerContext): TriggerResult
    = ???

    override def onEventTime(time: Long, timeWindow: TimeWindow, triggerContext: TriggerContext): TriggerResult = {
      // println(s"### onEventTime called (time: $time, window: $timeWindow)")

      if (time == timeWindow.maxTimestamp) {
        val firedOnWatermark = triggerContext.getPartitionedState(firedOnWatermarkDescriptor)
        if (firedOnWatermark.value()) {
          TriggerResult.CONTINUE
        } else {
          firedOnWatermark.update(true)
          val pw = new PrintWriter(new FileOutputStream(new File(fireFName), true), true)
          pw.println(System.currentTimeMillis())
          TriggerResult.FIRE
        }
      } else {
        val pw = new PrintWriter(new FileOutputStream(new File(fireAndPurgeFName), true), true)
        pw.println(System.currentTimeMillis())
        // println("FIRE AND PURGE!")
        TriggerResult.FIRE_AND_PURGE
      }
    }

    override def clear(window: TimeWindow, ctx: TriggerContext) = {
      ctx.getPartitionedState(firedOnWatermarkDescriptor).clear()
    }
  }

}
