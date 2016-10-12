import java.io.{File, FileOutputStream, PrintWriter}
import java.util.concurrent.TimeUnit

import org.apache.commons.math3.distribution.LogNormalDistribution
import org.apache.flink.api.scala._
import org.apache.flink.api.common.accumulators.{Accumulator, IntCounter, SimpleAccumulator}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hybrid.MemoryFsStateBackend
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{TimeCharacteristic, watermark}
import org.apache.flink.util.Collector

import scala.util.Random

object Main {

  val WindowDurationSec = 5
  val WindowDurationMillis = TimeUnit.SECONDS.toMillis(WindowDurationSec)
  val NumberOfPastWindows = 2

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: Main <maxTuplesInMemory> <tuplesToWatermarkThreshold> <complexity>")
      System.exit(1)
    }
    val (maxTuplesInMemory, tuplesWkThreshold, complexity) = (args(0).toInt, args(1).toLong, args(2).toInt)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(new MemoryFsStateBackend(maxTuplesInMemory))
    // env.setStateBackend(new FsStateBackend("hdfs://ginja-a1:9000/flink/checkpoints"));


//    def makeTuples(n: Int) = (1 to nransient).map((_, 1))
//    val stream = env.fromElements(makeTuples(1000000): _*)
//      .assignAscendingTimestamps(p => System.currentTimeMillis())


    val rawStream = env.socketTextStream("localhost", 9990)

    rawStream.map(line => Tuple1(1)).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(1))).sum(0)
      .writeAsCsv("records-per-second-" + System.currentTimeMillis() + ".csv")



    val punctuatedAssigner = new AssignerWithPunctuatedWatermarks[(String, Int, Long)] {
      // var watermarkEmitted = false
      // scale and shape (or mean and stddev) are 0 and 1 respectively
      // val logNormalDist = new LogNormalDistribution()

      var windowMaxTS = -1l

      @transient
      var r: Random = null

      override def extractTimestamp(element: (String, Int, Long), previousElementTimestamp: Long): Long = {
        if(r == null) r = Random
        val i = r.nextInt(100)

        // TODO generalize the following
        val windowIndex = if(i < 70) 0 else if(i < 90) 1 else 2

        val ts = System.currentTimeMillis()
        if(windowMaxTS < 0) {
          windowMaxTS = ts - (ts % WindowDurationMillis) + WindowDurationMillis
          println(s"### setting window maxTS to: $windowMaxTS, ts: $ts")
        }
        ts - windowIndex * WindowDurationMillis
      }

      override def checkAndGetNextWatermark(lastElement: (String, Int, Long), extractedTimestamp: Long): Watermark = {

        if(extractedTimestamp > windowMaxTS) {
          windowMaxTS = extractedTimestamp - (extractedTimestamp % WindowDurationMillis) + WindowDurationMillis
          println(s"### Emitting watermark at ts: $extractedTimestamp, windowsMaxTS: $windowMaxTS")
          new Watermark(extractedTimestamp)
        } else
          null

//        if(!watermarkEmitted && lastElement._3 > tuplesWkThreshold) {
//          watermarkEmitted = true
//          val ts = extractedTimestamp / 100000 * 100000 + TimeUnit.MINUTES.toMillis(5)
//          // println(s"### emitting watermark at ts: $ts")
//          new watermark.Watermark(ts)
//        } else null
      }
    }


    val stream = rawStream.map(line => {
      val Array(p1, p2, p3) = line.split(" ")
      (p1, p2.toInt, p3.toLong)
    })
      .assignTimestampsAndWatermarks(punctuatedAssigner)
      // .assignAscendingTimestamps(p => System.currentTimeMillis())
      .map(tuple => (tuple._1, tuple._2))

    // The means that it will fire every 10 minutes (in processing time) until the end of the window (event time),
    // and then every 5 minutes (processing time) for late elements up to 20 minutes late. In addition, previous
    // elements are not discarded.
    val trigger = EventTimeTriggerWithEarlyAndLateFiring.create()
      .withEarlyFiringEvery(Time.seconds(1))
      .withLateFiringEvery(Time.seconds(2))
      .withAllowedLateness(Time.seconds(2))
      .accumulating()

//    val trigger2 = new Trigger[Any, TimeWindow] {
//      var count = 0
//
//      def onElement(t: Any, l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult = {
//        count += 1
//        if (count % 100000 == 0) return TriggerResult.FIRE else return TriggerResult.CONTINUE
//      }
//
//      def onProcessingTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult =
//        TriggerResult.FIRE
//
//      def onEventTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult =
//        TriggerResult.FIRE
//    }


    val trigger3 = new Trigger[Any, TimeWindow] {
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
          if(firedOnWatermark.value()) {
            TriggerResult.CONTINUE
          } else {
            firedOnWatermark.update(true)
            TriggerResult.FIRE
          }
        } else {
          // println("FIRE AND PURGE!")
          TriggerResult.FIRE_AND_PURGE
        }
      }

      // override def clear(timeWindow: TimeWindow, triggerContext: Trigger.TriggerContext) = ???
    }

    val numRecords = new IntCounter()

    val accumulator = new SimpleAccumulator[(Int, Int)] {
      var value = (0, 0)

      override def getLocalValue: (Int, Int) = value

      override def resetLocal(): Unit = value = (0, 0)

      override def merge(other: Accumulator[(Int, Int), (Int, Int)]): Unit = ???

      override def add(value: (Int, Int)): Unit = this.value = (value._1, this.value._2 + value._2)
    }

    def myFunction = new RichWindowFunction[(Int, Int), (Int, Int), Tuple, TimeWindow] {
      override def open(parameters: Configuration)  {
        super.open(parameters)

        getRuntimeContext().addAccumulator("num-records", numRecords)
        getRuntimeContext().addAccumulator("accumulator",  accumulator)
      }

      override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Int, Int)], out: Collector[(Int, Int)]): Unit
      = {
        val newInput = input.drop(numRecords.getLocalValue)
        numRecords.add(newInput.size)

        val resultInput = newInput.reduce((p1, p2) => (p1._1, p1._2 + p2._2))
        accumulator.add(resultInput)
        out.collect(accumulator.getLocalValue)
      }
    }

    def simpleFunction = (key: Tuple, timeWindow: TimeWindow, iterator: Iterable[(String, Int)],
                                 collector: Collector[(String, Int)]) =>
      collector.collect(iterator.reduce((p1, p2) => (p1._1, p1._2 + p2._2)))
    def fairlyComplexFunction = (key: Tuple, timeWindow: TimeWindow, iterator: Iterable[(String, Int)],
                                 collector: Collector[(String, Int)]) => {
      println("### Iterator size: " + iterator.size)
      collector.collect(iterator.reduce((p1, p2) => (p1._1, p1._2 + p2._2)))
    }
    def complexFunction = (key: Tuple, timeWindow: TimeWindow, iterator: Iterable[(String, Int)],
                                 collector: Collector[(String, Int)]) =>
      collector.collect(iterator.reduce((p1, p2) => (p1._1, p1._2 + p2._2)))


    def function = complexity match {
      case 0 => simpleFunction
      case 1 => fairlyComplexFunction
      case 2 => complexFunction
    }

    stream.keyBy(1)
      .timeWindow(Time.of(WindowDurationSec, TimeUnit.SECONDS))
      .allowedLateness(Time.of(WindowDurationSec * NumberOfPastWindows, TimeUnit.SECONDS))
      .trigger(trigger3)
//      .trigger(trigger2)
        // .apply(myFunction)
        .apply(function)
//      .sum(1)
//        .reduce((p1, p2) => (p1._1, p1._2 + p2._2))
//        .max(0)
      //.print()


    val result = env.execute()

    val pw = new PrintWriter(new FileOutputStream(new File("runtime.txt"), true))
    pw.println(result.getNetRuntime(TimeUnit.SECONDS))
    pw.close()
  }
}