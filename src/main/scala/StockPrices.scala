/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.concurrent.TimeUnit

import org.apache.commons.math3.distribution.LogNormalDistribution
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.runtime.state.hybrid.MemoryFsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * This example showcases a moderately complex Flink Streaming pipeline.
  * It to computes statistics on stock market data that arrive continuously,
  * and combines the stock market data with tweet streams.
  * For a detailed explanation of the job, check out the
  * [[http://flink.apache.org/news/2015/02/09/streaming-example.html blog post]]
  * unrolling it. To run the example make sure that the service providing
  * the text data is already up and running.
  *
  * To start an example socket text stream on your local machine run netcat
  * from a command line, where the parameter specifies the port number:
  *
  * {{{
  *   nc -lk 9999
  * }}}
  *
  * Usage:
  * {{{
  *   StockPrices <hostname> <port> <output path>
  * }}}
  *
  * This example shows how to:
  *
  *   - union and join data streams,
  *   - use different windowing policies,
  *   - define windowing aggregations.
  */
object StockPrices {

  val HostName = "localhost"
  val StocksPort = 9992
  val TweetsPort = 9993


  case class StockPrice(symbol: String, price: Double, ts: Long, dummy: String = "X" * additionalTupleSize)
  case class Count(var symbol: String, var count: Int, var dummy: String = "X" * additionalTupleSize) {
    def this() = this(null, -1)
    def setSymbol(symbol: String) = this.symbol = symbol
    def setCount(count: Int) = this.count = count
    def setDummy(dummy: String) = this.dummy = dummy
    def getSymbol = symbol
    def getCount = count
    def getDummy = dummy
  }

  // val symbols = List("SPX", "FTSE", "DJI", "DJT", "BUX", "DAX", "GOOG")
  // val symbols = 1.to(1000).map(i => s"SYM$i")

  val defaultPrice = StockPrice("", 1000, -1)

  private var fileOutput: Boolean = false
  private var outputPath: String = null

  // val random = new Random(100)

  private var numberOfPastWindows: Int = _

  private var windowDurationMillis: Long = _

  val additionalTupleSize = 1

  def main(args: Array[String]) {
//    if (!parseParameters(args)) {
//      return
//    }

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

    this.windowDurationMillis = TimeUnit.SECONDS.toMillis(windowDurationSec)
    this.numberOfPastWindows = numberOfPastWindows

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(windowDurationSec * 1000)
    env.setStateBackend(new MemoryFsStateBackend(maxTuplesInMemory, tuplesAfterSpillFactor, 5))

    //Step 1
    //Read a stream of stock prices from different sources and union it into one stream

    //Read from a socket stream at map it to StockPrice objects
//    val socketStockStream = env.socketTextStream(hostName, port).map(x => {
//      val split = x.split(",")
//      StockPrice(split(0), split(1).toDouble)
//    })

    //Generate other stock streams
//    val SPX_Stream = env.addSource(generateStock("SPX")(10))
//    val FTSE_Stream = env.addSource(generateStock("FTSE")(20))
//    val DJI_Stream = env.addSource(generateStock("DJI")(30))
//    val BUX_Stream = env.addSource(generateStock("BUX")(40))

    val stockAssigner = new AssignerWithPeriodicWatermarks[StockPrice] {
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

      override def extractTimestamp(t: StockPrice, l: Long) = {
        val windowIndex = sample()
        t.ts - windowIndex * windowDurationMillis
      }

      override def getCurrentWatermark = {
        if(watermarkCount == maxWatermarks) System.exit(0)
        watermarkCount += 1
        val ts = System.currentTimeMillis()
        println(s"### E M I T T I N G   W A T E R M A R K (#$watermarkCount) at ts: $ts")
        new Watermark(ts)
      }
    }

    // read from a socket stream stock prices
    val stockStream = env.socketTextStream(HostName, StocksPort).map(s => {
      val elements = s.split(" ")
      StockPrice(elements(0), elements(1).toDouble, elements(2).toLong)
    }).assignTimestampsAndWatermarks(stockAssigner)

    //Union all stock streams together
//    val stockStream = socketStockStream.union(SPX_Stream, FTSE_Stream, DJI_Stream, BUX_Stream)
    // val stockStream = SPX_Stream.union(FTSE_Stream, DJI_Stream, BUX_Stream).assignTimestampsAndWatermarks(stockAssigner)

    //Step 2
    //Compute some simple statistics on a rolling window
    val trigger = new Trigger[Any, TimeWindow] {

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
    }

    /*
    val windowedStream = stockStream.keyBy("symbol", "price")
      .timeWindow(Time.of(10, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS))
      .allowedLateness(Time.of(TimeUnit.SECONDS.toNanos(10) * numberOfPastWindows - 1 , TimeUnit.NANOSECONDS))
      .trigger(trigger)

    val lowest = windowedStream.minBy("price")
    val maxByStock = windowedStream.maxBy("price")
    val rollingMean = windowedStream.apply(mean)
    */

    //Step 3
    //Use delta policy to create price change warnings,
    // and also count the number of warning every half minute
    val warningPerStockAssigner = new AssignerWithPeriodicWatermarks[(String, Long, String)] {
      override def extractTimestamp(t: (String, Long, String), l: Long) = t._2
      override def getCurrentWatermark = new Watermark(System.currentTimeMillis())
    }

//   eviction: https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/windowing/TopSpeedWindowing.scala
//    val priceWarnings = stockStream.keyBy("symbol")
//      .window(GlobalWindows.create()).evictor(TimeEvictor.of(windowDuration)).allowedLateness(lateness)
//      .trigger(DeltaTrigger.of(0.05, priceChange, stockStream.dataType.createSerializer(env.getConfig)))
//      .apply(sendWarning)

    def delta(oldPrice: Double, newPrice: Double) = Math.abs(oldPrice / newPrice - 1) > 0.05

    val myDeltaTrigger = new Trigger[StockPrice, TimeWindow] {
      val stateDesc = new ValueStateDescriptor[java.lang.Double]("last-element", classOf[java.lang.Double], null)

      override def onElement(element: StockPrice, ts: Long, w: TimeWindow, ctx: TriggerContext): TriggerResult = {
        val lastElementState = ctx.getPartitionedState(stateDesc)

        if (lastElementState.value() == null) {
          lastElementState.update(element.price)
          TriggerResult.CONTINUE
        } else if (delta(lastElementState.value(), element.price)) {
          lastElementState.update(element.price)
          return TriggerResult.FIRE
        } else {
          TriggerResult.CONTINUE
        }
      }

      override def onEventTime(time: Long, timeWindow: TimeWindow, ctx: TriggerContext): TriggerResult = {
        return TriggerResult.CONTINUE
      }

      override def onProcessingTime(time: Long, timeWindow: TimeWindow, ctx: TriggerContext): TriggerResult = {
        return TriggerResult.CONTINUE
      }

      override def clear(window: TimeWindow, ctx: TriggerContext) = {
        ctx.getPartitionedState(stateDesc).clear()
      }
    }

//    val priceWarnings = stockStream.keyBy("symbol").timeWindow(windowDuration).allowedLateness(lateness)
//      .trigger(myDeltaTrigger).apply(sendWarning)

    val priceWarnings = stockStream.keyBy("symbol").timeWindow(windowDuration).allowedLateness(lateness)
      .trigger(trigger)
      .apply((key: Tuple, tw: TimeWindow, in: Iterable[StockPrice], out: Collector[(String, Long, String)]) => {

        val startTick = System.currentTimeMillis()
        val it = in.iterator.toIterable
        in.sliding(2).filter(it => delta(it.head.price, it.last.price))
          .map(it => out.collect(it.head.symbol, it.head.ts, it.head.dummy))

        val endTick = System.currentTimeMillis()

        println("### I T E R A T O R (delta): " + it.size + ", time: " + (endTick - startTick))

        val pw = new PrintWriter(new FileOutputStream(new File(computeStartFName), true), true)
        pw.println(s"${tw.maxTimestamp()},$startTick,$endTick,${it.size}")
      }
    )

    // TODO check if assigned timestamps are passed across and if trigger is needed
    val warningsPerStock = priceWarnings.assignTimestampsAndWatermarks(warningPerStockAssigner)
      .map(t => Count(t._1, 1)).keyBy("symbol").timeWindow(windowDuration).allowedLateness(lateness).sum("count")

    warningsPerStock.print()

    //Step 4
    //Read a stream of tweets and extract the stock symbols
    val tweetAssigner = new AssignerWithPeriodicWatermarks[(String, Long)] {
      var watermarkCount = 0

      // scale and shape (or mean and stddev) are 0 and 1 respectively
      val logNormalDist = new LogNormalDistribution()
      logNormalDist.reseedRandomGenerator(100)

      def sample(): Int = {
        val i = math.round(logNormalDist.sample()).toInt - 1
        if (i <= Math.min(numberOfPastWindows, watermarkCount)) { if (i < 0) 0 else i } else sample()
      }

      override def extractTimestamp(t: (String, Long), l: Long) = {
        val windowIndex = sample()
        t._2 - windowIndex * windowDurationMillis
      }

      override def getCurrentWatermark = {
        watermarkCount += 1
        new Watermark(System.currentTimeMillis())
      }
    }

    // read from a socket stream stock prices
    val tweetStream = env.socketTextStream(HostName, TweetsPort).map(str => {
      val t = str.splitAt(str.lastIndexOf(" ") + 1)
      (t._1 , t._2.toLong)
    }).assignTimestampsAndWatermarks(tweetAssigner).map(_._1)

    //val tweetStream = env.addSource(generateTweets).assignTimestampsAndWatermarks(tweetAssigner).map(_._1)

    val mentionedSymbols = tweetStream.flatMap(_.split(' ')).map(_.toUpperCase) // .filter(symbols.contains(_))

    val tweetsPerStock = mentionedSymbols.map(Count(_, 1)).keyBy("symbol").timeWindow(windowDuration)
      .allowedLateness(lateness).trigger(trigger).sum("count")

    //Step 5
    //For advanced analysis we join the number of tweets and the number of price change warnings by stock
    //for the last half minute, we keep only the counts.
    //This information is used to compute rolling correlations between the tweets and the price changes

    val tweetsAndWarning = warningsPerStock.union(tweetsPerStock).keyBy("symbol")
      .timeWindow(windowDuration).allowedLateness(lateness).trigger(trigger)
      .apply((key: Tuple, tw: TimeWindow, in: Iterable[Count], out: Collector[(Int, Int)]) => {
        in.groupBy(_.symbol).foreach({case (_, it) =>
          val v = (it.head.count, it.last.count)
          out.collect(v) })
    })


//    val tweetsAndWarning = warningsPerStock.join(tweetsPerStock).where(_.symbol).equalTo(_.symbol)
//      .window(SlidingEventTimeWindows.of(Time.of(windowDurationSec, TimeUnit.SECONDS), Time.of(windowDurationSec,
//          TimeUnit.SECONDS)))
//      .apply((c1, c2) => (c1.count, c2.count))

    val fireFName = s"fire-${System.currentTimeMillis()}.txt"
    val fireAndPurgeFName = s"fire-and-purge-${System.currentTimeMillis()}.txt"
    val trigger2 = new Trigger[Any, TimeWindow] {

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
    }

    // TODO assign timestamps if needed
    val rollingCorrelation = tweetsAndWarning.timeWindowAll(windowDuration).allowedLateness(lateness).trigger(trigger2)
      .apply(computeCorrelation)

    rollingCorrelation.print

    env.execute("Stock stream")
  }


  def priceChange = new DeltaFunction[StockPrice] {
    override def getDelta(oldSP: StockPrice, newSP: StockPrice) = Math.abs(oldSP.price / newSP.price - 1)
  }

  def mean = (key: Tuple, tw: TimeWindow, in: Iterable[StockPrice], out: Collector[StockPrice]) => {

    val (symbol, priceSum, elementSum) = in.map(stockPrice => (stockPrice.symbol, stockPrice.price, 1))
      .reduce((p1, p2) => (p1._1, p1._2 + p2._2, p1._3 + p2._3))
    out.collect(StockPrice(symbol, priceSum / elementSum, tw.maxTimestamp()))

  }

  def sendWarning = (key: Tuple, tw: TimeWindow, in: Iterable[StockPrice], out: Collector[(String, Long, String)]) => {

    val it = in.iterator.toIterable
    out.collect((it.head.symbol, it.head.ts, it.head.dummy))
  }

  val computeStartFName = s"compute-time-${System.currentTimeMillis()}.txt"

  def computeCorrelation = (tw: TimeWindow, in: Iterable[(Int, Int)], out: Collector[Double]) => {
    val startTick = System.currentTimeMillis()

    val it = in.iterator.toIterable

    val var1 = it.map(_._1)
    val mean1 = average(var1)
    val var2 = it.map(_._2)
    val mean2 = average(var2)

    val cov = average(var1.zip(var2).map(xy => (xy._1 - mean1) * (xy._2 - mean2)))
    val d1 = Math.sqrt(average(var1.map(x => Math.pow((x - mean1), 2))))
    val d2 = Math.sqrt(average(var2.map(x => Math.pow((x - mean2), 2))))

    out.collect(cov / (d1 * d2))

    val endTick = System.currentTimeMillis()
    println("### I T E R A T O R (correlation): " + it.size + ", time: " + (endTick - startTick))

//    val pw = new PrintWriter(new FileOutputStream(new File(computeStartFName), true), true)
//    pw.println(s"${tw.maxTimestamp()},$startTick,$endTick,${it.size}")
  }

//  def generateStock(symbol: String)(sigma: Int) = {
//    var price = 1000.0
//    (context: SourceContext[StockPrice]) =>
//      while(true) {
//        price = price + random.nextGaussian * sigma
//        Thread.sleep(100)
//
//        val ts = System.currentTimeMillis()
//        context.collect(StockPrice(symbol, price, ts))
//      }
//  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

//  def generateTweets = {
//    (context: SourceContext[(String, Long)]) =>
//      while(true) {
//        val s = for (i <- 1 to 3) yield (symbols(random.nextInt(symbols.size)))
//        Thread.sleep(200)
//
//        val ts = System.currentTimeMillis()
//        context.collect((s.mkString(" "), ts))
//      }
//  }

}