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

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.runtime.state.hybrid.MemoryFsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import scala.util.Random

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

  case class StockPrice(symbol: String, price: Double, ts: Long)
  case class Count(symbol: String, count: Int)

  val symbols = List("SPX", "FTSE", "DJI", "DJT", "BUX", "DAX", "GOOG")

  val defaultPrice = StockPrice("", 1000, -1)

  private var fileOutput: Boolean = false
  private var hostName: String = null
  private var port: Int = 0
  private var outputPath: String = null

  def main(args: Array[String]) {

//    if (!parseParameters(args)) {
//      return
//    }

    val maxTuplesInMemory = 0
    val tuplesAfterSpillFactor = 0
    val windowDurationSec = 30
    val windowDurationNanos = TimeUnit.SECONDS.toNanos(windowDurationSec)
    val numberOfPastWindows = 2

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(windowDurationSec * 1000)
    // env.setStateBackend(new MemoryFsStateBackend(maxTuplesInMemory, tuplesAfterSpillFactor, 5))

    //Step 1
    //Read a stream of stock prices from different sources and union it into one stream

    //Read from a socket stream at map it to StockPrice objects
//    val socketStockStream = env.socketTextStream(hostName, port).map(x => {
//      val split = x.split(",")
//      StockPrice(split(0), split(1).toDouble)
//    })

    //Generate other stock streams
    val SPX_Stream = env.addSource(generateStock("SPX")(10))
    val FTSE_Stream = env.addSource(generateStock("FTSE")(20))
    val DJI_Stream = env.addSource(generateStock("DJI")(30))
    val BUX_Stream = env.addSource(generateStock("BUX")(40))

    //Union all stock streams together
//    val stockStream = socketStockStream.union(SPX_Stream, FTSE_Stream, DJI_Stream, BUX_Stream)
    val stockStream = SPX_Stream.union(FTSE_Stream, DJI_Stream, BUX_Stream)

    //Step 2
    //Compute some simple statistics on a rolling window
    val windowedAssigner = new AssignerWithPeriodicWatermarks[StockPrice] {
      override def extractTimestamp(t: StockPrice, l: Long) = t.ts
      override def getCurrentWatermark = new Watermark(System.currentTimeMillis())
    }

    val windowedStream = stockStream.assignTimestampsAndWatermarks(windowedAssigner).keyBy("symbol", "price")
      .timeWindow(Time.of(10, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS))
      .allowedLateness(Time.of(TimeUnit.SECONDS.toNanos(10) * numberOfPastWindows - 1 , TimeUnit.NANOSECONDS))

    val lowest = windowedStream.minBy("price")
    val maxByStock = windowedStream.maxBy("price")
    val rollingMean = windowedStream.apply(mean)

    //Step 3
    //Use delta policy to create price change warnings,
    // and also count the number of warning every half minute
    val warningAssigner = new AssignerWithPeriodicWatermarks[StockPrice] {
      override def extractTimestamp(t: StockPrice, l: Long) = t.ts
      override def getCurrentWatermark = new Watermark(System.currentTimeMillis())
    }
    val warningPerStockAssigner = new AssignerWithPeriodicWatermarks[(String, Long)] {
      override def extractTimestamp(t: (String, Long), l: Long) = t._2
      override def getCurrentWatermark = new Watermark(System.currentTimeMillis())
    }

    // TODO: check whether eviction is needed:       .evictor()  https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/windowing/TopSpeedWindowing.scala
    val priceWarnings = stockStream.assignTimestampsAndWatermarks(warningAssigner).keyBy("symbol")
      .window(GlobalWindows.create()).evictor(TimeEvictor.of(Time.of(windowDurationSec, TimeUnit.SECONDS)))
      .allowedLateness(Time.of(windowDurationNanos * numberOfPastWindows - 1, TimeUnit.NANOSECONDS))
      .trigger(DeltaTrigger.of(0.05, priceChange, stockStream.dataType.createSerializer(env.getConfig)))
      .apply(sendWarning)

    val warningsPerStock = priceWarnings.assignTimestampsAndWatermarks(warningPerStockAssigner)
      .map(t => Count(t._1, 1)).keyBy("symbol")
      .timeWindow(Time.of(windowDurationSec, TimeUnit.SECONDS))
      .allowedLateness(Time.of(windowDurationNanos * numberOfPastWindows - 1, TimeUnit.NANOSECONDS)).sum("count")

    //Step 4
    //Read a stream of tweets and extract the stock symbols
    val tweetAssigner = new AssignerWithPeriodicWatermarks[(String, Long)] {
      override def extractTimestamp(t: (String, Long), l: Long) = t._2
      override def getCurrentWatermark = new Watermark(System.currentTimeMillis())
    }

    val tweetStream = env.addSource(generateTweets).assignTimestampsAndWatermarks(tweetAssigner).map(_._1)

    val mentionedSymbols = tweetStream.flatMap(tweet => tweet.split(" ")).map(_.toUpperCase())
      .filter(symbols.contains(_))

    val tweetsPerStock = mentionedSymbols.map(Count(_, 1)).keyBy("symbol")
      .timeWindow(Time.of(windowDurationSec, TimeUnit.SECONDS))
      .allowedLateness(Time.of(windowDurationNanos * numberOfPastWindows - 1, TimeUnit.NANOSECONDS)).sum("count")

    //Step 5
    //For advanced analysis we join the number of tweets and
    //the number of price change warnings by stock
    //for the last half minute, we keep only the counts.
    //This information is used to compute rolling correlations
    //between the tweets and the price changes
    val tweetsAndWarning = warningsPerStock.join(tweetsPerStock).where(_.symbol).equalTo(_.symbol)
        .window(SlidingEventTimeWindows.of(Time.of(windowDurationSec, TimeUnit.SECONDS), Time.of(windowDurationSec,
          TimeUnit.SECONDS)))
      .apply((c1, c2) => (c1.count, c2.count))

    // TODO missing lateness

    tweetsAndWarning.print()

    // TODO assign timestamps
    val rollingCorrelation = tweetsAndWarning.timeWindowAll(Time.of(windowDurationSec, TimeUnit.SECONDS))
      .allowedLateness(Time.of(windowDurationNanos * numberOfPastWindows - 1, TimeUnit.NANOSECONDS))
      .apply(computeCorrelation)

    rollingCorrelation.print



    env.execute("Stock stream")
  }

  def priceChange = new DeltaFunction[StockPrice] {
    override def getDelta(oldSP: StockPrice, newSP: StockPrice) = Math.abs(oldSP.price / newSP.price - 1)
  }

  def mean = (key: Tuple, timeWindow: TimeWindow, iterator: Iterable[StockPrice], out: Collector[StockPrice]) => {

    val (symbol, priceSum, elementSum) = iterator.map(stockPrice => (stockPrice.symbol, stockPrice.price, 1))
      .reduce((p1, p2) => (p1._1, p1._2 + p2._2, p1._3 + p2._3))
    out.collect(StockPrice(symbol, priceSum / elementSum, timeWindow.maxTimestamp()))

  }

  def sendWarning = (key: Tuple, globalWindow: GlobalWindow, iterator: Iterable[StockPrice],
                     out: Collector[(String, Long)]) => {
    if (iterator.nonEmpty) {
      val head = iterator.head
      out.collect((head.symbol, head.ts))
    }
  }

  def computeCorrelation = (timeWindow: TimeWindow, input: Iterable[(Int, Int)], out: Collector[Double]) => {
    if (input.nonEmpty) {
      val var1 = input.map(_._1)
      val mean1 = average(var1)
      val var2 = input.map(_._2)
      val mean2 = average(var2)

      val cov = average(var1.zip(var2).map(xy => (xy._1 - mean1) * (xy._2 - mean2)))
      val d1 = Math.sqrt(average(var1.map(x => Math.pow((x - mean1), 2))))
      val d2 = Math.sqrt(average(var2.map(x => Math.pow((x - mean2), 2))))

      out.collect(cov / (d1 * d2))
    }
  }

  def generateStock(symbol: String)(sigma: Int) = {
    var price = 1000.0
    (context: SourceContext[StockPrice]) =>
      while(true) {
        price = price + Random.nextGaussian * sigma
        Thread.sleep(Random.nextInt(200))

        val ts = System.currentTimeMillis()
        context.collect(StockPrice(symbol, price, ts))
      }
  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  def generateTweets = {
    (context: SourceContext[(String, Long)]) =>
      while(true) {
        val s = for (i <- 1 to 3) yield (symbols(Random.nextInt(symbols.size)))
        Thread.sleep(Random.nextInt(500))

        val ts = System.currentTimeMillis()
        context.collect((s.mkString(" "), ts))
      }
  }

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 3) {
      fileOutput = true
      hostName = args(0)
      port = args(1).toInt
      outputPath = args(2)
    } else if (args.length == 2) {
      hostName = args(0)
      port = args(1).toInt
    } else {
      System.err.println("Usage: StockPrices <hostname> <port> [<output path>]")
      return false
    }
    true
  }

}