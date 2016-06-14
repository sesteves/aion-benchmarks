import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.accumulators.{Accumulator, IntCounter, SimpleAccumulator}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{RichWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Main {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


//    def makeTuples(n: Int) = (1 to n).map((_, 1))
//    val stream = env.fromElements(makeTuples(1000000): _*)
//      .assignAscendingTimestamps(p => System.currentTimeMillis())


    val stream = env.socketTextStream("localhost", 9990).map(line => {
      val Array(p1, p2) = line.split(" ")
      (p1, p2.toInt)
    })
      .assignAscendingTimestamps(p => System.currentTimeMillis())


    // The means that it will fire every 10 minutes (in processing time) until the end of the window (event time),
    // and then every 5 minutes (processing time) for late elements up to 20 minutes late. In addition, previous
    // elements are not discarded.
    val trigger = EventTimeTriggerWithEarlyAndLateFiring.create()
      .withEarlyFiringEvery(Time.seconds(1))
      .withLateFiringEvery(Time.seconds(2))
      .withAllowedLateness(Time.seconds(2))
      .accumulating()

    val trigger2 = new Trigger[Any, TimeWindow] {
      var count = 0

      override def onElement(t: Any, l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult = {
        count += 1
        if (count % 8 == 0) return TriggerResult.FIRE else return TriggerResult.CONTINUE
      }

      override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult =
        TriggerResult.FIRE

      override def onEventTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult =
        TriggerResult.FIRE
    }


    val numRecords = new IntCounter();

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

    stream.keyBy(1)
      .timeWindow(Time.of(5, TimeUnit.MINUTES))
      //.trigger(trigger2)
        // .apply(myFunction)
      .apply((tuple, timeWindow, iterator, collector: Collector[(String, Int)]) => {
        collector.collect(iterator.reduce((p1, p2) => (p1._1, p1._2 + p2._2)))
      })
//      .sum(1)
//        .reduce((p1, p2) => (p1._1, p1._2 + p2._2))
//        .max(0)
      //.print()




    env.execute()
  }
}