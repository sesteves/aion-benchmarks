import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object Main {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.fromElements((1,1),(2,1),(3,1),(4,1),(5,1))
      .assignAscendingTimestamps(p => System.currentTimeMillis)

    stream.keyBy(1)
      .timeWindow(Time.of(5, TimeUnit.MINUTES))
      .sum(1)
      .print()

    // The means that it will fire every 10 minutes (in processing time) until the end of the window (event time), and then every 5 minutes (processing time) for late elements up to 20 minutes late. In addition, previous elements are not discarded.
      val trigger = EventTimeTriggerWithEarlyAndLateFiring.create()
        .withEarlyFiringEvery(Time.minutes(10))
        .withLateFiringEvery(Time.minutes(5))
        .withAllowedLateness(Time.minutes(20))
        .accumulating()


    env.execute()
  }
}