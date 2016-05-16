import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object Main {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.fromElements((1,1),(2,1),(3,1),(4,1),(5,1))
      .assignAscendingTimestamps(p => System.currentTimeMillis + p._1 * 1000)

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
        if (count % 2 == 0) return TriggerResult.FIRE else return TriggerResult.CONTINUE
      }

      override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult =
        TriggerResult.FIRE

      override def onEventTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult =
        TriggerResult.FIRE
    }

    stream.keyBy(1)
      .timeWindow(Time.of(5, TimeUnit.MINUTES))
      .trigger(trigger2)
//      .sum(1)
//        .reduce((p1, p2) => (p1._1, p1._2 + p2._2))
        .max(0)
      .print()




    env.execute()
  }
}