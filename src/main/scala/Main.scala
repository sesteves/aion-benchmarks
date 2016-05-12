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

    stream.keyBy(0)
      .timeWindow(Time.of(5, TimeUnit.MINUTES), Time.of(1, TimeUnit.MINUTES))
      .sum(1)
      .print()

    env.execute()
  }
}