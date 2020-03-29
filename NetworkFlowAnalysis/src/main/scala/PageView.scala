import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/*
* 思路：
* 1.读取数据源
* 2.对数据源进行结构重构，封装样例类
* 3.进行过滤、分组、聚合
* */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class PvCount(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val inputDataStream: DataStream[String] = env.readTextFile(resource.getPath)
    val userData: DataStream[UserBehavior] = inputDataStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      }
    ) //升序设置
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val resultCount: DataStream[PvCount] = userData.filter(_.behavior == "pv")
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new pvCountAgg(), new pvResult())
    resultCount.print()
    env.execute()
  }

}

class pvCountAgg() extends AggregateFunction[(String, Int), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Int), acc: Long): Long = acc + 1L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class pvResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.iterator.next()))
  }
}