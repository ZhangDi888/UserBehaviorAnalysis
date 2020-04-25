-+import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val inputData: DataStream[String] = env.readTextFile(resource.getPath)


    val dataStream: DataStream[UserBehavior] = inputData.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      }
    )
      .assignAscendingTimestamps(_.timestamp * 1000L)
    val result: DataStream[UvCount] = dataStream.filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UcCountByWindow())

    result.print()
    env.execute()
  }

}

class UcCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //用set保存uid进行去重
    var idSet = Set[Long]()
    //遍历值，放进set集合中
    for (elem <- input) {
      idSet += elem.userId
    }
    //输出统计值
    out.collect(UvCount(window.getEnd,idSet.size))
  }
}*/
