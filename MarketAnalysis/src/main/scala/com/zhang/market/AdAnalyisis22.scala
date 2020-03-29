package com.zhang.market

import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//log数据样例类
case class AdClickLog111(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

//输出按照省份统计结果样例类
case class AdCountByProvince111(windowEnd: String, province: String, count: Long)

//用户黑名单样例类
case class BlackListWarning111(userId: Long, adId: Long, msg: String)

object AdAnalyisis22 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resourcess: URL = getClass.getResource("/AdClickLog.csv")
    val inputDataStream = env.readTextFile(resourcess.getPath)
      .map(
        data => {
          val strings: Array[String] = data.split(",")
          AdClickLog(strings(0).toLong, strings(1).toLong, strings(2), strings(3), strings(4).toLong)
        }
      ).assignAscendingTimestamps(_.timestamp * 1000L)

    //过滤
    val keyFunction: DataStream[AdClickLog] = inputDataStream.keyBy(data => (data.userId, data.adId))
      .process(new keyfunction(100))
    val keyDS: DataStream[AdCountByProvince111] = keyFunction.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new aggcount(), new aggResultCount())

    keyDS.print()
    env.execute()
  }

}

class keyfunction(size: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]() {

  private var count: ValueState[Long] = _
  private var isSend: ValueState[Boolean] = _
  private var resetTimeState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    count = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-1", classOf[Long]))
    isSend = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("boolean", classOf[Boolean]))
    resetTimeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTime", classOf[Long]))
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if(timestamp == resetTimeState.value()){
      resetTimeState.clear()
      isSend.clear()
      count.clear()
    }
  }

  override def processElement(value: AdClickLog, context: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {
    val countValue: Long = count.value()
    if (countValue == 0) {
      val ts: Long = (context.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * 1000 * 60 * 60 * 24
      context.timerService().registerProcessingTimeTimer(ts)
      resetTimeState.update(ts)
    }
    if (countValue > size) {
      if (!isSend.value()) {
        context.output(new OutputTag[BlackListWarning111]("output"), BlackListWarning111(value.userId, value.adId, "Click over" + size + "times"))
        isSend.update(true)
      }
    } else {
      count.update(countValue + 1)
      collector.collect(value)
    }

  }
}

class aggcount() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class aggResultCount() extends WindowFunction[Long, AdCountByProvince111, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince111]): Unit = {
    val end: String = window.getEnd.toString
    out.collect(AdCountByProvince111(end, key, input.iterator.next()))
  }
}
