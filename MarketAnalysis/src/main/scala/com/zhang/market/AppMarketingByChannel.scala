package com.zhang.market

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//定数数据源样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

//输出样例类
case class MarketViewCountByChannel(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

//自定义生成数据
class SimulatedDataSource() extends RichParallelSourceFunction[MarketUserBehavior] {
  //制作标识位
  var running = true
  //定义推广渠道和用户行为的集合
  val channelSet: Seq[String] = Seq("AppStore", "HuaweiStore", "XiaoStore", "weibo", "wechat")
  val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  //定义随机生成器
  private val rand = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    //定义一个Long的最大值
    val maxElements: Long = Long.MaxValue
    var count = 0L
    //无限循环
    while (running && count < maxElements) {
      val id: String = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()
      sourceContext.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(10L)
    }
  }

  override def cancel(): Unit = false
}

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulatedDataSource())
      .assignAscendingTimestamps(_.timestamp)

    val result: DataStream[MarketViewCountByChannel] = dataStream.filter(_.behavior != "UNINSTALL")
      .keyBy(data => (data.behavior, data.channel))
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketCountByChannel())
    result.print()
    env.execute()

  }
}

class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCountByChannel, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCountByChannel]): Unit = {
    //从上下文中获取window信息，包装成样例类
    val windowStart: String = new Timestamp(context.window.getStart).toString
    val windowEnd: String = new Timestamp(context.window.getEnd).toString
    out.collect(MarketViewCountByChannel(windowStart,windowStart,key._2,key._1,elements.size))
  }

}
