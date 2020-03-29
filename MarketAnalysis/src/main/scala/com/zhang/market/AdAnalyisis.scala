package com.zhang.market

import java.net.URL
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//log数据样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

//输出按照省份统计结果样例类
case class AdCountByProvince(windowEnd: String, province: String, count: Long)

//用户黑名单样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)

/*
 * 总体思路
 * 1.读取数据
 * 2.过滤黑名单 //定义点击次数状态，定义时间状态，定义用户是黑名单状态
 * 3.分组开窗聚合
 * 4.输出
 * 实现过滤黑名单的方法，实现过滤之后，按省分组，统计数量；也可以输出测输出流的数据
 * */
object AdAnalyisis {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val resource: URL = getClass.getResource("/AdClickLog.csv")
    val inputDataStream = env.readTextFile(resource.getPath)
      //转换结构
      .map(
      data => {
        val strings: Array[String] = data.split(",")
        //封装样例类
        AdClickLog(strings(0).toLong, strings(1).toLong, strings(2), strings(3), strings(4).toLong)
      })
      //因为flink都是毫秒算时间的，如果数据是秒，得*1000转换为毫秒
      //为指定时间戳
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //黑名单过滤，侧输出流报警；按照key，分组过滤
    val filterDS = inputDataStream.keyBy(data => (data.userId, data.adId))
      //如果用户点击单个广告次数超过100就加入黑名单，第二天在解除
      .process(new FilterBlackListUser(100))

    //按时间开窗，按照省份统计
    val resullt = filterDS.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new aggCount(), new resultAggCount())

    resullt.print("count")
    filterDS.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("blackList")
    env.execute("ad analysis job")

  }
}

class aggCount() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class resultAggCount() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow]() {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    val wEnd: String = window.getEnd.toString
    out.collect(AdCountByProvince(wEnd, key, input.iterator.next()))
  }
}

class FilterBlackListUser(size: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  //定义点击量的状态
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  //定义0点闹钟的状态
  lazy val resetTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timer", classOf[Long]))
  //定义用户是否已经是黑名状态
  lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSent", classOf[Boolean]))


  //如果点击量为0 设置0点的定时器，判断用户点击次数是否大于100次，如果大于，且没加入黑名单，就加入黑名单
  override def processElement(value: AdClickLog, context: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {
    //取出点击状态
    val count: Long = countState.value()
    //如果点击次数为0，设置0点的定时器
    if (count == 0) {
      //求出第二天的时间，context.timerService().currentProcessingTime()1970-当前的总共毫秒数/ (1000 * 60 * 60 * 24)是这之中有多少天，在+1就是明天* (1000 * 60 * 60 * 24)就是转换成毫秒数
      val ts: Long = (context.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
      //按照第二天的时间注册定时器
      context.timerService().registerProcessingTimeTimer(ts)
      //更新重置时间
      resetTimerState.update(ts)
    }
    //如果用户单个广告点击次数大于100
    if (count >= size) {
      //true为加入黑名单，没有就算了；如果没有加入黑名单，就发送到测输出流，同时加入黑名单
      if (!isSentState.value()) {
        context.output(new OutputTag[BlackListWarning]("blacklist"), BlackListWarning(value.userId, value.adId, "Click over" + size + "times"))
        //同时将状态更新为true，说明这个用户点击的这个广告已经被拉黑
        isSentState.update(true)
      }
    } else {
      //如果点击次数不大于100，更新状态，次数+ 1
      countState.update(count + 1)
      //继续输出
      collector.collect(value)
    }

  }

  //如果时间戳是重置的定时器，对定时器、黑名单、点击状态清除
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    //如果是0点的时间戳，那么触发清空操作
    if (timestamp == resetTimerState.value()) {
      resetTimerState.clear()
      isSentState.clear()
      countState.clear()
    }
  }
}


