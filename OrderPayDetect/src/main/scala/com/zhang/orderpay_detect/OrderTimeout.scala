package com.zhang.orderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource: URL = getClass.getResource("/OrderLog.csv")
    val inputDataStream = env.readTextFile(resource.getPath)
      .map(data => {
        //转换样例类
        val strings: Array[String] = data.split(",")
        OrderLog(strings(0).toLong, strings(1), strings(2), strings(3).toLong)
      }) //延迟0秒
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderLog](Time.seconds(0)) {
      override def extractTimestamp(t: OrderLog): Long = t.eventTime * 1000L
    }).keyBy(_.orderId)

    val orderMatch = inputDataStream
      .process(new OrderMatch())
    //定义测输出流标签
    val timeOutOrder = new OutputTag[OrderResult]("timeOut")
    orderMatch.print("payed")
    orderMatch.getSideOutput(timeOutOrder).print("timeOutOrder")
    env.execute()
  }
}

class OrderMatch() extends KeyedProcessFunction[Long, OrderLog, OrderResult] {
  //定义是否create过
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isCreated", classOf[Boolean]))
  //定义是否pay过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed", classOf[Boolean]))
  //用来保存注册定时器的时间戳
  lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeState", classOf[Long]))

  val timeOutOrder = new OutputTag[OrderResult]("timeOut")

  override def processElement(value: OrderLog, context: KeyedProcessFunction[Long, OrderLog, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    //取出状态
    val isCreated: Boolean = isCreatedState.value()
    val isPayed: Boolean = isPayedState.value()
    val time: Long = timeState.value()
    //从两方便判断，create和pay
    //来的是create，要判断是否 pay过
    if (value.eventType == "create") {
      //如果pay过
      if (isPayed) {
        //输出到主流
        out.collect(OrderResult(value.orderId, "payed successfully"))
        //清空状态
        isPayedState.clear()
        isCreatedState.clear()
        context.timerService().deleteEventTimeTimer(time)
      } else {
        //如果pay没来，注册定时器延长15分钟等待
        val ts: Long = value.eventTime * 1000L + (15 * 60 * 1000L)
        context.timerService().registerEventTimeTimer(ts)
        //更新状态
        isCreatedState.update(true)
        timeState.update(ts)
      }
      //如果来的是pay
    } else if (value.eventType == "pay") {
      //看下是否被创建过
      if (isCreated) {
        //查看pay的时间戳是否超过15分钟
        if (value.eventTime * 1000L < time) {
          out.collect(OrderResult(value.orderId,"payed successfully"))
        } else {
          //已经超时，输出到侧输出流，超时报警
          context.output(timeOutOrder,OrderResult(value.orderId,"payed but timeout"))
        }
        //已经输出,清空状态
        isCreatedState.clear()
        timeState.clear()
        context.timerService().deleteEventTimeTimer(time)
      } else {
        //pay来早了，注册定时器等待create
        context.timerService().registerEventTimeTimer(value.eventTime)
        //更新状态
        isPayedState.update(true)
        timeState.update(value.eventTime)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderLog, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    if(isPayedState.value()){
      //create没来，超时报警
      ctx.output(timeOutOrder,OrderResult(ctx.getCurrentKey,"payed but not found create"))
    } else if(isCreatedState.value()){
      //pay没来，超时报警
      ctx.output(timeOutOrder,OrderResult(ctx.getCurrentKey,"timeout"))
    }
    //清除状态
    isCreatedState.clear()
    isPayedState.clear()
    timeState.clear()
  }
}
