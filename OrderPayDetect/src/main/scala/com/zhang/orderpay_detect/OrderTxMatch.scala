package com.zhang.orderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


//输入数据的样例类
case class OrderLog(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object OrderTxMatch {
  //为了公用的outputTag，直接定义
  val unmatchedPays = new OutputTag[OrderLog]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource: URL = getClass.getResource("/OrderLog.csv")
    val inputDataStream = env.readTextFile(resource.getPath)
      .map(
        data => {
          val strings: Array[String] = data.split(",")
          OrderLog(strings(0).toLong, strings(1), strings(2), strings(3).toLong)
        }
      ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderLog](Time.seconds(0)) {
      override def extractTimestamp(t: OrderLog): Long = t.eventTime * 1000L
    }).filter(_.txId != "")
      .keyBy(_.txId)

    val resource2: URL = getClass.getResource("/ReceiptLog.csv")
    val inputDataStream2 = env.readTextFile(resource2.getPath)
      .map(
        data => {
          val strings: Array[String] = data.split(",")
          ReceiptEvent(strings(0), strings(1), strings(2).toLong)
        }
      ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(0)) {
      override def extractTimestamp(t: ReceiptEvent): Long = t.eventTime * 1000L
    })
      .keyBy(_.txId)

    val connectDS: DataStream[(OrderLog, ReceiptEvent)] = inputDataStream.connect(inputDataStream2).process(new connectFunction())
    connectDS.print("matched")
    connectDS.getSideOutput(unmatchedPays).print("unmatched pays")
    connectDS.getSideOutput(unmatchedReceipts).print("unmatched receipts")

    env.execute("order tx match")

  }
  class connectFunction() extends CoProcessFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)] {
    //定义支付状态
    lazy val payedStated: ValueState[OrderLog] = getRuntimeContext.getState(new ValueStateDescriptor[OrderLog]("payed", classOf[OrderLog]))
    //定义收到状态
    lazy val receipStated: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

    override def processElement1(pay: OrderLog, context: CoProcessFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)]#Context, collector: Collector[(OrderLog, ReceiptEvent)]): Unit = {
      val receip: ReceiptEvent = receipStated.value()
      if (receip != null) {
        collector.collect((pay, receip))
        receipStated.clear()
      } else {
        //如果receip还没来,储存pay状态，注册定时器等待
        payedStated.update(pay)
        context.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
      }
    }

    override def processElement2(receip: ReceiptEvent, context: CoProcessFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)]#Context, collector: Collector[(OrderLog, ReceiptEvent)]): Unit = {
      val payed: OrderLog = payedStated.value()
      if (payed != null) {
        collector.collect((payed, receip))
        payedStated.clear()
      } else {
        receipStated.update(receip)
        context.timerService().registerEventTimeTimer(receip.eventTime * 1000L + 3000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderLog, ReceiptEvent)]): Unit = {
      //如果定时器触发，说明有一方没有到
      if (payedStated.value() != null) {
        //说明receip没来
        ctx.output(unmatchedPays, payedStated.value())
      } else if(receipStated.value() != null){
        //说明pay没来
        ctx.output(unmatchedReceipts,receipStated.value())
      }
      payedStated.clear()
      receipStated.clear()
    }
  }
}

