package com.zhang.orderpay_detect

import java.net.URL
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//输入数据的样例类
case class OrderLog(orderId: Long, eventType: String, txId: String, eventTime: Long)

//输出订单检测结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeoutWithCEP {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

    //指定pattern规则
    val pattern: Pattern[OrderLog, OrderLog] = Pattern.begin[OrderLog]("create").where(_.eventType == "create")
      //followedBy是宽松近邻，意思是说15分钟以内，只要同个订单id，create之后有pay都有效
      .followedBy("pay").where(_.eventType == "pay")
      //设置时间范围
      .within(Time.minutes(15))

    //应用到数据中
    val patternStream: PatternStream[OrderLog] = CEP.pattern(inputDataStream, pattern)
    //制作一个测输出流标签，把超时的订单输出
    val OrderTimeOut = new OutputTag[OrderResult]("OrderTimeOut")
    //调用select方法，得到最终的输出结果
    val patternResult: DataStream[OrderResult] = patternStream.select(OrderTimeOut,
      new timeOutOrder(),
      new payOrder()
    )
    patternResult.print("payed")
    patternResult.getSideOutput(OrderTimeOut).print("TimeOut")
    env.execute()
  }
}

class timeOutOrder() extends PatternTimeoutFunction[OrderLog, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderLog]], l: Long): OrderResult = {
    val id: Long = map.get("create").iterator().next().orderId
    OrderResult(id, "timeOut at" + l)
  }
}

class payOrder() extends PatternSelectFunction[OrderLog, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderLog]]): OrderResult = {
    val id: Long = map.get("pay").iterator().next().orderId
    OrderResult(id, "payed successfully")
  }
}
