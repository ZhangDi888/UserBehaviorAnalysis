package com.zhang.loginfail_detect

import java.net.URL
import java.util

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//数据转化结构
case class Login(userId: Long, ip: String, eventType: String, eventTime: Long)

//报警样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, msg: String)

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    //    val resource: URL = getClass.getResource("/LoginLog.csv")
    //    val inputDataStream = env.readTextFile(resource.getPath)
    val inputDataStream = env.socketTextStream(host,port)
      .map(
        data => {
          val strings: Array[String] = data.split(",")
          Login(strings(0).toLong,strings(1),strings(2),strings(3).toLong)
        }
      )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Login](Time.seconds(6)) {
        override def extractTimestamp(t: Login): Long = t.eventTime * 1000L
      })
      .keyBy(_.userId)

    //指定pattern规则,用来检车流里面的连续登录失败事件
    val loginFailPattern: Pattern[Login, Login] = Pattern.begin[Login]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      //设置检测的时间5秒内
      .within(Time.seconds(5))

    //将pattern应用与当前的数据流
    val patternStream: PatternStream[Login] = CEP.pattern(inputDataStream, loginFailPattern)

    //定义一个SelectFunction，从检测到匹配的复杂时间中提取事件，输出报警信息
    val warningStream = patternStream.select(new LoginFailMatch())
    warningStream.print("login---fail")
    env.execute()
  }

}

class LoginFailMatch() extends PatternSelectFunction[Login,Warning]() {
  override def select(map: util.Map[String, util.List[Login]]): Warning = {
    val first: Login = map.get("firstFail").get(0)
    val second: Login = map.get("secondFail").get(0)
    Warning(first.userId,first.eventTime,second.eventTime,"login fail")
  }
}

