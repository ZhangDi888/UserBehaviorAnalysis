import java.net.URL
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import HotPageAnalysis.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object HotPageAnalysis {

  /*
  * 思路：
  * 1.读取数据 2.设置waterMark 3.对数据转换结构封装样例类 4.按k分组开窗聚合
  * 因为使用了窗口延迟，在窗口延迟的时间以内，如果数据来到会更新数据，这样就会造成数据重复
  * 在process定义了map状态，这样可以去重
  * */
  //制作数据源的样例类
  case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

  //定义中间的聚合结果样例类
  case class UrlViewCount(url: String, windowEnd: Long, count: Long)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    //读取数据
    val resource: URL = getClass.getResource("/apache.log")
    //转换结构封装样例类
    val inputDataStream = env.readTextFile(resource.getPath)
      .map(
        data => {
          val strings: Array[String] = data.split(" ")
          //定义格式化方式
          val simpleData = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
          //对日期进行格式化，转换成时间戳,获取到的是毫秒数
          val timestamp: Long = simpleData.parse(strings(3)).getTime
          //封装样例类
          ApacheLogEvent(strings(0), strings(1), timestamp, strings(5), strings(6))
        }
      ) //设置Watermarks，延迟1秒，extractTimestamp方法是确认哪个是时间的字段
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
    })
    //过滤分组开窗聚合
    val aggStream: DataStream[UrlViewCount] = inputDataStream
      //      .filter(_.method == "GET")
      //使用正则表达式去掉包含css和js的相关url
      .filter(data => {
      val pattern: Regex = "^((?!\\.(css|js)$).)*$".r
      (pattern findFirstIn data.url).nonEmpty
    }
    )
      .keyBy(_.url)
      //设置1小时的滑动窗口，5分钟滚动一次
      .timeWindow(Time.minutes(10), Time.seconds(10))
      //设置窗口延迟1分钟后关闭，各个数据所在的窗口会继续计算，超时的数据可以放入侧输出流
      .allowedLateness(Time.minutes(1))
      //设置侧输出流
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      //设置预聚合，和输出的函数
      .aggregate(new UrlCountAgg(), new UrlCountResult())

    //分组排序，要求是每个窗口内的排序
    val result: DataStream[String] = aggStream.keyBy(_.windowEnd)
      .process(new TopNUrls(5))
    result.print("result")
    env.execute("hot page job")
  }
}

//自定义窗口函数包装成样例类
class UrlCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  //设置初始值为0
  override def createAccumulator(): Long = 0L

  //来一个数据加1
  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  //返回结果
  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//对预聚合的值进行输出
class UrlCountResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNUrls(size: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  //自定义map状态,存方url的所有结果，因为存在窗口延迟，数据1分钟内出现都会进行状态更新，用map是为了去重
  private var urlMap: MapState[String, Long] = _

  //真正定义这个状态，设置名称和类型，用于flink进行状态管理
  override def open(parameters: Configuration): Unit = {
    urlMap = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("url-map", classOf[String], classOf[Long]))
  }

  override def processElement(value: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    //将值放进map状态
    urlMap.put(value.url, value.count)
    //定义一个定时器,窗口关闭时间+1毫秒，也就是说，watermark超过窗口关闭时间，就可以触发了
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //当定时器触发时，watermark涨过了windowEnd，就会调用这个方法
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlsCount: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]
    //获取到map状态，放到list中，方便排序
    val iter: util.Iterator[Map.Entry[String, Long]] = urlMap.entries().iterator()
    while (iter.hasNext) {
      //将一个个map迭代出来
      val entry: Map.Entry[String, Long] = iter.next()
      //放入list集合
      allUrlsCount += ((entry.getKey, entry.getValue))
    }
    //进行排序
    val sortList: ListBuffer[(String, Long)] = allUrlsCount.sortWith(_._2 > _._2).take(size)

    //格式化输出
    val result = new StringBuffer()
    result.append("==========================\n")
    result.append("窗口关闭时间：").append(new Timestamp(timestamp - 1)).append("\n")
    //遍历list的下标
    for (i <- sortList.indices) {
      val currentYrl = sortList(i)
      result.append("No").append(i + 1).append(":")
        .append("URL=").append(currentYrl._1)
        .append("访问量=").append(currentYrl._2)
        .append("\n")
    }
    //控制显示频率
    Thread.sleep(1000L)
    //输出
    out.collect(result.toString)
  }
}