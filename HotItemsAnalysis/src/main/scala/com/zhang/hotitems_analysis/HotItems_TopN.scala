package com.zhang.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import Util.ProducerKafka
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

//将数据的参数，转换为样例类格式
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义定义中间聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/**
  * Flink,pv TopN思路：
  * 1.创建env环境，定义时间语义
  * 2.从kafka读取参数
  * 3.对数据进行转换结构封装一个样例类，得到一个DataStream，之后分配时间戳和waterMark
  * 4.对DataStream的行为过滤出pv，根据id进行分组
  * 5.开窗设置窗口大小和滑动时间
  * 6.使用aggregate把增量聚合作为一个预聚合，把结果包装，用WindowResult输出
  * 7.aggregate需要传两个参数，a第一个增量聚合，b第二个对增量聚合的输出
  * 8.这两个参数a继承AggregateFunction（负责来一条增加一条记录）,b继承WindowFunction（提交窗口内的聚合值，id，窗口的end时间）
  * 9.对得到的数据进行排序，首先根据windowEnd分组，然后使用process，传入一个参数（用来控制top点击）
  * 10.自定义process需要继承KeyedProcessFunction：重写open，processElement，onTimer方法
  * 11.open方法定义list状态，在processElement方法取到窗口的值存到状态，设置定时器，如果watermark达到了windowEnd+1，就调用onTimer方法
  * 12.在onTimer方法获取list状态的值，根据count进行排序使用take就可以得到topN
  *
  */
object HotItems_TopN {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置waterMark,这个地方导包的时候选择org.apache.flink.streaming.api.TimeCharacteristic不然出不来EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)
    ProducerKafka.writeToKafka("UserBehavior")

    //1.读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    //key和value的反序列化
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //    val dataStream: DataStream[UserBehavior] = env.readTextFile("D:\\ideaworkspace\\Flink_UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = env.addSource(new FlinkKafkaConsumer[String]("UserBehavior", new SimpleStringSchema(), properties))
      .map { data =>
        val strings: Array[String] = data.split(",")
        //封装到样例类
        UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      }
      //分配时间戳和mark，此处是因为已经得到具体数据，不存在延迟，所以用这个方法，因为时间戳是秒，要*1000得到毫秒
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //2.过滤分组开窗聚合函数
    val itemCountDS: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      //开滑动窗口,窗口一小时，5分钟滑动一次
      .timeWindow(Time.hours(1), Time.minutes(5))
      //CountAgg把增量聚合作为一个预聚合，把结果包装，用WindowResult输出
      .aggregate(new CountAgg(), new WindowResult())

    //3.排序
    val resultDS: DataStream[String] = itemCountDS
      .keyBy("windowEnd") //按照窗口分组
      .process(new TopNitems(3))
    resultDS.print() //进行状态编程

    env.execute("Top_N")
  }

}

//自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  //初始值为1
  override def createAccumulator(): Long = 0L

  //来一条数据就+1
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  //结果返回acc
  override def getResult(acc: Long): Long = acc

  //这个用于会话窗口，大概是窗口和窗口间的累加
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//对预聚合进行包装，然后输出；此处key的类型为Tuple，是因为按k分组的时候用的是字段名
class WindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //将需要的属性转换类型之后封装到输出的样例类中
    //此处将key的Tuple类型转换成Long类型，需要导包：import org.apache.flink.api.java.tuple.{Tuple,Tuple1}
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    //设置Tuple1的类型为Long
    //获取5分钟窗口的最后一个事件时间
    val windowEnd: Long = window.getEnd
    //因为是Iterable类型，如果有的话，拿出这个值
    val count: Long = input.iterator.next()
    //提交
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

//自定义process
class TopNitems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //先定义一个list列表状态，存方items的count所有聚合结果
  private var itemListState: ListState[ItemViewCount] = _

  //真正定义这个状态，设置名称和类型；flink会根据这个进行状态管理
  override def open(parameters: Configuration): Unit = {
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-list", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //将状态添加到状态列表，第一条数据来时，注册一个定时器，延长1毫秒
    itemListState.add(value)
    //watermark要和定时器的时间戳比较~如果watermark达到了windowEnd+1，就会调用onTimer方法
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //watermark达到了windowEnd+1，就会调用onTimer方法
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //iterator()是java类型，需要导包
    import scala.collection.JavaConversions._

    //定义一个本地的list，用于提取所有的聚合的值,方便后边做排序
    val resultList: List[ItemViewCount] = itemListState.get().iterator().toList
    //清空list状态，释放资源
    itemListState.clear()
    //sort排序是从小到大，reverse是反过来，从大到小
    val sortedItemsList: List[ItemViewCount] = resultList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //将排序后的值格式化之后输出
    val stringBuffer = new StringBuffer()
    stringBuffer.append("==================================\n")
    stringBuffer.append("窗口关闭时间：").append(new Timestamp(timestamp - 1)).append("\n")
    //遍历排序之后集合的下标
    for (i <- sortedItemsList.indices) {
      val currentItem = sortedItemsList(0)
      stringBuffer.append("No").append(i + 1).append(":")
        .append("商品ID=").append(currentItem.itemId)
        .append("浏览量=").append(currentItem.count)
        .append("\n")
    }
    //控制显示频率
    Thread.sleep(1000L)
    //最终输出
    out.collect(stringBuffer.toString)
  }


}

