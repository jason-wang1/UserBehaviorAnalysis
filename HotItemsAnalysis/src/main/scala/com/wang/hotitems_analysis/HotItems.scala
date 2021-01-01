package com.wang.hotitems_analysis

import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}

import scala.collection.mutable.ListBuffer


/**
  * Descreption:
  * 热门商品统计：每隔5分钟输出最近一个小时点击量最多的前N个商品
  * 考察点：窗口操作、状态编程
  *
  * Date: 2020年05月21日
  *
  * @author WangBo
  * @version 1.0
  */

// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val path: String = this.getClass.getResource("/UserBehavior.csv").getPath
    val dataStream: DataStream[UserBehavior] = env.readTextFile(path)
//      .socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps( _.timestamp * 1000L )


    // 3.transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
      .aggregate(new CountAgg(), new WindowResult()) // 窗口聚合
      .keyBy(_.windowEnd) // 按照窗口分组
      .process(new TopNHotItems(3))

    // 3.sink：控制台输出
    processedStream.print()
    env.execute("hot item job")
  }
}

// 自定义预聚合函数
class CountAgg extends AggregateFunction[UserBehavior, Long, Long]() {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，输出ItemViewCount
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, aggregateResult: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, aggregateResult.iterator.next()))
  }
}

// 自定义的处理函数
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{
  private var itemState: ListState[ItemViewCount] = _

  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)

    // 注册一个定时器
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  // 定时器触发时堆所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 将所有state中的数据取出，放到一个List Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get()){
      allItems += item
    }

    // 按照count大小排序，取前N个
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 清空状态
    itemState.clear()

    // 将排名结果格式化输出
    val result = new StringBuilder()
    result.append(s"时间：").append(new Timestamp(timestamp - 1)).append("\n")

    // 输出每一个商品的信息
    for(i <- sortedItems.indices){
      val currentItem: ItemViewCount = sortedItems(i)
      result.append("No").append(i + 1).append(":").append(" 商品ID = ").append(currentItem.itemId).append("浏览量 = ")
        .append(currentItem.count).append("\n")
    }

    result.append("==================")
    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

