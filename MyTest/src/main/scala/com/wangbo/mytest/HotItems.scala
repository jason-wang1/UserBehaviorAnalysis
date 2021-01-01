package com.wangbo.mytest

import java.net.URL
import java.sql.Timestamp
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Descreption: 热门商品统计：每隔5分钟输出最近一个小时点击量最多的前N个商品
  * Date: 2020年05月24日
  *
  * @author WangBo
  * @version 1.0
  */

// 输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timstamp: Long)

// 输出数据样例类
case class ItemViewCount(itemId: Long, timestamp: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path: String = "C:\\Users\\BoWANG\\IdeaProjects\\UserBehaviorAnalysis\\MyTest\\src\\main\\resources\\UserBehavior.csv"
    val dataStream = env.readTextFile(path)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toLong, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timstamp * 1000L)

    val processedStream: DataStream[String] = dataStream
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.timestamp)
      .process(new TopNHotItems(3))

    processedStream.print()
    env.execute("hot items")
  }
}

class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult() extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, context.window.getEnd, elements.iterator.next()))
  }
}

class TopNHotItems(topN: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  lazy val itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    itemState.add(value)
    ctx.timerService().registerEventTimeTimer(value.timestamp + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val itemViewCounts = new ListBuffer[ItemViewCount]
    val itemIter: util.Iterator[ItemViewCount] = itemState.get().iterator()
    while (itemIter.hasNext){
      itemViewCounts.append(itemIter.next())
    }
    val topItems: ListBuffer[ItemViewCount] = itemViewCounts.sortBy(_.count).reverse.take(topN)

    // 清空状态
    itemState.clear()

    // 格式化输出
    val sb = new StringBuffer()
    sb.append(s"时间：${new Timestamp(timestamp - 1)}\n")
    for (elem <- topItems) {
      sb.append(s"itemId: ${elem.itemId}; count: ${elem.count}\n")
    }
    sb.append("=======================")

    Thread.sleep(200)
    out.collect(sb.toString)
  }
}


