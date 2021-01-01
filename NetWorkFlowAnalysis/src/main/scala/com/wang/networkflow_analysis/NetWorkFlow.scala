package com.wang.networkflow_analysis

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Descreption:
  * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
  * Date: 2020年05月21日
  *
  * @author WangBo
  * @version 1.0
  */

// 输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path: String = this.getClass.getResource("/apache.log").getPath
    val dataStream: DataStream[UrlViewCount] = env.readTextFile(path)
      .map(data => {
        val dataArray: Array[String] = data.split(" ")
        // 定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = simpleDateFormat.parse(dataArray(3).trim)
        val timestamp: Long = date.getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
        }
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60)) // 允许60秒的迟到数据
      .aggregate(new CountAgg(), new WindowResult())

    val processedStream: DataStream[String] = dataStream
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
    processedStream.print()

    env.execute("network flow job")
  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def createAccumulator(): Long = 0

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口处理函数 输入是前面聚合函数的输出
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义排序输出处理函数
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState((new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount])))
  override def processElement(value: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    urlState.add(value)
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 从状态中拿到数据
    val allUrlViews = new ListBuffer[UrlViewCount]()
    val iter: util.Iterator[UrlViewCount] = urlState.get().iterator()
    while (iter.hasNext){
      allUrlViews += iter.next()
    }

    urlState.clear()

    val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortWith(_.count > _.count).take(topSize)

    // 格式化输出
    val result = new StringBuilder()
    result.append("时间，").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedUrlViews.indices){
      val currentUrlView: UrlViewCount = sortedUrlViews(i)
      result.append("NO").append(i+1).append(":")
        .append("URL=").append(currentUrlView.url)
        .append("访问量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
