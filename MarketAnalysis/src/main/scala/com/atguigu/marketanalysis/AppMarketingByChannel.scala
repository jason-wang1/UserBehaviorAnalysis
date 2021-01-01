package com.atguigu.marketanalysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Descreption:
  * 根据不同行为、渠道开窗统计用户数量
  * Date: 2020年05月23日
  *
  * @author WangBo
  * @version 1.0
  */
// 输入数据样例类
case class MarketingUserBehavier(userId: String, behavier: String, channel: String, timestamp: Long)

// 输出结果样例类
case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

object MarketAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavier != "UNINSTALL")
      .map(data => {
        ((data.channel, data.behavier), 1L)
      })
      .keyBy(_._1) // 以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannel())



    dataStream.print()
    env.execute("app marketing by channel job")
  }
}

// 自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavier] {
  // 定义是否运行的标志位
  var running = true
  // 定义用户行为的集合
  private val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 定义渠道集合
  private val channelSet: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  // 定义一个随机数发生器
  private val rand = new Random()
  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavier]): Unit = {
  // 定义一个生成数据的上线
    val maxElements: Long = Long.MaxValue
    var count = 0L

    // 随机生成数据
    while (running && count < maxElements) {
      val id: String = UUID.randomUUID().toString
      val behavior: String = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel: String = channelSet(rand.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()
      ctx.collect(MarketingUserBehavier(id, behavior, channel, ts))

      count += 1
      TimeUnit.MILLISECONDS.sleep(10L)
    }

  }

  override def cancel(): Unit = false
}

class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(context.window.getStart).toString
    val endTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect( MarketingViewCount(startTs, endTs, channel, behavior, count) )
  }
}
