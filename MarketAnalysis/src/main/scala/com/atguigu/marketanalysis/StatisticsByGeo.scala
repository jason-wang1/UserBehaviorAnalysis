package com.atguigu.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Descreption: XXXX<br/>
  * Date: 2020年05月23日
  *
  * @author WangBo
  * @version 1.0
  */

// 输入的广告点击事件样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

// 按照省份统计的输出结果样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)

// 输出黑名单报警信息：如果一个用户一天内点击同一个广告超过100次，
// 应该将用户加入黑名单报警，此后其点击行为将不再统计
case class BlackListWarning(userId: Long, adId: Long, msg: String)

object StatisticsByGeo {

  // 定义侧输出流的tag
  val blackListOutputTag = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据并转换成AdClickEvent
    val adEventStream: DataStream[AdClickEvent] = env.readTextFile("C:\\Users\\BoWANG\\IdeaProjects\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv")
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        AdClickEvent(dataArray(0).toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)

    // 自定义process function，过滤大量刷点击的行为
    val filterBlackListStream = adEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100)) // 相当于复杂版的filter

    // 根据省份分组，开窗聚合
    val adCountStream: DataStream[CountByProvince] = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print("count")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blacklist")
    env.execute("ad statistic job")

  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
    // 定义状态，保存当前用户对当前广告的点击量
    // 由于之前已经做了keyBy，所以这里就是当前用户对当前广告的点击
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))

    // 保存是否发送黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))

    // 保存定时器触发的时间戳
    lazy val reseTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      // 取出count状态
      val curCount: Long = countState.value()

      // 如果是第一次处理，注册定时器，每天零点触发
      if (curCount == 0){
        // 得到天数，从1970.01.01开始计算
        val days: Long = ctx.timerService().currentProcessingTime() / (1000*60*60*24)
        // 得到明天零点的时间戳
        val ts: Long = (days + 1) * (1000*60*60*24)

        reseTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断计数是否达到上限，如果达到则加入黑名单
      if (curCount >= maxCount){
        // 判断是否发送过黑名单，只发过一次
        if (!isSentBlackList.value()){
          isSentBlackList.update(true)
          // 输出到侧输出流
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over "+ maxCount + "times today"))
        }
        return
      }

      // 计数状态加1，输出数据到主流
      countState.update(curCount + 1)
      out.collect(value)
    }

    // 定时器触发时，清空状态
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      if (timestamp == reseTimer.value()){
        isSentBlackList.clear()
        countState.clear()
        reseTimer.clear()
      }
    }
  }
}

// 自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}
