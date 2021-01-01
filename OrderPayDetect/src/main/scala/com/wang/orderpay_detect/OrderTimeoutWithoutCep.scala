package com.wang.orderpay_detect

import java.net.URL

import com.wang.orderpay_detect.OrderTimeout.getClass
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Descreption: XXXX<br/>
  * Date: 2020年05月24日
  *
  * @author WangBo
  * @version 1.0
  */
object OrderTimeoutWithoutCep {
  // 侧输出流标签
  val orderTimeoutWithoutTag = new OutputTag[OrderResult]("orderTimeout")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1.读取订单数据
    val resource: URL = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 定义process function进行超时检测
//    val timeoutWarningStream = orderEventStream.process(new OrderTimeoutWarning())

    val orderResultStream = orderEventStream.process(new OrderPayMatch())
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutWithoutTag).print("timeout")

//    timeoutWarningStream.print()
    env.execute("order timeout without cep job")
  }

  // 需要使用orderTimeoutWithoutTag标签，所以在里面写
  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    // 保存pay是否来过的状态
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

    // 保存定时器的时间戳为状态
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))
    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      // 先读取状态
      val isPayed = isPayedState.value()
      val timer = timerState.value()

      // 根据事件的类型进行分类判断，做不同的处理逻辑
      if (value.eventType == "create"){
        // 1.如果是create事件，接下来判断pay是否来过
        if (isPayed){
          // 1.1 如果已经pay过，匹配成功，输出主流数据，清空状态
          out.collect(OrderResult(value.orderId, "payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timer)
          timerState.clear()
        } else {
          // 1.2 如果没有pay过，注册定时器等待pay的到来
          val ts = value.eventTime * 1000L + 15 * 60 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (value.eventType == "pay"){
        // 2 如果是pay事件，判断是否create过，用timer表示
        if (timer > 0){
          // 2.1 如果由定时器，说明create已经来过
          // 继续判断是否超过了timeout时间
          if (timer > value.eventTime * 1000L){
            // 2.1.1 如果定时器时间还没到，输出成功匹配
            out.collect(OrderResult(value.orderId, "payed successfully"))
          } else {
            // 2.1.2 如果当前pay的时间已经超时，输出侧输出流
            ctx.output(orderTimeoutWithoutTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          // 输出结束，清空状态
          out.collect(OrderResult(value.orderId, "payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timer)
          timerState.clear()
        } else {
          // 2.2 pay先到了，更新状态，注册定时器，等一下create
          isPayedState.update(true)
          // 使用当前事件时间注册定时器，此时watermark还没有涨到这里来
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
          timerState.update(value.eventTime * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 1 根据状态的值判断哪个数据没来
      if (isPayedState.value()){
        // 如果为true，表示pay先到了，没有等到create
        ctx.output(orderTimeoutWithoutTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))
      } else {
        // create到了，没有等到pay
        ctx.output(orderTimeoutWithoutTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
    }
  }
}

class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
  // 保存pay是否来过的状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 先取出状态标志位
    val isPayed = isPayedState.value()

    // 如果遇到了create事件，并且pay没有来过，注册定时器开始等待
    if (value.eventType == "create" && !isPayed){
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
    } else if (value.eventType == "pay"){
      // 如果是pay事件，直接把状态改为true
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 判断isPayed是否为true
    val isPayed: Boolean = isPayedState.value()
    if (isPayed){
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }
    // 清空状态
    isPayedState.clear()
  }
}
