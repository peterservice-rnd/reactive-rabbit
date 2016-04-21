package io.scalac.amqp.impl

import java.util.Objects.requireNonNull
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.rabbitmq.client.{Channel, ConfirmListener}
import io.scalac.amqp.Routed
import org.reactivestreams.{Subscriber, Subscription}

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.stm.{Ref, atomic}
import scala.util.control.NonFatal

private[amqp] class ExchangeSubscriber(channel: Channel, exchange: String)
  extends Subscriber[Routed] with ConfirmListener {
  require(exchange.length <= 255, "exchange.length > 255")

  channel.addConfirmListener(this)
  channel.confirmSelect()

  val active = new AtomicReference[Subscription]()
  val publishingThreadRunning = Ref(false)
  val buffer = Ref(Queue[Routed]())
  val closeRequested = Ref(false)

  val deliveryTag = new AtomicLong()
  val unacknowledged = TrieMap[Long, Boolean]()

  override def onSubscribe(subscription: Subscription): Unit =
    active.compareAndSet(null, subscription) match {
      case true  ⇒ subscription.request(1)
      case false ⇒ subscription.cancel() // 2.5: cancel
    }

  override def onNext(routed: Routed): Unit = {
    requireNonNull(routed) // 2.13
    val running = atomic { implicit txn =>
        buffer.transform(_ :+ routed)
        publishingThreadRunning.getAndTransform(_ => true)
      }
    if (!running) {
      Future(publishFromBuffer())
    }
  }

  @tailrec
  private def publishFromBuffer(): Unit = {
    val headOpt = buffer.single.transformAndExtract(q => (q.tail, q.headOption))
    headOpt.foreach(publish)
    val continue = atomic { implicit txn =>
      publishingThreadRunning.transformAndGet(_ => buffer().nonEmpty)
    }
    if (continue) {
      publishFromBuffer()
    }
  }

  private def publish(routed: Routed): Unit = {
    try {
      unacknowledged += deliveryTag.incrementAndGet() -> true
      channel.basicPublish(
        exchange,
        routed.routingKey,
        Conversions.toBasicProperties(routed.message),
        routed.message.body.toArray)
    } catch {
      case NonFatal(exception) => // 2.6
        active.get().cancel()
        closeChannel()
    }
  }

  /** Double check before calling `close`. Second `close` on channel kills connection.*/
  private def closeChannel(): Unit = {
    if (closeRequested.single.compareAndSet(false, true) && channel.isOpen()) {
      channel.close()
    }
  }

  /** Our life cycle is bounded to underlying `Channel`. */
  override def onError(t: Throwable): Unit = {
    requireNonNull(t)
    shutdownWhenFinished()
  }

  /** Our life cycle is bounded to underlying `Channel`. */
  override def onComplete(): Unit = shutdownWhenFinished()

  private def shutdownWhenFinished(): Unit = {
    Future {
      publishingThreadRunning.single.await(!_)
      closeChannel()
    }
  }

  override def toString = s"ExchangeSubscriber(channel=$channel, exchange=$exchange)"

  override def handleAck(deliveryTag: Long, multiple: Boolean): Unit =
    if (multiple) {
      for (i <- unacknowledged.keys.filter(_ <= deliveryTag))
        if(unacknowledged.remove(i, true))
          active.get().request(1)
    }
    else {
      if(unacknowledged.remove(deliveryTag, true))
        active.get().request(1)
    }

  override def handleNack(deliveryTag: Long, multiple: Boolean): Unit = {
    active.get().cancel()
    closeChannel()
  }
}
