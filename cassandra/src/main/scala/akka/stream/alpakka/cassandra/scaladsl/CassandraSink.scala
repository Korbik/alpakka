/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.datastax.driver.core._

import scala.concurrent.Future
import akka.stream.alpakka.cassandra.GuavaFutures._

object CassandraSink {
  def apply[T](
      parallelism: Int,
      statement: PreparedStatement,
      statementBinder: (T, PreparedStatement) => Statement
  )(implicit session: Session): Sink[T, Future[Done]] =
    Flow[T]
      .mapAsyncUnordered(parallelism)(t ⇒ session.executeAsync(statementBinder(t, statement)).asScala())
      .toMat(Sink.ignore)(Keep.right)

  def batch[T](
      parallelism: Int,
      statement: PreparedStatement,
      statementBinder: (T, PreparedStatement) => Statement,
      batchStatement: BatchStatement = new BatchStatement()
  )(implicit session: Session): Sink[TraversableOnce[T], Future[Done]] = {
    def batchElement(seq: TraversableOnce[T], ps: PreparedStatement): Statement =
      seq.foldLeft(batchStatement) {
        case (bs: BatchStatement, t: T) =>
          bs.add(statementBinder(t, statement))
      }
    apply[TraversableOnce[T]](parallelism, statement, batchElement)
  }

}
