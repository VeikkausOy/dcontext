package fi.veikkaus.dcontext.value

import java.io.File

import fi.veikkaus.dcontext.store.Store

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by arau on 1.11.2016.
  */
trait Val[T] {
  def get : T
}

object Val {
  def apply[T](f : => T) = new Val[T] {
    override def get = f
  }
}

trait Var[T] extends Val[T] {
  def update(v:T) : Unit
}

class HeapVar[T](init:T) extends Var[T] {
  private var value = init
  override def update(v: T): Unit = {value = v}
  override def get: T = value
}

trait AsyncVal[T] extends Val[Future[T]] {}

class StoreVar[T](store:Store[T], init: => T) extends Var[T] {
  store.get match {
    case None => store.update(Some(init))
    case Some(v) =>
  }
  def get = store.get.get
  def update(v:T) = store.update(Some(v))
}

// passes only one request throught
class Barrier[T](f: => Future[T]) {
  private var future: Option[Future[T]] = None
  def get: Future[T] = synchronized {
    future match {
      case None =>
        val rv = f.map { rv =>
          Barrier.this.synchronized {
            future = None
          }
          rv
        }
        future = Some(rv)
        rv
      case Some(f) => f
    }
  }
}

class AsyncLazyVal[T](store:Store[T], init: => Future[T]) extends AsyncVal[T] {
  val barrier =
    new Barrier[T] (init.map { v => store.update(Some(v)); v })
  def get = {
    store.get match {
      case Some(v) => Future (v)
      case None    => barrier.get
    }
  }

}

