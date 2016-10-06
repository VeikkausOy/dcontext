package fi.veikkaus.dcontext

import java.io.Closeable
import java.io.IOException
import scala.collection.mutable.HashMap

trait MutableDContext extends DContext {
  def remove(key:String)
  def put(key: String, value: Any, closer: Option[Closeable])

  def put(key: String, value: Any, closer: Closeable) : Unit= {
    put(key, value, Some(closer))
  }
  def put(key: String, value: Any) {
    put (key, value, None)
  }
  def put(key: String, value: Closeable) {
    put (key, value, Some(value))
  }
}

object MutableDContext {
  def apply() = new HashMapDContext()

  // NOTE: that closing returned context closes only the top context
  def mask(masked: DContext) = new MutableDContext {
    val mask = new HashMapDContext
    private def orElse[T](a: Option[T], b: => Option[T]) = {
      a match {
        case Some(v) => Some(v)
        case None => b
      }
    }
    override def getType(name: String) = {
      orElse(mask.getType(name), masked.getType(name))
    }
    override def get[T](name: String) = {
      orElse(mask.get(name), masked.get(name))
    }
    override def keySet: Set[String] = {
      mask.keySet.toSet ++ masked.keySet
    }
    override def remove(key: String): Unit = {
      mask.remove(key)
    }
    override def put(key: String, value: Any, closer: Option[Closeable]): Unit = {
      mask.put(key, value, closer)
    }
    override def close() = {
      mask.close()
    }
  }
}