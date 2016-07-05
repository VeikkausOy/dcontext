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
  def apply = new HashMapDContext()
}