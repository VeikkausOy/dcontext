package fi.veikkaus.dcontext

import java.io.{Closeable, IOException}

import org.slf4j.LoggerFactory

import scala.collection.mutable.HashMap


class HashMapDContext extends MutableDContext {

  private val logger = LoggerFactory.getLogger(getClass)

  private val data = new HashMap[String, Any]
  private val closeables = new HashMap[String, Closeable]

  override def remove(key: String) {
    data.remove (key)
    closeables.get (key) match {
      case Some (c) =>
        try {
          c.close
        } catch {
          case e: IOException => {
            throw new RuntimeException (e)
          }
        }
      case None =>
    }
    closeables.remove (key)
  }

  override def close {

    import scala.collection.JavaConversions._
    for ((key, c) <- closeables) {
      if (c != null) {
        try {
          c.close
        } catch {
          case e: Exception => {
            logger.error("closing '" + key + "' failed", e)
          }
        }
      }
    }
    data.clear
    closeables.clear
  }

  override def get[T] (key: String): Option[T] = {
    data.get (key).map (_.asInstanceOf[T] )
  }

  override def getType (key: String): Option[Class[_]] = {
    data.get (key).map (_.getClass)
  }

  override def keySet = data.keySet

  override def put (key: String, value: Any, closer: Option[Closeable]) {
    remove (key)
    data.put (key, value)
    closer match {
      case Some(v) => closeables.put (key, v)
      case None    =>
    }
  }
}
