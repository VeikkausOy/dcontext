package fi.veikkaus.dcontext

import java.io.{Closeable, IOException}

import org.slf4j.LoggerFactory

import scala.collection.immutable.HashSet
import scala.collection.mutable.HashMap


class HashMapDContext extends MutableDContext {

  private val logger = LoggerFactory.getLogger(getClass)

  private val data = new HashMap[String, Any]
  private val closeables = new HashMap[String, Closeable]

  override def remove(key: String) = this.synchronized {
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

  override def close = this.synchronized {

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

  override def get[T] (key: String): Option[T] = this.synchronized  {
    data.get (key).map (_.asInstanceOf[T] )
  }

  override def getType (key: String): Option[Class[_]] = this.synchronized  {
    data.get (key).map (_.getClass)
  }

  override def keySet = this.synchronized {
    HashSet.apply[String](data.keySet.toSeq  : _*) // make a copy
  }

  override def put (key: String, value: Any, closer: Option[Closeable]) = this.synchronized {
    remove (key)
    data.put (key, value)
    closer match {
      case Some(v) => closeables.put (key, v)
      case None    =>
    }
  }
}
