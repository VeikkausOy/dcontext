package fi.veikkaus.dcontext

import java.io.Closeable

/**
  * Created by arau on 28.6.2016.
  */
trait DContext extends java.io.Closeable {
  def get[T](name:String) : Option[T]
  def getType(name:String) : Option[Class[_]]
  def keySet : scala.collection.Set[String]
  def apply[T](name:String) = get[T](name).get

  def ++(other:DContext) = DContext.mask(this, other)
  def ++(other:MutableDContext) = DContext.mask(this, other)
}

object DContext {

  def empty = new DContext {
    override def getType(name: String) = None
    override def get[T](name: String) = None
    override def keySet: Set[String] = Set.empty
    override def apply[T](name: String): T = throw new NoSuchElementException(name)
    override def close() = Unit
  }

  def apply(ctx : Map[String, Any]) = new ImmutableDContext(ctx)
  def apply(ctx : (String, Any)* ) = new ImmutableDContext(ctx.toMap)

  def mask(masked:DContext, mask:DContext) = new DContext {
    private def orElse[T](a:Option[T], b: => Option[T]) = {
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
    override def close() = {
      mask.close()
      masked.close()
    }
  }

  def mask(masked:DContext, mask:MutableDContext) = new MutableDContext {
    private def orElse[T](a:Option[T], b: => Option[T]) = {
      a match {
        case Some(v) => Some(v)
        case None => b
      }
    }
    override def getType(name: String) = {
      orElse(mask.getType(name), masked.getType(name))
    }
    override def get[T](name: String) : Option[T] = {
      orElse(mask.get(name), masked.get(name))
    }
    override def keySet: Set[String] = {
      mask.keySet.toSet ++ masked.keySet
    }
    override def close() = {
      mask.close()
      masked.close()
    }
    override def remove(key:String): Unit = {
      mask.remove(key)
    }
    override def put (key: String, value: Any, closer: Option[Closeable]): Unit = {
      mask.put(key, value, closer)
    }
  }

}
