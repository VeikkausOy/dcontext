package fi.veikkaus.dcontext

/**
  * Created by arau on 25.5.2016.
  */
class ContextVal[T](key:String, make: (MutableDContext)=> (T, Option[java.io.Closeable])) {
  def apply(implicit c:MutableDContext) : T = {
    var v =
      try {
        c.get[T](key)
      } catch {
        case e:ClassCastException => None
      }
    v match {
      case None =>
        val (vr, cl) = make(c)
        c.put(key, vr, cl)
        vr
      case Some(v) =>
        v
    }
  }
  def update(v:T, cl:Option[java.io.Closeable] = None)(implicit c:MutableDContext) = {
    reset(c)
    if (cl.isEmpty && v.isInstanceOf[java.io.Closeable]) {
      c.put(key, v, Some(v.asInstanceOf[java.io.Closeable]))
    } else {
      c.put(key, v, cl)
    }
  }
  def isDefined(implicit c:MutableDContext) = c.get[T](key).isDefined
  def reset(implicit c:MutableDContext) = {
    if (isDefined) c.remove(key)
  }

}

