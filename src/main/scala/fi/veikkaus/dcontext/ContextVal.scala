package fi.veikkaus.dcontext

/**
  * Created by arau on 25.5.2016.
  */
class ContextVal[T <: Object](key:String, make: (MutableDContext)=> (T, Option[java.io.Closeable])) {
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

}

