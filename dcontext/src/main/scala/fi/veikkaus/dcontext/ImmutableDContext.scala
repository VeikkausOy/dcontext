package fi.veikkaus.dcontext

/**
  * Created by arau on 28.6.2016.
  */
class ImmutableDContext(ctx:Map[String, Any], closeable:Set[java.io.Closeable] = Set.empty) extends DContext {
  def get[T](name:String) =
    ctx.get(name).map(_.asInstanceOf[T])
  def getType(name:String) =
    ctx.get(name).map(_.getClass)
  def close = {
    ctx.values.foreach { v =>
      v match {
        case c : java.io.Closeable => c.close()
        case _ =>
      }
    }
    closeable.foreach { _.close }
  }
  def keySet = ctx.keySet
}
