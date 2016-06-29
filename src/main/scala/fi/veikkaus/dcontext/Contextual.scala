package fi.veikkaus.dcontext

import java.io.Closeable

/**
  * Created by arau on 25.5.2016.
  */
class Contextual(val contextName:String) {

  def cval[T <: Object](vname:String)(make : MutableDContext => T) =
    new ContextVal(contextName+"."+vname, (c:MutableDContext) => (make(c), None))
  def cvalc[T <: Object](vname:String)(make : MutableDContext => T)(closer : T => Unit) =
    new ContextVal(contextName+"."+vname, (c:MutableDContext) => {
      val v = make(c)
      (v, Some(new Closeable() {
        override def close()  = {
          closer(v)
        }
      }))
    })

/*  def cval[T <: Object](vname:String)(make : => T) =
    new ContextVal(contextName+"."+vname, (c:ServerContext) => make)*/

}
