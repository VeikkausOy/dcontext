package fi.veikkaus.dcontext


/**
  * Created by arau on 24.5.2016.
  */
class ScalaTestTask extends ContextTask  {
  override def run(context: MutableDContext, args: Array[String]) = {
    var v = context.get[Int]("test.runNumber").getOrElse(0)
    System.out.println("this scala task has been run before " + v + " times")
    context.put("test.runNumber", (v + 1).asInstanceOf[Integer])

    System.out.println("args are: " + args.mkString(","))
  }
}


