//
// dcontext
//
// provides a data context and dynamic class loader for code
//

scalaVersion := "2.10.6"
val scalaMajorVersion = "2.10"

version := "0.2-SNAPSHOT"

libraryDependencies ++= Seq()

lazy val testsh =
  taskKey[Unit]("interactive shell for running tasks in a JVM instance, while code may be modified")

lazy val root = (project in file(".")).
  settings(
    name := "dcontext",
    organization := "fi.veikkaus",
    testsh := {
      val mainClass = "fi.veikkaus.dcontext.Console"
      val keyPath =  f"target/scala-${scalaMajorVersion}/test-classes"
      val selector = (path:File) => {
        val p = path.getPath
        p.contains(keyPath)
      }
      val classpath =
        (fullClasspath in Test)
          .value
          .map(i=>i.data)
      val staticCP = classpath.filter(!selector(_))
      val dynamicCP = classpath.filter(selector)

      val options =  (javaOptions in Test).value
      val log = (streams in Test).value.log
      val args = Seq(
        f"-p ${dynamicCP.mkString(":")}",
        "-m fi.veikkaus.dcontext.TestContext",
        "-i")

      (runner in run).value.run(mainClass, staticCP, args, streams.value.log)
    }
  )
